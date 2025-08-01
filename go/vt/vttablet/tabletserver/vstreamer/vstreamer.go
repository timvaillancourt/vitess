/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vstreamer

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/logutil"
	vtschema "vitess.io/vitess/go/vt/schema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	vttablet "vitess.io/vitess/go/vt/vttablet/common"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"

	mysqlbinlog "vitess.io/vitess/go/mysql/binlog"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const (
	trxHistoryLenQuery = `select count as history_len from information_schema.INNODB_METRICS where name = 'trx_rseg_history_len'`
	replicaLagQuery    = `show replica status`
	legacyLagQuery     = `show slave status`
	hostQuery          = `select @@hostname as hostname, @@port as port`
)

// HeartbeatTime is set to slightly below 1s, compared to idleTimeout
// set by VPlayer at slightly above 1s. This minimizes conflicts
// between the two timeouts.
var HeartbeatTime = 900 * time.Millisecond

// vstreamer is for serving a single vreplication stream on the source side.
type vstreamer struct {
	ctx    context.Context
	cancel func()

	cp           dbconfigs.Connector
	se           *schema.Engine
	startPos     string
	filter       *binlogdatapb.Filter
	send         func([]*binlogdatapb.VEvent) error
	throttlerApp throttlerapp.Name

	vevents        chan *localVSchema
	vschema        *localVSchema
	plans          map[uint64]*streamerPlan
	journalTableID uint64
	versionTableID uint64

	// format and pos are updated by parseEvent.
	format  mysql.BinlogFormat
	pos     replication.Position
	stopPos string

	phase   string
	vse     *Engine
	options *binlogdatapb.VStreamOptions
	config  *vttablet.VReplicationConfig
}

// streamerPlan extends the original plan to also include
// the TableMap, which comes from the binlog. It's used
// to extract values from the ROW events.
type streamerPlan struct {
	*Plan
	TableMap *mysql.TableMap
}

// newVStreamer creates a new vstreamer.
// cp: the mysql conn params.
// sh: the schema engine. The vstreamer uses it to convert the TableMap into field info.
// startPos: a flavor compliant position to stream from. This can also contain the special
//
//	value "current", which means start from the current position.
//
// filter: the list of filtering rules. If a rule has a select expression for its filter,
//
//	the select list can only reference direct columns. No other expressions are allowed.
//	The select expression is allowed to contain the special 'in_keyrange()' function which
//	will return the keyspace id of the row. Examples:
//	"select * from t", same as an empty Filter,
//	"select * from t where in_keyrange('-80')", same as "-80",
//	"select * from t where in_keyrange(col1, 'hash', '-80')",
//	"select col1, col2 from t where...",
//	"select col1, keyspace_id() from t where...".
//	Only "in_keyrange" and limited comparison operators (see enum Opcode in planbuilder.go) are supported in the where clause.
//	Other constructs like joins, group by, etc. are not supported.
//
// vschema: the current vschema. This value can later be changed through the SetVSchema method.
// send: callback function to send events.
func newVStreamer(ctx context.Context, cp dbconfigs.Connector, se *schema.Engine, startPos string, stopPos string,
	filter *binlogdatapb.Filter, vschema *localVSchema, throttlerApp throttlerapp.Name,
	send func([]*binlogdatapb.VEvent) error, phase string, vse *Engine, options *binlogdatapb.VStreamOptions) *vstreamer {

	config, err := GetVReplicationConfig(options)
	if err != nil {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	return &vstreamer{
		ctx:          ctx,
		cancel:       cancel,
		cp:           cp,
		se:           se,
		startPos:     startPos,
		stopPos:      stopPos,
		throttlerApp: throttlerApp,
		filter:       filter,
		send:         send,
		vevents:      make(chan *localVSchema, 1),
		vschema:      vschema,
		plans:        make(map[uint64]*streamerPlan),
		phase:        phase,
		vse:          vse,
		options:      options,
		config:       config,
	}
}

// SetVSchema updates the vstreamer against the new vschema.
func (vs *vstreamer) SetVSchema(vschema *localVSchema) {
	// Since vs.Stream is a single-threaded loop. We just send an event to
	// that thread, which helps us avoid mutexes to update the plans.
	select {
	case vs.vevents <- vschema:
	case <-vs.ctx.Done():
	default: // if there is a pending vschema in the channel, drain it and update it with the latest one
		select {
		case <-vs.vevents:
			vs.vevents <- vschema
		default:
		}
	}
}

// Cancel stops the streaming.
func (vs *vstreamer) Cancel() {
	vs.cancel()
}

// Stream streams binlog events.
func (vs *vstreamer) Stream() error {
	ctx := context.Background()
	vs.vse.vstreamerCount.Add(1)
	defer func() {
		ctx.Done()
		vs.vse.vstreamerCount.Add(-1)
	}()
	vs.vse.vstreamersCreated.Add(1)
	log.Infof("Starting Stream() with startPos %s", vs.startPos)
	pos, err := replication.DecodePosition(vs.startPos)
	if err != nil {
		vs.vse.errorCounts.Add("StreamRows", 1)
		vs.vse.vstreamersEndedWithErrors.Add(1)
		return vterrors.Wrapf(err, "failed to determine starting position")
	}
	vs.pos = pos
	return vs.replicate(ctx)
}

// Stream streams binlog events.
func (vs *vstreamer) replicate(ctx context.Context) error {
	// Ensure se is Open. If vttablet came up in a non_serving role,
	// the schema engine may not have been initialized.
	if err := vs.se.Open(); err != nil {
		return wrapError(err, vs.pos, vs.vse)
	}

	conn, err := binlog.NewBinlogConnection(vs.cp)
	if err != nil {
		return wrapError(err, vs.pos, vs.vse)
	}
	defer conn.Close()

	events, errs, err := conn.StartBinlogDumpFromPosition(vs.ctx, "", vs.pos)
	if err != nil {
		return wrapError(err, vs.pos, vs.vse)
	}
	err = vs.parseEvents(vs.ctx, events, errs)
	return wrapError(err, vs.pos, vs.vse)
}

// parseEvents parses and sends events.
func (vs *vstreamer) parseEvents(ctx context.Context, events <-chan mysql.BinlogEvent, errs <-chan error) error {
	// bufferAndTransmit uses bufferedEvents and curSize to buffer events.
	var (
		bufferedEvents []*binlogdatapb.VEvent
		curSize        int
	)

	// Only the following patterns are possible:
	// BEGIN->ROWs or Statements->GTID->COMMIT. In the case of large transactions, this can be broken into chunks.
	// BEGIN->JOURNAL->GTID->COMMIT
	// GTID->DDL
	// GTID->OTHER
	// HEARTBEAT is issued if there's inactivity, which is likely
	// to happen between one group of events and another.
	//
	// Buffering only takes row or statement lengths into consideration.
	// Length of other events is considered negligible.
	// If a new row event causes the packet size to be exceeded,
	// all existing rows are sent without the new row.
	// If a single row exceeds the packet size, it will be in its own packet.
	bufferAndTransmit := func(vevent *binlogdatapb.VEvent) error {
		vevent.Keyspace = vs.vse.keyspace
		vevent.Shard = vs.vse.shard

		switch vevent.Type {
		case binlogdatapb.VEventType_GTID, binlogdatapb.VEventType_BEGIN, binlogdatapb.VEventType_FIELD,
			binlogdatapb.VEventType_JOURNAL:
			// We never have to send GTID, BEGIN, FIELD events on their own.
			// A JOURNAL event is always preceded by a BEGIN and followed by a COMMIT.
			// So, we don't have to send it right away.
			bufferedEvents = append(bufferedEvents, vevent)
		case binlogdatapb.VEventType_COMMIT, binlogdatapb.VEventType_DDL, binlogdatapb.VEventType_OTHER,
			binlogdatapb.VEventType_HEARTBEAT, binlogdatapb.VEventType_VERSION:
			// COMMIT, DDL, OTHER and HEARTBEAT must be immediately sent.
			// Although unlikely, it's possible to get a HEARTBEAT in the middle
			// of a transaction. If so, we still send the partial transaction along
			// with the heartbeat.
			bufferedEvents = append(bufferedEvents, vevent)
			vevents := bufferedEvents
			bufferedEvents = nil
			curSize = 0
			return vs.send(vevents)
		case binlogdatapb.VEventType_INSERT, binlogdatapb.VEventType_DELETE, binlogdatapb.VEventType_UPDATE, binlogdatapb.VEventType_REPLACE:
			newSize := len(vevent.GetDml())
			if curSize+newSize > vs.config.VStreamPacketSize {
				vs.vse.vstreamerNumPackets.Add(1)
				vevents := bufferedEvents
				bufferedEvents = []*binlogdatapb.VEvent{vevent}
				curSize = newSize
				return vs.send(vevents)
			}
			curSize += newSize
			bufferedEvents = append(bufferedEvents, vevent)
		case binlogdatapb.VEventType_ROW:
			// ROW events happen inside transactions. So, we can chunk them.
			// Buffer everything until packet size is reached, and then send.
			newSize := 0
			for _, rowChange := range vevent.RowEvent.RowChanges {
				if rowChange.Before != nil {
					newSize += len(rowChange.Before.Values)
				}
				if rowChange.After != nil {
					newSize += len(rowChange.After.Values)
				}
			}
			if curSize+newSize > vs.config.VStreamPacketSize {
				vs.vse.vstreamerNumPackets.Add(1)
				vevents := bufferedEvents
				bufferedEvents = []*binlogdatapb.VEvent{vevent}
				curSize = newSize
				return vs.send(vevents)
			}
			curSize += newSize
			bufferedEvents = append(bufferedEvents, vevent)
		default:
			vs.vse.errorCounts.Add("BufferAndTransmit", 1)
			return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "usupported event type %s found for event: %+v",
				vevent.Type.String(), vevent)
		}
		return nil
	}

	// Main loop: calls bufferAndTransmit as events arrive.
	hbTimer := time.NewTimer(HeartbeatTime)
	defer hbTimer.Stop()

	injectHeartbeat := func(throttled bool, throttledReason string) error {
		select {
		case <-ctx.Done():
			return vterrors.Errorf(vtrpcpb.Code_CANCELED, "context has expired")
		default:
			now := time.Now().UnixNano()
			err := bufferAndTransmit(&binlogdatapb.VEvent{
				Type:            binlogdatapb.VEventType_HEARTBEAT,
				Timestamp:       now / 1e9,
				CurrentTime:     now,
				Throttled:       throttled,
				ThrottledReason: throttledReason,
			})
			return err
		}
	}

	logger := logutil.NewThrottledLogger(vs.vse.GetTabletInfo(), throttledLoggerInterval)
	wfNameLog := ""
	if vs.filter != nil && vs.filter.WorkflowName != "" {
		wfNameLog = fmt.Sprintf(" in workflow %s", vs.filter.WorkflowName)
	}
	throttleEvents := func(throttledEvents chan mysql.BinlogEvent) {
		for {
			// Check throttler.
			if checkResult, ok := vs.vse.throttlerClient.ThrottleCheckOKOrWaitAppName(ctx, vs.throttlerApp); !ok {
				// Make sure to leave if context is cancelled.
				select {
				case <-ctx.Done():
					return
				default:
					// Do nothing special.
				}
				logger.Infof("vstreamer throttled%s: %s.", wfNameLog, checkResult.Summary())
				continue
			}
			select {
			case ev, ok := <-events:
				if ok {
					select {
					case throttledEvents <- ev:
					case <-ctx.Done():
						return
					}
				} else {
					close(throttledEvents)
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}
	// throttledEvents can be read just like you would read from events
	// throttledEvents pulls data from events, but throttles pulling data,
	// which in turn blocks the BinlogConnection from pushing events to the channel
	throttledEvents := make(chan mysql.BinlogEvent)
	go throttleEvents(throttledEvents)

	for {
		hbTimer.Reset(HeartbeatTime)
		// Drain event if timer fired before reset.
		select {
		case <-hbTimer.C:
		default:
		}

		select {
		case ev, ok := <-throttledEvents:
			if !ok {
				select {
				case err := <-errs:
					return err
				case <-ctx.Done():
					return nil
				default:
				}
				return vterrors.Errorf(vtrpcpb.Code_ABORTED, "unexpected server EOF while parsing events")
			}
			vevents, err := vs.parseEvent(ev, bufferAndTransmit)
			if err != nil {
				vs.vse.errorCounts.Add("ParseEvent", 1)
				return err
			}
			for _, vevent := range vevents {
				if err := bufferAndTransmit(vevent); err != nil {
					if err == io.EOF {
						return nil
					}
					vs.vse.errorCounts.Add("BufferAndTransmit", 1)
					return vterrors.Wrapf(err, "error sending event: %+v", vevent)
				}
			}
		case vs.vschema = <-vs.vevents:
			select {
			case err := <-errs:
				return err
			case <-ctx.Done():
				return nil
			default:
				if err := vs.rebuildPlans(); err != nil {
					return vterrors.Wrap(err, "failed to rebuild replication plans")
				}
			}
		case err := <-errs:
			return err
		case <-ctx.Done():
			return nil
		case <-hbTimer.C:
			checkResult, ok := vs.vse.throttlerClient.ThrottleCheckOK(ctx, vs.throttlerApp)
			if err := injectHeartbeat(!ok, checkResult.Summary()); err != nil {
				if err == io.EOF {
					return nil
				}
				vs.vse.errorCounts.Add("Send", 1)
				return vterrors.Wrapf(err, "failed to send heartbeat event")
			}
		}
	}
}

// parseEvent parses an event from the binlog and converts it to a list of VEvents.
// The bufferAndTransmit function must be passed if the event is a TransactionPayloadEvent
// as for larger payloads (> ZstdInMemoryDecompressorMaxSize) the internal events need
// to be streamed directly here in order to avoid holding the entire payload's contents,
// which can be 10s or even 100s of GiBs, all in memory.
func (vs *vstreamer) parseEvent(ev mysql.BinlogEvent, bufferAndTransmit func(vevent *binlogdatapb.VEvent) error) ([]*binlogdatapb.VEvent, error) {
	if !ev.IsValid() {
		return nil, fmt.Errorf("can't parse binlog event: invalid data: %#v", ev)
	}

	// We need to keep checking for FORMAT_DESCRIPTION_EVENT even after we've
	// seen one, because another one might come along (e.g. on log rotate due to
	// binlog settings change) that changes the format.
	if ev.IsFormatDescription() {
		var err error
		vs.format, err = ev.Format()
		if err != nil {
			return nil, fmt.Errorf("can't parse FORMAT_DESCRIPTION_EVENT: %v, event data: %#v", err, ev)
		}
		return nil, nil
	}

	// We can't parse anything until we get a FORMAT_DESCRIPTION_EVENT that
	// tells us the size of the event header.
	if vs.format.IsZero() {
		// The only thing that should come before the FORMAT_DESCRIPTION_EVENT
		// is a fake ROTATE_EVENT, which the primary sends to tell us the name
		// of the current log file.
		if ev.IsRotate() {
			return nil, nil
		}
		return nil, fmt.Errorf("got a real event before FORMAT_DESCRIPTION_EVENT: %#v", ev)
	}

	// Strip the checksum, if any. We don't actually verify the checksum, so discard it.
	ev, _, err := ev.StripChecksum(vs.format)
	if err != nil {
		return nil, fmt.Errorf("can't strip checksum from binlog event: %v, event data: %#v", err, ev)
	}

	var vevents []*binlogdatapb.VEvent
	switch {
	case ev.IsGTID():
		gtid, hasBegin, err := ev.GTID(vs.format)
		if err != nil {
			return nil, vterrors.Wrapf(err, "failed to get GTID from binlog event: %#v", ev)
		}
		if hasBegin {
			vevents = append(vevents, &binlogdatapb.VEvent{
				Type: binlogdatapb.VEventType_BEGIN,
			})
		}
		vs.pos = replication.AppendGTID(vs.pos, gtid)
	case ev.IsXID():
		vevents = append(vevents, &binlogdatapb.VEvent{
			Type: binlogdatapb.VEventType_GTID,
			Gtid: replication.EncodePosition(vs.pos),
		}, &binlogdatapb.VEvent{
			Type: binlogdatapb.VEventType_COMMIT,
		})
	case ev.IsQuery():
		q, err := ev.Query(vs.format)
		if err != nil {
			return nil, vterrors.Wrapf(err, "failed to get query from binlog event: %#v", ev)
		}
		// Insert/Delete/Update are supported only to be used in the context of external mysql streams where source databases
		// could be using SBR. Vitess itself will never run into cases where it needs to consume non rbr statements.
		switch cat := sqlparser.Preview(q.SQL); cat {
		case sqlparser.StmtInsert:
			mustSend := mustSendStmt(q, vs.cp.DBName())
			if mustSend {
				vevents = append(vevents, &binlogdatapb.VEvent{
					Type: binlogdatapb.VEventType_INSERT,
					Dml:  q.SQL,
				})
			}
		case sqlparser.StmtUpdate:
			mustSend := mustSendStmt(q, vs.cp.DBName())
			if mustSend {
				vevents = append(vevents, &binlogdatapb.VEvent{
					Type: binlogdatapb.VEventType_UPDATE,
					Dml:  q.SQL,
				})
			}
		case sqlparser.StmtDelete:
			mustSend := mustSendStmt(q, vs.cp.DBName())
			if mustSend {
				vevents = append(vevents, &binlogdatapb.VEvent{
					Type: binlogdatapb.VEventType_DELETE,
					Dml:  q.SQL,
				})
			}
		case sqlparser.StmtReplace:
			mustSend := mustSendStmt(q, vs.cp.DBName())
			if mustSend {
				vevents = append(vevents, &binlogdatapb.VEvent{
					Type: binlogdatapb.VEventType_REPLACE,
					Dml:  q.SQL,
				})
			}
		case sqlparser.StmtBegin:
			vevents = append(vevents, &binlogdatapb.VEvent{
				Type: binlogdatapb.VEventType_BEGIN,
			})
		case sqlparser.StmtCommit:
			vevents = append(vevents, &binlogdatapb.VEvent{
				Type: binlogdatapb.VEventType_COMMIT,
			})
		case sqlparser.StmtDDL:
			if mustSendDDL(q, vs.cp.DBName(), vs.filter, vs.vse.env.Environment().Parser()) {
				vevents = append(vevents, &binlogdatapb.VEvent{
					Type: binlogdatapb.VEventType_GTID,
					Gtid: replication.EncodePosition(vs.pos),
				}, &binlogdatapb.VEvent{
					Type:      binlogdatapb.VEventType_DDL,
					Statement: q.SQL,
				})
			} else {
				// If the DDL need not be sent, send a dummy OTHER event.
				vevents = append(vevents, &binlogdatapb.VEvent{
					Type: binlogdatapb.VEventType_GTID,
					Gtid: replication.EncodePosition(vs.pos),
				}, &binlogdatapb.VEvent{
					Type: binlogdatapb.VEventType_OTHER,
				})
			}
			if schema.MustReloadSchemaOnDDL(q.SQL, vs.cp.DBName(), vs.vse.env.Environment().Parser()) {
				vs.se.ReloadAt(context.Background(), vs.pos)
			}
		case sqlparser.StmtSavepoint:
			// We currently completely skip `SAVEPOINT ...` statements.
			//
			// MySQL inserts `SAVEPOINT ...` statements into the binlog in row based, statement based
			// and in mixed replication modes, but only ever writes `ROLLBACK TO ...` statements to the
			// binlog in mixed or statement based replication modes. Without `ROLLBACK TO ...` statements,
			// savepoints are side-effect free.
			//
			// Vitess only supports row based replication, so skipping the creation of savepoints
			// reduces the amount of data send over to vplayer.
		case sqlparser.StmtOther, sqlparser.StmtAnalyze, sqlparser.StmtPriv, sqlparser.StmtSet, sqlparser.StmtComment, sqlparser.StmtFlush:
			// These are either:
			// 1) DBA statements like REPAIR that can be ignored.
			// 2) Privilege-altering statements like GRANT/REVOKE
			//    that we want to keep out of the stream for now.
			vevents = append(vevents, &binlogdatapb.VEvent{
				Type: binlogdatapb.VEventType_GTID,
				Gtid: replication.EncodePosition(vs.pos),
			}, &binlogdatapb.VEvent{
				Type: binlogdatapb.VEventType_OTHER,
			})
		default:
			return nil, fmt.Errorf("unexpected statement type %s in row-based replication: %q", cat, q.SQL)
		}
	case ev.IsTableMap():
		// This is very frequent. It precedes every row event.
		// If it's the first time for a table, we generate a FIELD
		// event, and also cache the plan. Subsequent TableMap events
		// for that table id don't generate VEvents.
		// A schema change will result in a change in table id, which
		// will generate a new plan and FIELD event.
		id := ev.TableID(vs.format)

		tm, err := ev.TableMap(vs.format)
		if err != nil {
			return nil, vterrors.Wrapf(err, "failed to parse table map from binlog event: %#v", ev)
		}
		if plan, ok := vs.plans[id]; ok {
			// When the underlying mysql server restarts the table map can change.
			// Usually the vstreamer will also error out when this happens, and vstreamer re-initializes its table map.
			// But if the vstreamer is not aware of the restart, we could get an id that matches one in the cache, but
			// is for a different table. We then invalidate and recompute the plan for this id.
			isInternal := tm.Database == sidecar.GetName()
			if plan == nil ||
				(plan.Table.Name == tm.Name && isInternal == plan.IsInternal) {
				return nil, nil
			}
			vs.plans[id] = nil
			log.Infof("table map changed: id %d for %s has changed to %s", id, plan.Table.Name, tm.Name)
		}

		// The database connector `vs.cp` points to the keyspace's database.
		// If this is also setup as the sidecar database name, as is the case in the distributed transaction unit tests,
		// for example, we stream all tables as usual.
		// If not, we only stream the schema_version and journal tables and those specified in the internal_tables list.
		if tm.Database == sidecar.GetName() && vs.cp.DBName() != sidecar.GetName() {
			return vs.buildSidecarTablePlan(id, tm)
		}

		if tm.Database != "" && tm.Database != vs.cp.DBName() {
			vs.plans[id] = nil
			return nil, nil
		}
		if vtschema.IsInternalOperationTableName(tm.Name) { // ignore tables created by onlineddl/GC
			vs.plans[id] = nil
			return nil, nil
		}
		if !ruleMatches(tm.Name, vs.filter) {
			return nil, nil
		}

		vevent, err := vs.buildTablePlan(id, tm)
		if err != nil {
			vs.vse.errorCounts.Add("TablePlan", 1)
			return nil, vterrors.Wrapf(err, "failed to build table replication plan for table %s", tm.Name)
		}
		if vevent != nil {
			vevents = append(vevents, vevent)
		}
	case ev.IsWriteRows() || ev.IsDeleteRows() || ev.IsUpdateRows() || ev.IsPartialUpdateRows():
		// The existence of before and after images can be used to
		// identify statement types. It's also possible that the
		// before and after images end up going to different shards.
		// If so, an update will be treated as delete on one shard
		// and insert on the other.
		id := ev.TableID(vs.format)
		plan := vs.plans[id]
		if plan == nil {
			return nil, nil
		}
		rows, err := ev.Rows(vs.format, plan.TableMap)
		if err != nil {
			return nil, err
		}

		switch id {
		case vs.journalTableID:
			vevents, err = vs.processJournalEvent(vevents, plan, rows)
		case vs.versionTableID:
			vs.se.RegisterVersionEvent()
			vevent := &binlogdatapb.VEvent{
				Type: binlogdatapb.VEventType_VERSION,
			}
			vevents = append(vevents, vevent)

		default:
			vevents, err = vs.processRowEvent(vevents, plan, rows)
		}
		if err != nil {
			return nil, err
		}
	case ev.IsTransactionPayload():
		if !vs.pos.MatchesFlavor(replication.Mysql56FlavorID) {
			return nil, fmt.Errorf("compressed transaction payload events are not supported with database flavor %s",
				vs.vse.env.Config().DB.Flavor)
		}
		tp, err := ev.TransactionPayload(vs.format)
		if err != nil {
			return nil, err
		}
		defer tp.Close()
		// Events inside the payload don't have their own checksum.
		ogca := vs.format.ChecksumAlgorithm
		defer func() { vs.format.ChecksumAlgorithm = ogca }()
		vs.format.ChecksumAlgorithm = mysql.BinlogChecksumAlgOff
		for {
			tpevent, err := tp.GetNextEvent()
			if err != nil {
				if err == io.EOF {
					break
				}
				return nil, err
			}
			tpvevents, err := vs.parseEvent(tpevent, nil) // Parse the internal event
			if err != nil {
				return nil, vterrors.Wrap(err, "failed to parse transaction payload's internal event")
			}
			if tp.StreamingContents {
				// Transmit each internal event individually to avoid buffering
				// the large transaction's entire payload of events in memory, as
				// the uncompressed size can be 10s or even 100s of GiBs in size.
				if bufferAndTransmit == nil {
					return nil, vterrors.New(vtrpcpb.Code_INTERNAL, "[bug] cannot stream compressed transaction payload's internal events as no bufferAndTransmit function was provided")
				}
				for _, tpvevent := range tpvevents {
					tpvevent.Timestamp = int64(ev.Timestamp())
					tpvevent.CurrentTime = time.Now().UnixNano()
					if err := bufferAndTransmit(tpvevent); err != nil {
						if err == io.EOF {
							return nil, nil
						}
						vs.vse.errorCounts.Add("TransactionPayloadBufferAndTransmit", 1)
						return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "error sending compressed transaction payload's internal event: %v", err)
					}
				}
			} else { // Process the payload's internal events all at once
				vevents = append(vevents, tpvevents...)
			}
		}
		vs.vse.vstreamerCompressedTransactionsDecoded.Add(1)
	}
	for _, vevent := range vevents {
		vevent.Timestamp = int64(ev.Timestamp())
		vevent.CurrentTime = time.Now().UnixNano()
	}
	return vevents, nil
}

func (vs *vstreamer) buildSidecarTablePlan(id uint64, tm *mysql.TableMap) ([]*binlogdatapb.VEvent, error) {
	tableName := tm.Name
	switch tableName {
	case "resharding_journal":
		// A journal is a special case that generates a JOURNAL event.
	case "schema_version":
		// Generates a Version event when it detects that a schema is stored in the schema_version table.

		// SkipMetaCheck is set during PITR restore: some table metadata is not fetched in that case.
		if vs.se.SkipMetaCheck {
			return nil, nil
		}
	default:
		if vs.options == nil {
			return nil, nil
		}
		found := false
		for _, table := range vs.options.InternalTables {
			if table == tableName {
				found = true
				break
			}
		}
		if !found {
			return nil, nil
		}
	}

	conn, err := vs.cp.Connect(vs.ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	qr, err := conn.ExecuteFetch(sqlparser.BuildParsedQuery("select * from %s.%s where 1 != 1",
		sidecar.GetIdentifier(), tableName).Query, 1, true)
	if err != nil {
		return nil, err
	}
	fields := qr.Fields
	if len(fields) < len(tm.Types) {
		return nil, fmt.Errorf("cannot determine table columns for %s: event has %v, schema has %v", tm.Name, tm.Types, fields)
	}
	table := &Table{
		Name:   tableName,
		Fields: fields[:len(tm.Types)],
	}

	// Build a normal table plan, which means, return all rows
	// and columns as is. Special handling may be done when we actually
	// receive the row event, example: we'll build a JOURNAL or VERSION event instead.
	plan, err := buildREPlan(vs.se.Environment(), table, nil, "")
	if err != nil {
		return nil, err
	}
	plan.IsInternal = true
	vs.plans[id] = &streamerPlan{
		Plan:     plan,
		TableMap: tm,
	}

	var vevents []*binlogdatapb.VEvent
	switch tm.Name {
	case "resharding_journal":
		vs.journalTableID = id
	case "schema_version":
		vs.versionTableID = id
	default:
		vevents = append(vevents, &binlogdatapb.VEvent{
			Type: binlogdatapb.VEventType_FIELD,
			FieldEvent: &binlogdatapb.FieldEvent{
				TableName:       tableName,
				Fields:          plan.fields(),
				Keyspace:        vs.vse.keyspace,
				Shard:           vs.vse.shard,
				IsInternalTable: plan.IsInternal,
			}})
	}
	return vevents, nil
}

func (vs *vstreamer) buildTablePlan(id uint64, tm *mysql.TableMap) (*binlogdatapb.VEvent, error) {
	cols, err := vs.buildTableColumns(tm)
	if err != nil {
		return nil, err
	}
	table := &Table{
		Name:   tm.Name,
		Fields: cols,
	}
	plan, err := buildPlan(vs.se.Environment(), table, vs.vschema, vs.filter)
	if err != nil {
		return nil, err
	}
	if plan == nil {
		vs.plans[id] = nil
		return nil, nil
	}
	if err := addEnumAndSetMappingstoPlan(vs.se.Environment(), plan, cols, tm.Metadata); err != nil {
		return nil, vterrors.Wrapf(err, "failed to build ENUM and SET column integer to string mappings")
	}
	vs.plans[id] = &streamerPlan{
		Plan:     plan,
		TableMap: tm,
	}
	return &binlogdatapb.VEvent{
		Type: binlogdatapb.VEventType_FIELD,
		FieldEvent: &binlogdatapb.FieldEvent{
			TableName: plan.Table.Name,
			Fields:    plan.fields(),
			Keyspace:  vs.vse.keyspace,
			Shard:     vs.vse.shard,
			// This mapping will be done, if needed, in the vstreamer when we process
			// and build ROW events.
			EnumSetStringValues: len(plan.EnumSetValuesMap) > 0,
		},
	}, nil
}

func (vs *vstreamer) buildTableColumns(tm *mysql.TableMap) ([]*querypb.Field, error) {
	var fields []*querypb.Field
	var txtFieldIdx int
	for i, typ := range tm.Types {
		t, err := sqltypes.MySQLToType(typ, 0)
		if err != nil {
			return nil, fmt.Errorf("unsupported type: %d, position: %d", typ, i)
		}
		// Use the collation inherited or the one specified explicitly for the
		// column if one was provided in the event's optional metadata (MySQL only
		// provides this for text based columns).
		var coll collations.ID
		switch {
		case sqltypes.IsText(t) && len(tm.ColumnCollationIDs) > txtFieldIdx:
			coll = tm.ColumnCollationIDs[txtFieldIdx]
			txtFieldIdx++
		case t == sqltypes.TypeJSON:
			// JSON is a blob at this (storage) layer -- vs the connection/query serving
			// layer which CollationForType seems primarily concerned about and JSON at
			// the response layer should be using utf-8 as that's the standard -- so we
			// should NOT use utf8mb4 as the collation in MySQL for a JSON column is
			// NULL, meaning there is not one (same as for int) and we should use binary.
			coll = collations.CollationBinaryID
		default: // Use the server defined default for the column's type
			coll = collations.CollationForType(t, vs.se.Environment().CollationEnv().DefaultConnectionCharset())
		}
		fields = append(fields, &querypb.Field{
			Name:    fmt.Sprintf("@%d", i+1),
			Type:    t,
			Charset: uint32(coll),
			Flags:   mysql.FlagsForColumn(t, coll),
		})
	}
	st, err := vs.se.GetTableForPos(vs.ctx, sqlparser.NewIdentifierCS(tm.Name), replication.EncodePosition(vs.pos))
	if err != nil {
		if vs.filter.FieldEventMode == binlogdatapb.Filter_ERR_ON_MISMATCH {
			log.Infof("No schema found for table %s", tm.Name)
			return nil, fmt.Errorf("unknown table %v in schema", tm.Name)
		}
		return fields, nil
	}

	if len(st.Fields) < len(tm.Types) {
		if vs.filter.FieldEventMode == binlogdatapb.Filter_ERR_ON_MISMATCH {
			log.Infof("Cannot determine columns for table %s", tm.Name)
			return nil, fmt.Errorf("cannot determine table columns for %s: event has %v, schema has %v", tm.Name, tm.Types, st.Fields)
		}
		return fields, nil
	}

	// Check if the schema returned by schema.Engine matches with row.
	for i := range tm.Types {
		if !sqltypes.AreTypesEquivalent(fields[i].Type, st.Fields[i].Type) {
			return fields, nil
		}
	}

	// Columns should be truncated to match those in tm.
	// This uses the historian which queries the columns in the table and uses the
	// generated fields metadata. This means that the fields for text types are
	// initially using collations for the column types based on the *connection
	// collation* and not the actual *column collation*.
	// But because we now get the correct collation for the actual column from
	// mysqld in getExtColInfos we know this is the correct one for the vstream
	// target and we use that rather than any that were in the binlog events,
	// which were for the source and which can be using a different collation
	// than the target.
	fieldsCopy, err := getFields(vs.ctx, vs.cp, vs.se, tm.Name, tm.Database, st.Fields[:len(tm.Types)])
	if err != nil {
		return nil, err
	}
	return fieldsCopy, nil
}

func getExtColInfos(ctx context.Context, cp dbconfigs.Connector, se *schema.Engine, table, database string) (map[string]*extColInfo, error) {
	extColInfos := make(map[string]*extColInfo)
	conn, err := cp.Connect(ctx)
	if err != nil {
		return nil, vterrors.Wrapf(err, "failed to connect to database %s", database)
	}
	defer conn.Close()
	queryTemplate := "select column_name, column_type, collation_name from information_schema.columns where table_schema=%s and table_name=%s;"
	query := fmt.Sprintf(queryTemplate, encodeString(database), encodeString(table))
	qr, err := conn.ExecuteFetch(query, 10000, false)
	if err != nil {
		return nil, err
	}
	for _, row := range qr.Rows {
		extColInfo := &extColInfo{
			columnType: row[1].ToString(),
		}
		collationName := row[2].ToString()
		var coll collations.ID
		if row[2].IsNull() || collationName == "" {
			coll = collations.CollationBinaryID
		} else {
			coll = se.Environment().CollationEnv().LookupByName(collationName)
		}
		extColInfo.collationID = coll
		extColInfos[row[0].ToString()] = extColInfo
	}
	return extColInfos, nil
}

func getFields(ctx context.Context, cp dbconfigs.Connector, se *schema.Engine, table, database string, fields []*querypb.Field) ([]*querypb.Field, error) {
	// Make a deep copy of the schema.Engine fields as they are pointers and
	// will be modified by adding ColumnType below
	fieldsCopy := make([]*querypb.Field, len(fields))
	for i, field := range fields {
		fieldsCopy[i] = field.CloneVT()
	}
	extColInfos, err := getExtColInfos(ctx, cp, se, table, database)
	if err != nil {
		return nil, err
	}
	for _, field := range fieldsCopy {
		if colInfo, ok := extColInfos[field.Name]; ok {
			field.ColumnType = colInfo.columnType
			field.Charset = uint32(colInfo.collationID)
		}
	}
	return fieldsCopy, nil
}

// Additional column attributes to get from information_schema.columns.
type extColInfo struct {
	columnType  string
	collationID collations.ID
}

func encodeString(in string) string {
	return sqltypes.EncodeStringSQL(in)
}

func (vs *vstreamer) processJournalEvent(vevents []*binlogdatapb.VEvent, plan *streamerPlan, rows mysql.Rows) ([]*binlogdatapb.VEvent, error) {
	// Get DbName
	params, err := vs.cp.MysqlParams()
	if err != nil {
		return nil, err
	}
nextrow:
	for _, row := range rows.Rows {
		afterValues, _, _, err := vs.getValues(plan, row.Data, rows.DataColumns, row.NullColumns, row.JSONPartialValues)
		if err != nil {
			return nil, vterrors.Wrap(err, "failed to extract journal from binlog event and apply filters")
		}
		if len(afterValues) == 0 {
			continue
		}
		// Exclude events that don't match the db_name.
		for i, fld := range plan.fields() {
			if fld.Name == "db_name" && afterValues[i].ToString() != params.DbName {
				continue nextrow
			}
		}
		for i, fld := range plan.fields() {
			if fld.Name != "val" {
				continue
			}
			journal := &binlogdatapb.Journal{}
			avBytes, err := afterValues[i].ToBytes()
			if err != nil {
				return nil, err
			}
			if err := prototext.Unmarshal(avBytes, journal); err != nil {
				return nil, vterrors.Wrap(err, "failed to unmarshal journal event")
			}
			vevents = append(vevents, &binlogdatapb.VEvent{
				Type:    binlogdatapb.VEventType_JOURNAL,
				Journal: journal,
			})
		}
	}
	return vevents, nil
}

// processRowEvent converts binlog rows into row vevents using the following steps:
//   - converts the raw before and after binlog images into Values
//   - finds which before or after images passes the filter criterion
//   - if the target is sharded, pass only images that pass
//   - if the target is not sharded, pass both images if either after or before passes
func (vs *vstreamer) processRowEvent(vevents []*binlogdatapb.VEvent, plan *streamerPlan, rows mysql.Rows) ([]*binlogdatapb.VEvent, error) {
	rowChanges := make([]*binlogdatapb.RowChange, 0, len(rows.Rows))
	for _, row := range rows.Rows {
		// The BEFORE image does not have partial JSON values so we pass an empty bitmap.
		beforeRawValues, beforeCharsets, _, err := vs.getValues(plan, row.Identify, rows.IdentifyColumns, row.NullIdentifyColumns, mysql.Bitmap{})
		if err != nil {
			return nil, err
		}
		beforeOK, beforeHasVindex, err := plan.shouldFilter(beforeRawValues, beforeCharsets)
		if err != nil {
			return nil, err
		}

		// The AFTER image is where we may have partial JSON values, as reflected in the
		// row's JSONPartialValues bitmap.
		afterRawValues, afterCharsets, partial, err := vs.getValues(plan, row.Data, rows.DataColumns, row.NullColumns, row.JSONPartialValues)
		if err != nil {
			return nil, err
		}
		afterOK, afterHasVindex, err := plan.shouldFilter(afterRawValues, afterCharsets)
		if err != nil {
			return nil, err
		}

		hasVindex := beforeHasVindex || afterHasVindex
		if !afterOK && !beforeOK {
			// both before and after images are filtered out
			continue
		}

		// at least one image passes the filter and is not a sharded filter
		if !hasVindex {
			// we want both images to be part of the row event if either passes and we are not in a sharded situation
			afterOK = true
			beforeOK = true
		}

		rowChange := &binlogdatapb.RowChange{}
		if beforeOK {
			if len(beforeRawValues) > 0 {
				beforeValues, err := plan.mapValues(beforeRawValues)
				if err != nil {
					return nil, err
				}
				rowChange.Before = sqltypes.RowToProto3(beforeValues)
			}
		}
		if afterOK {
			if len(afterRawValues) > 0 {
				afterValues, err := plan.mapValues(afterRawValues)
				if err != nil {
					return nil, err
				}
				rowChange.After = sqltypes.RowToProto3(afterValues)
				if ((vs.config.ExperimentalFlags /**/ & /**/ vttablet.VReplicationExperimentalFlagAllowNoBlobBinlogRowImage != 0) && partial) ||
					(row.JSONPartialValues.Count() > 0) {
					rowChange.DataColumns = &binlogdatapb.RowChange_Bitmap{
						Count: int64(rows.DataColumns.Count()),
						Cols:  rows.DataColumns.Bits(),
					}
				}
				if row.JSONPartialValues.Count() > 0 {
					rowChange.JsonPartialValues = &binlogdatapb.RowChange_Bitmap{
						Count: int64(row.JSONPartialValues.Count()),
						Cols:  row.JSONPartialValues.Bits(),
					}
				}
			}
		}
		rowChanges = append(rowChanges, rowChange)
	}
	if len(rowChanges) != 0 {
		vevents = append(vevents, &binlogdatapb.VEvent{
			Type: binlogdatapb.VEventType_ROW,
			RowEvent: &binlogdatapb.RowEvent{
				TableName:       plan.Table.Name,
				RowChanges:      rowChanges,
				Keyspace:        vs.vse.keyspace,
				Shard:           vs.vse.shard,
				Flags:           uint32(rows.Flags),
				IsInternalTable: plan.IsInternal,
			},
		})
	}
	return vevents, nil
}

func (vs *vstreamer) rebuildPlans() error {
	for id, plan := range vs.plans {
		if plan == nil {
			// If a table has no plan, a vschema change will not
			// cause that to change.
			continue
		}
		newPlan, err := buildPlan(vs.se.Environment(), plan.Table, vs.vschema, vs.filter)
		if err != nil {
			return err
		}
		if newPlan == nil {
			continue
		}
		vs.plans[id] = &streamerPlan{
			Plan:     newPlan,
			TableMap: plan.TableMap,
		}
	}
	return nil
}

func (vs *vstreamer) getValues(plan *streamerPlan, data []byte,
	dataColumns, nullColumns mysql.Bitmap, jsonPartialValues mysql.Bitmap) ([]sqltypes.Value, []collations.ID, bool, error) {
	if len(data) == 0 {
		return nil, nil, false, nil
	}
	values := make([]sqltypes.Value, dataColumns.Count())
	charsets := make([]collations.ID, len(values))
	valueIndex := 0
	jsonIndex := 0
	pos := 0
	partial := false
	for colNum := 0; colNum < dataColumns.Count(); colNum++ {
		if !dataColumns.Bit(colNum) {
			if vs.config.ExperimentalFlags /**/ & /**/ vttablet.VReplicationExperimentalFlagAllowNoBlobBinlogRowImage == 0 {
				return nil, nil, false, fmt.Errorf("partial row image encountered: ensure binlog_row_image is set to 'full'")
			} else {
				partial = true
			}
			continue
		}
		if nullColumns.Bit(valueIndex) {
			valueIndex++
			if plan.Table.Fields[colNum].Type == querypb.Type_JSON {
				jsonIndex++
			}
			continue
		}
		partialJSON := false
		if jsonPartialValues.Count() > 0 && plan.Table.Fields[colNum].Type == querypb.Type_JSON {
			partialJSON = jsonPartialValues.Bit(jsonIndex)
			jsonIndex++
		}
		value, l, err := mysqlbinlog.CellValue(data, pos, plan.TableMap.Types[colNum], plan.TableMap.Metadata[colNum], plan.Table.Fields[colNum], partialJSON)
		if err != nil {
			log.Errorf("extractRowAndFilter: %s, table: %s, colNum: %d, fields: %+v, current values: %+v",
				err, plan.Table.Name, colNum, plan.Table.Fields, values)
			return nil, nil, false, vterrors.Wrapf(err, "failed to extract row's value for column %s from binlog event",
				plan.Table.Fields[colNum].Name)
		}
		pos += l

		if !value.IsNull() { // ENUMs and SETs require no special handling if they are NULL
			// If the column is a CHAR based type with a binary collation (e.g. utf8mb4_bin) then the
			// actual column type is included in the second byte of the event metadata while the
			// event's type for the field is BINARY. This is true for ENUM and SET types.
			var mysqlType uint16
			if sqltypes.IsQuoted(plan.Table.Fields[colNum].Type) {
				mysqlType = plan.TableMap.Metadata[colNum] >> 8
			}
			// Convert the integer values in the binlog event for any SET and ENUM fields into their
			// string representations.
			if plan.Table.Fields[colNum].Type == querypb.Type_ENUM || mysqlType == mysqlbinlog.TypeEnum {
				value, err = buildEnumStringValue(vs.se.Environment(), plan, colNum, value)
				if err != nil {
					return nil, nil, false, vterrors.Wrapf(err, "failed to perform ENUM column integer to string value mapping")
				}
			}
			if plan.Table.Fields[colNum].Type == querypb.Type_SET || mysqlType == mysqlbinlog.TypeSet {
				value, err = buildSetStringValue(vs.se.Environment(), plan, colNum, value)
				if err != nil {
					return nil, nil, false, vterrors.Wrapf(err, "failed to perform SET column integer to string value mapping")
				}
			}
		}

		charsets[colNum] = collations.ID(plan.Table.Fields[colNum].Charset)
		values[colNum] = value
		valueIndex++
	}
	return values, charsets, partial, nil
}

// addEnumAndSetMappingstoPlan sets up any necessary ENUM and SET integer to string mappings.
func addEnumAndSetMappingstoPlan(env *vtenv.Environment, plan *Plan, cols []*querypb.Field, metadata []uint16) error {
	plan.EnumSetValuesMap = make(map[int]map[int]string)
	for i, col := range cols {
		// If the column is a CHAR based type with a binary collation (e.g. utf8mb4_bin) then
		// the actual column type is included in the second byte of the event metadata while
		// the event's type for the field is BINARY. This is true for ENUM and SET types.
		var mysqlType uint16
		if sqltypes.IsQuoted(col.Type) {
			mysqlType = metadata[i] >> 8
		}
		if col.Type == querypb.Type_ENUM || mysqlType == mysqlbinlog.TypeEnum ||
			col.Type == querypb.Type_SET || mysqlType == mysqlbinlog.TypeSet {
			// Strip the enum() / set() parts out.
			begin := strings.Index(col.ColumnType, "(")
			end := strings.LastIndex(col.ColumnType, ")")
			if begin == -1 || end == -1 {
				return fmt.Errorf("enum or set column %s does not have valid string values: %s",
					col.Name, col.ColumnType)
			}
			var err error
			plan.EnumSetValuesMap[i], err = vtschema.ParseEnumOrSetTokensMap(env, col.ColumnType[begin+1:end])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// buildEnumStringValue takes the integer value of an ENUM column and returns the string value.
func buildEnumStringValue(env *vtenv.Environment, plan *streamerPlan, colNum int, value sqltypes.Value) (sqltypes.Value, error) {
	if value.IsNull() { // No work is needed
		return value, nil
	}
	// Add the mappings just-in-time in case we haven't properly received and processed a
	// table map event to initialize it.
	if plan.EnumSetValuesMap == nil {
		if err := addEnumAndSetMappingstoPlan(env, plan.Plan, plan.Table.Fields, plan.TableMap.Metadata); err != nil {
			return sqltypes.Value{}, vterrors.Wrap(err, "failed to build ENUM column integer to string mappings")
		}
	}
	// ENUM columns are stored as an unsigned 16-bit integer as they can contain a maximum
	// of 65,535 elements (https://dev.mysql.com/doc/refman/en/enum.html) with the 0 element
	// reserved for any integer value that has no string mapping.
	iv, err := value.ToUint16()
	if err != nil {
		return sqltypes.Value{}, vterrors.Wrapf(err, "no valid integer value found for column %s in table %s, bytes: %b",
			plan.Table.Fields[colNum].Name, plan.Table.Name, iv)
	}
	var strVal string
	// Match the MySQL behavior of returning an empty string for invalid ENUM values.
	// This is what the 0 position in an ENUM is reserved for.
	if iv != 0 {
		var ok bool
		strVal, ok = plan.EnumSetValuesMap[colNum][int(iv)]
		if !ok {
			// The integer value was NOT 0 yet we found no mapping. This should never happen.
			return sqltypes.Value{}, fmt.Errorf("no string value found for ENUM column %s in table %s -- with available values being: %v -- using the found integer value: %d",
				plan.Table.Fields[colNum].Name, plan.Table.Name, plan.EnumSetValuesMap[colNum], iv)
		}
	}
	return sqltypes.MakeTrusted(plan.Table.Fields[colNum].Type, []byte(strVal)), nil
}

// buildSetStringValue takes the integer value of a SET column and returns the string value.
func buildSetStringValue(env *vtenv.Environment, plan *streamerPlan, colNum int, value sqltypes.Value) (sqltypes.Value, error) {
	if value.IsNull() { // No work is needed
		return value, nil
	}
	// Add the mappings just-in-time in case we haven't properly received and processed a
	// table map event to initialize it.
	if plan.EnumSetValuesMap == nil {
		if err := addEnumAndSetMappingstoPlan(env, plan.Plan, plan.Table.Fields, plan.TableMap.Metadata); err != nil {
			return sqltypes.Value{}, vterrors.Wrap(err, "failed to build SET column integer to string mappings")
		}
	}
	// A SET column can have 64 unique values: https://dev.mysql.com/doc/refman/en/set.html
	// For this reason the binlog event contains the values encoded as an unsigned 64-bit
	// integer which is really a bitmap.
	val := bytes.Buffer{}
	iv, err := value.ToUint64()
	if err != nil {
		return value, vterrors.Wrapf(err, "no valid integer value found for column %s in table %s, bytes: %b",
			plan.Table.Fields[colNum].Name, plan.Table.Name, iv)
	}
	idx := 1
	// See what bits are set in the bitmap using bitmasks.
	for b := uint64(1); b < 1<<63; b <<= 1 {
		if iv&b > 0 { // This bit is set and the SET's string value needs to be provided.
			strVal, ok := plan.EnumSetValuesMap[colNum][idx]
			// When you insert values not found in the SET (which requires disabling STRICT mode) then
			// they are effectively pruned and ignored (not actually saved). So this should never happen.
			if !ok {
				return sqltypes.Value{}, fmt.Errorf("no valid integer value found for SET column %s in table %s, bytes: %b",
					plan.Table.Fields[colNum].Name, plan.Table.Name, iv)
			}
			if val.Len() > 0 {
				val.WriteByte(',')
			}
			val.WriteString(strVal)
		}
		idx++
	}
	return sqltypes.MakeTrusted(plan.Table.Fields[colNum].Type, val.Bytes()), nil
}

func wrapError(err error, stopPos replication.Position, vse *Engine) error {
	if err != nil {
		vse.vstreamersEndedWithErrors.Add(1)
		vse.errorCounts.Add("StreamEnded", 1)
		err = fmt.Errorf("stream (at source tablet) error @ %v: %v", stopPos, err)
		log.Error(err)
		return err
	}
	log.Infof("stream (at source tablet) ended @ %v", stopPos)
	return nil
}
