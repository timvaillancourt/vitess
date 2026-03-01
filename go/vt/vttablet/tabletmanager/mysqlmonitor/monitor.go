/*
Copyright 2025 The Vitess Authors.

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

package mysqlmonitor

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"vitess.io/vitess/go/constants/sidecar"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/timer"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/dbconnpool"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

const (
	// How many seconds we should wait for table/metadata locks.
	// We do NOT want our TRUNCATE statement to block things indefinitely, and
	// we do NOT want our INSERTs blocking indefinitely on any locks to appear
	// as though they are blocking on a semi-sync ACK, which is what we really
	// care about in the monitor as when we hit the limit of writers blocked
	// on semi-sync ACKs we signal to VTOrc that we need help to unblock
	// things and it will perform an ERS to do so.
	// Note: this is something we are entirely fine being set in all of the
	// monitor connection pool sessions, so we do not ever bother to set the
	// session value back to the global default.
	setLockWaitTimeoutQuery = "SET SESSION lock_wait_timeout=%d"

	semiSyncStatsQuery     = "SELECT /*+ MAX_EXECUTION_TIME(%d) */ variable_name, variable_value FROM performance_schema.global_status WHERE REGEXP_LIKE(variable_name, 'Rpl_semi_sync_(source|master)_(wait_sessions|yes_tx)')"
	semiSyncHeartbeatWrite = "INSERT INTO %s.semisync_heartbeat (ts) VALUES (NOW())"
	semiSyncHeartbeatClear = "TRUNCATE TABLE %s.semisync_heartbeat"
	maxWritesPermitted     = 15
	clearTimerDuration     = 24 * time.Hour

	mysqlAliveQuery      = "SELECT 1"
	semiSyncEnabledQuery = "SELECT /*+ MAX_EXECUTION_TIME(%d) */ variable_value FROM performance_schema.global_variables WHERE variable_name IN ('rpl_semi_sync_source_enabled', 'rpl_semi_sync_master_enabled') LIMIT 1"
	semiSyncRunningQuery = "SELECT /*+ MAX_EXECUTION_TIME(%d) */ variable_value FROM performance_schema.global_status WHERE variable_name IN ('Rpl_semi_sync_source_status', 'Rpl_semi_sync_master_status') LIMIT 1"
)

type (
	semiSyncStats struct {
		waitingSessions, ackedTrxs int64
	}

	// Monitor is a monitor that checks MySQL health and whether the primary tablet
	// is blocked on a semi-sync ack from the replica.
	// If the semi-sync ACK is lost in the network,
	// it is possible that the primary is indefinitely stuck,
	// blocking PRS. The monitor looks for this situation and manufactures a write
	// periodically to unblock the primary.
	Monitor struct {
		// config is used to get the connection parameters.
		config *tabletenv.TabletConfig
		// ticks is the ticker on which we'll check
		// if the primary is blocked on semi-sync ACKs or not.
		ticks *timer.Timer
		// clearTicks is the ticker to clear the data in
		// the semisync_heartbeat table.
		clearTicks *timer.Timer

		// timerMu protects operations on the timers to prevent deadlocks
		// This must be acquired before mu if both are needed.
		timerMu sync.Mutex

		// mu protects the fields below.
		mu      sync.Mutex
		appPool *dbconnpool.ConnectionPool
		isOpen  bool
		// isWriting stores if the monitor is currently writing to the DB.
		// We don't want two different threads initiating writes, so we use this
		// for synchronization.
		isWriting atomic.Bool
		// inProgressWriteCount is the number of writes currently in progress.
		// The writes from the monitor themselves might get blocked and hence a count for them is required.
		// After enough writes are blocked, we want to notify VTOrc to run an ERS.
		inProgressWriteCount int
		// isBlocked stores if the primary is blocked on semi-sync ack.
		isBlocked bool
		// waiters stores the list of waiters that are waiting for the primary to be unblocked.
		waiters []chan struct{}
		// writesBlockedGauge is a gauge tracking the number of writes the monitor is blocked on.
		writesBlockedGauge *stats.Gauge
		// errorCount is the number of errors that the MySQL monitor ran into.
		// We ignore some of the errors, so the counter is a good way to track how many errors we have seen.
		errorCount *stats.Counter

		// actionDelay is the time to wait between various actions.
		actionDelay time.Duration
		// actionTimeout is when we should time out a given action.
		actionTimeout time.Duration

		// mysqlAlive is the last observed liveness of MySQL (SELECT 1).
		// Set to false when the connection pool fails or SELECT 1 fails.
		mysqlAlive bool
		// semiSyncEnabled is the last observed value of whether semi-sync is administratively enabled.
		// Set to false when MySQL is down.
		semiSyncEnabled bool
		// semiSyncRunning is the last observed value of whether semi-sync is actively transmitting ACKs.
		// Set to false when MySQL is down.
		semiSyncRunning bool
		// lastMySQLAliveAt is the last time MySQL was confirmed alive (SELECT 1 succeeded).
		// Never reset to zero — retains the last known alive time even after MySQL goes down.
		lastMySQLAliveAt time.Time
		// lastSemiSyncEnabledAt is the last time semi-sync was observed as administratively enabled.
		// Never reset to zero.
		lastSemiSyncEnabledAt time.Time
		// lastSemiSyncRunningAt is the last time semi-sync was observed as actively transmitting ACKs.
		// Never reset to zero.
		lastSemiSyncRunningAt time.Time
	}
)

// NewMonitor creates a new Monitor.
func NewMonitor(config *tabletenv.TabletConfig, exporter *servenv.Exporter) *Monitor {
	return &Monitor{
		config: config,
		ticks:  timer.NewTimer(config.SemiSyncMonitor.Interval),
		// We clear the data every day. We can make it configurable in the future,
		// but this seams fine for now.
		clearTicks:         timer.NewTimer(clearTimerDuration),
		writesBlockedGauge: exporter.NewGauge("SemiSyncMonitorWritesBlocked", "Number of writes blocked in the semi-sync monitor"),
		errorCount:         exporter.NewCounter("SemiSyncMonitorErrorCount", "Number of errors encountered by the semi-sync monitor"),
		appPool:            dbconnpool.NewConnectionPool("SemiSyncMonitorAppPool", exporter, maxWritesPermitted+5, mysqlctl.DbaIdleTimeout, 0, mysqlctl.PoolDynamicHostnameResolution),
		waiters:            make([]chan struct{}, 0),
		actionDelay:        config.SemiSyncMonitor.Interval / 10,
		actionTimeout:      config.SemiSyncMonitor.Interval / 2,
	}
}

// CreateTestMySQLMonitor creates a monitor for testing.
// It takes an optional fake db.
func CreateTestMySQLMonitor(db *fakesqldb.DB, exporter *servenv.Exporter) *Monitor {
	var dbc *dbconfigs.DBConfigs
	if db != nil {
		params := db.ConnParams()
		cp := *params
		dbc = dbconfigs.NewTestDBConfigs(cp, cp, "")
	}
	return NewMonitor(&tabletenv.TabletConfig{
		DB: dbc,
		SemiSyncMonitor: tabletenv.SemiSyncMonitorConfig{
			Interval: 1 * time.Second,
		},
	}, exporter)
}

// Open starts the monitor.
func (m *Monitor) Open() {
	// First acquire the timer mutex to prevent deadlock - we acquire in a consistent order
	m.timerMu.Lock()
	defer m.timerMu.Unlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	// The check for config being nil is only requried for tests.
	if m.isOpen || m.config == nil || m.config.DB == nil {
		// If we are already open, then there is nothing to do
		return
	}
	// Set the monitor to be open.
	m.isOpen = true
	log.Info("MySQL Monitor: opening")

	// This function could be running from within a unit test scope, in which case we use
	// mock pools that are already open. This is why we test for the pool being open.
	if !m.appPool.IsOpen() {
		m.appPool.Open(m.config.DB.AppWithDB())
	}
	m.clearTicks.Start(m.clearAllData)
	m.ticks.Start(m.checkMySQL)
}

// Close stops the monitor.
func (m *Monitor) Close() {
	// First acquire the timer mutex to prevent deadlock - we acquire in a consistent order
	m.timerMu.Lock()
	defer m.timerMu.Unlock()
	log.Info("MySQL Monitor: closing")
	// We close the ticks before we acquire the mu mutex to prevent deadlock.
	// The timer Close should not be called while holding a mutex that the function
	// the timer runs also acquires.
	m.clearTicks.Stop()
	m.ticks.Stop()
	// Acquire the mu mutex to update the isOpen field.
	m.mu.Lock()
	defer m.mu.Unlock()
	m.isOpen = false
	m.appPool.Close()
}

// checkMySQL is the periodic tick function. It checks MySQL liveness, semi-sync
// configuration, and whether the primary is blocked on semi-sync ACKs.
func (m *Monitor) checkMySQL() {
	ctx, cancel := context.WithTimeout(context.Background(), m.ticks.Interval())
	defer cancel()

	conn, err := m.appPool.Get(ctx)
	if err != nil {
		m.mu.Lock()
		m.mysqlAlive = false
		m.semiSyncEnabled = false
		m.semiSyncRunning = false
		m.mu.Unlock()
		m.errorCount.Add(1)
		log.Error(fmt.Sprintf("MySQL Monitor: failed to get a connection: %v", err))
		return
	}
	defer conn.Recycle()

	alive := m.checkMySQLAlive(conn)
	m.mu.Lock()
	m.mysqlAlive = alive
	if alive {
		m.lastMySQLAliveAt = time.Now()
	} else {
		m.semiSyncEnabled = false
		m.semiSyncRunning = false
	}
	m.mu.Unlock()

	if !alive {
		return
	}

	enabled := m.checkSemiSyncEnabled(conn)
	m.mu.Lock()
	m.semiSyncEnabled = enabled
	if enabled {
		m.lastSemiSyncEnabledAt = time.Now()
	}
	m.mu.Unlock()

	running := m.checkSemiSyncRunning(conn)
	m.mu.Lock()
	m.semiSyncRunning = running
	if running {
		m.lastSemiSyncRunningAt = time.Now()
	}
	m.mu.Unlock()

	isBlocked, err := m.isSemiSyncBlockedWithConn(conn)
	if err != nil {
		m.errorCount.Add(1)
		log.Error(fmt.Sprintf("MySQL Monitor: failed to check if primary is blocked on semi-sync: %v", err))
		return
	}
	m.setIsBlocked(isBlocked)
	if isBlocked {
		go m.startWrites()
	}
}

// IsMySQLAlive returns true if MySQL was alive on the last check.
func (m *Monitor) IsMySQLAlive() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.mysqlAlive
}

// LastMySQLAliveAt returns the last time MySQL was confirmed alive (SELECT 1 succeeded).
// The zero value means MySQL has never been observed alive since the monitor started.
// This value is never reset to zero, so callers can determine how long MySQL has been
// unavailable even after it stops responding.
func (m *Monitor) LastMySQLAliveAt() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastMySQLAliveAt
}

// IsSemiSyncEnabled returns true if semi-sync was administratively enabled on the last check.
func (m *Monitor) IsSemiSyncEnabled() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.semiSyncEnabled
}

// LastSemiSyncEnabledAt returns the last time semi-sync was observed as administratively enabled.
// The zero value means semi-sync has never been observed enabled since the monitor started.
func (m *Monitor) LastSemiSyncEnabledAt() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastSemiSyncEnabledAt
}

// IsSemiSyncRunning returns true if semi-sync was actively transmitting ACKs on the last check.
func (m *Monitor) IsSemiSyncRunning() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.semiSyncRunning
}

// LastSemiSyncRunningAt returns the last time semi-sync was observed as actively transmitting ACKs.
// The zero value means semi-sync has never been observed running since the monitor started.
func (m *Monitor) LastSemiSyncRunningAt() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastSemiSyncRunningAt
}

// checkMySQLAlive runs SELECT 1 and returns true if MySQL responds.
// Errors are absorbed: logged and counted.
func (m *Monitor) checkMySQLAlive(conn *dbconnpool.PooledDBConnection) bool {
	_, err := conn.Conn.ExecuteFetch(mysqlAliveQuery, 1, false)
	if err != nil {
		m.errorCount.Add(1)
		log.Error(fmt.Sprintf("MySQL Monitor: SELECT 1 failed: %v", err))
		return false
	}
	return true
}

// checkSemiSyncEnabled returns true if the semi-sync source plugin is administratively enabled.
// Errors are absorbed: logged and counted.
func (m *Monitor) checkSemiSyncEnabled(conn *dbconnpool.PooledDBConnection) bool {
	query := fmt.Sprintf(semiSyncEnabledQuery, m.actionTimeout.Milliseconds())
	res, err := conn.Conn.ExecuteFetch(query, 1, false)
	if err != nil {
		m.errorCount.Add(1)
		log.Error(fmt.Sprintf("MySQL Monitor: failed to check semi-sync enabled: %v", err))
		return false
	}
	if len(res.Rows) == 0 {
		return false
	}
	return res.Rows[0][0].ToString() == "ON"
}

// checkSemiSyncRunning returns true if semi-sync is actively transmitting ACKs.
// Errors are absorbed: logged and counted.
func (m *Monitor) checkSemiSyncRunning(conn *dbconnpool.PooledDBConnection) bool {
	query := fmt.Sprintf(semiSyncRunningQuery, m.actionTimeout.Milliseconds())
	res, err := conn.Conn.ExecuteFetch(query, 1, false)
	if err != nil {
		m.errorCount.Add(1)
		log.Error(fmt.Sprintf("MySQL Monitor: failed to check semi-sync running: %v", err))
		return false
	}
	if len(res.Rows) == 0 {
		return false
	}
	return res.Rows[0][0].ToString() == "ON"
}

// isSemiSyncBlocked checks if the primary is blocked on semi-sync.
func (m *Monitor) isSemiSyncBlocked() (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), m.ticks.Interval())
	defer cancel()
	// Get a connection from the pool
	conn, err := m.appPool.Get(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Recycle()

	return m.isSemiSyncBlockedWithConn(conn)
}

// isSemiSyncBlockedWithConn checks if the primary is blocked on semi-sync using an existing connection.
func (m *Monitor) isSemiSyncBlockedWithConn(conn *dbconnpool.PooledDBConnection) (bool, error) {
	stats, err := m.getSemiSyncStats(conn)
	if err != nil || stats.waitingSessions == 0 {
		return false, err
	}
	time.Sleep(m.actionDelay)
	followUpStats, err := m.getSemiSyncStats(conn)
	if err != nil || followUpStats.waitingSessions == 0 || followUpStats.ackedTrxs > stats.ackedTrxs {
		return false, err
	}
	return true, nil
}

// isClosed returns if the monitor is currently closed or not.
func (m *Monitor) isClosed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return !m.isOpen
}

// WaitUntilSemiSyncUnblocked waits until the primary is not blocked
// on semi-sync or until the context expires.
func (m *Monitor) WaitUntilSemiSyncUnblocked(ctx context.Context) error {
	// MySQL Monitor is closed, which means semi-sync is not enabled.
	// We don't have anything to wait for.
	if m.isClosed() {
		return nil
	}
	// run one iteration of checking MySQL health and semi-sync state.
	m.checkMySQL()
	if !m.stillBlocked() {
		// If we find that the primary isn't blocked, we're good,
		// we don't need to wait for anything.
		log.Info("Primary not blocked on semi-sync ACKs")
		return nil
	}
	log.Info("Waiting for semi-sync to be unblocked")
	// The primary is blocked. We need to wait for it to be unblocked
	// or the context to expire.
	ch := m.addWaiter()
	select {
	case <-ch:
		log.Info("Finished waiting for semi-sync to be unblocked")
		return nil
	case <-ctx.Done():
		log.Info("Error while waiting for semi-sync to be unblocked - " + ctx.Err().Error())
		return ctx.Err()
	}
}

// stillBlocked returns true if the monitor should continue writing to the DB
// because the monitor is still open, and the primary is still blocked.
func (m *Monitor) stillBlocked() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.isOpen && m.isBlocked
}

// checkAndSetIsWriting checks if the monitor is already writing to the DB.
// If it is not, then it sets the isWriting field and signals the caller.
func (m *Monitor) checkAndSetIsWriting() bool {
	return m.isWriting.CompareAndSwap(false, true)
}

// clearIsWriting clears the isWriting field.
func (m *Monitor) clearIsWriting() {
	m.isWriting.Store(false)
}

// startWrites starts writing to the DB.
// It is re-entrant and will return if we are already writing.
func (m *Monitor) startWrites() {
	ctx, cancel := context.WithTimeout(context.Background(), m.ticks.Interval())
	defer cancel()
	// If we are already writing, then we can just return.
	if !m.checkAndSetIsWriting() {
		return
	}
	// We defer the clear of the isWriting field.
	defer m.clearIsWriting()

	// Check if we need to continue writing or not.
	for m.stillBlocked() {
		select {
		case <-ctx.Done():
			return
		default:
			// We only need to do another write if there were no other successful
			// writes and we're indeed still blocked.
			blocked, err := m.isSemiSyncBlocked()
			if err != nil {
				return
			}
			if !blocked {
				m.setIsBlocked(false)
				return
			}
		}
		// We do the writes in a go-routine because if the network disruption
		// is somewhat long-lived, then the writes themselves can also block.
		// By doing them in a go-routine we give the system more time to recover while
		// exponentially backing off. We will not do more than maxWritesPermitted writes and once
		// all maxWritesPermitted writes are blocked, we'll wait for VTOrc to run an ERS.
		go m.write()
	}
}

// incrementWriteCount tries to increment the write count. It
// also checks that the write count value should not exceed
// the maximum value configured. It returns whether it was able
// to increment the value or not.
func (m *Monitor) incrementWriteCount() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.inProgressWriteCount == maxWritesPermitted {
		return false
	}
	m.inProgressWriteCount++
	m.writesBlockedGauge.Set(int64(m.inProgressWriteCount))
	return true
}

// AllWritesBlocked returns if maxWritesPermitted number of writes
// are already outstanding.
func (m *Monitor) AllWritesBlocked() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.isOpen && m.isBlocked && m.inProgressWriteCount == maxWritesPermitted
}

// decrementWriteCount decrements the write count.
func (m *Monitor) decrementWriteCount() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.inProgressWriteCount--
	m.writesBlockedGauge.Set(int64(m.inProgressWriteCount))
}

// write writes a heartbeat to unblock semi-sync being stuck.
func (m *Monitor) write() {
	if shouldWrite := m.incrementWriteCount(); !shouldWrite {
		return
	}
	defer m.decrementWriteCount()
	// Get a connection from the pool
	ctx, cancel := context.WithTimeout(context.Background(), m.actionTimeout)
	defer cancel()
	conn, err := m.appPool.Get(ctx)
	if err != nil {
		m.errorCount.Add(1)
		log.Error(fmt.Sprintf("MySQL Monitor: failed to get a connection when writing to semisync_heartbeat table: %v", err))
		return
	}
	err = conn.Conn.ExecuteFetchMultiDrain(m.addLockWaitTimeout(m.bindSideCarDBName(semiSyncHeartbeatWrite)))
	conn.Recycle()
	if err != nil {
		m.errorCount.Add(1)
		log.Error(fmt.Sprintf("MySQL Monitor: failed to write to semisync_heartbeat table: %v", err))
	} else {
		// One of the writes went through without an error.
		// This means that we aren't blocked on semi-sync anymore.
		m.setIsBlocked(false)
	}
}

// setIsBlocked sets the isBlocked field.
func (m *Monitor) setIsBlocked(val bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.isBlocked = val
	if !val {
		// If we are unblocked, then we need to signal all the waiters.
		for _, ch := range m.waiters {
			close(ch)
		}
		// We also empty the list of current waiters.
		m.waiters = nil
	}
}

// clearAllData clears all the data in the table so that it never
// consumes too much space on the MySQL instance.
func (m *Monitor) clearAllData() {
	// Get a connection from the pool
	ctx, cancel := context.WithTimeout(context.Background(), m.actionTimeout)
	defer cancel()
	conn, err := m.appPool.Get(ctx)
	if err != nil {
		m.errorCount.Add(1)
		log.Error(fmt.Sprintf("MySQL Monitor: failed get a connection to clear semisync_heartbeat table: %v", err))
		return
	}
	defer conn.Recycle()
	_, _, err = conn.Conn.ExecuteFetchMulti(m.addLockWaitTimeout(m.bindSideCarDBName(semiSyncHeartbeatClear)), 0, false)
	if err != nil {
		m.errorCount.Add(1)
		log.Error(fmt.Sprintf("MySQL Monitor: failed to clear semisync_heartbeat table: %v", err))
	}
}

// addWaiter adds a waiter to the list of waiters
// that will be unblocked when the primary is no longer blocked.
func (m *Monitor) addWaiter() chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	ch := make(chan struct{})
	m.waiters = append(m.waiters, ch)
	return ch
}

// bindSideCarDBName binds the sidecar db name to the query.
func (m *Monitor) bindSideCarDBName(query string) string {
	return sqlparser.BuildParsedQuery(query, sidecar.GetIdentifier()).Query
}

func (m *Monitor) addLockWaitTimeout(query string) string {
	timeoutQuery := fmt.Sprintf(setLockWaitTimeoutQuery, int(m.actionTimeout.Seconds()))
	return timeoutQuery + ";" + query
}

func (m *Monitor) getSemiSyncStats(conn *dbconnpool.PooledDBConnection) (semiSyncStats, error) {
	stats := semiSyncStats{}
	// Execute the query to check if the primary is blocked on semi-sync.
	query := fmt.Sprintf(semiSyncStatsQuery, m.actionTimeout.Milliseconds())
	res, err := conn.Conn.ExecuteFetch(query, 2, false)
	if err != nil {
		return stats, err
	}
	// If we have no rows, then the primary doesn't have semi-sync enabled.
	// It then follows, that the primary isn't blocked :)
	if len(res.Rows) == 0 {
		return stats, nil
	}

	// Read the status value and check if it is non-zero.
	if len(res.Rows) != 2 {
		return stats, vterrors.Errorf(vtrpc.Code_INTERNAL, "unexpected number of rows received, expected 2 but got %d, for semi-sync stats query %s", len(res.Rows), query)
	}
	if len(res.Rows[0]) != 2 {
		return stats, vterrors.Errorf(vtrpc.Code_INTERNAL, "unexpected number of columns received, expected 2 but got %d, for semi-sync stats query %s", len(res.Rows[0]), query)
	}
	for i := range len(res.Rows) {
		name := res.Rows[i][0].ToString()
		value, err := res.Rows[i][1].ToCastInt64()
		if err != nil {
			return stats, vterrors.Wrapf(err, "unexpected results for semi-sync stats query %s: %v", query, res.Rows)
		}
		switch name {
		case "Rpl_semi_sync_master_wait_sessions", "Rpl_semi_sync_source_wait_sessions":
			stats.waitingSessions = value
		case "Rpl_semi_sync_master_yes_tx", "Rpl_semi_sync_source_yes_tx":
			stats.ackedTrxs = value
		default:
			return stats, vterrors.Errorf(vtrpc.Code_INTERNAL, "unexpected results for semi-sync stats query %s: %v", query, res.Rows)
		}
	}
	return stats, nil
}
