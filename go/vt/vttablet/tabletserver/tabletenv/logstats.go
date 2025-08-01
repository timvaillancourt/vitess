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

package tabletenv

import (
	"context"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/google/safehtml"

	"vitess.io/vitess/go/logstats"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/callinfo"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

const (
	// QuerySourceConsolidator means query result is found in consolidator.
	QuerySourceConsolidator = 1 << iota
	// QuerySourceMySQL means query result is returned from MySQL.
	QuerySourceMySQL
)

// LogStats records the stats for a single query
type LogStats struct {
	Config streamlog.QueryLogConfig

	Ctx                  context.Context
	Method               string
	Target               *querypb.Target
	PlanType             string
	OriginalSQL          string
	BindVariables        map[string]*querypb.BindVariable
	rewrittenSqls        []string
	RowsAffected         int
	NumberOfQueries      int
	StartTime            time.Time
	EndTime              time.Time
	MysqlResponseTime    time.Duration
	WaitingForConnection time.Duration
	QuerySources         byte
	Rows                 [][]sqltypes.Value
	TransactionID        int64
	ReservedID           int64
	Error                error
	CachedPlan           bool
}

// NewLogStats constructs a new LogStats with supplied Method and ctx
// field values, and the StartTime field set to the present time.
func NewLogStats(ctx context.Context, methodName string, config streamlog.QueryLogConfig) *LogStats {
	return &LogStats{
		Ctx:       ctx,
		Method:    methodName,
		StartTime: time.Now(),
		Config:    config,
	}
}

// Send finalizes a record and sends it
func (stats *LogStats) Send() {
	stats.EndTime = time.Now()
	StatsLogger.Send(stats)
}

// ImmediateCaller returns the immediate caller stored in LogStats.Ctx
func (stats *LogStats) ImmediateCaller() string {
	return callerid.GetUsername(callerid.ImmediateCallerIDFromContext(stats.Ctx))
}

// EffectiveCaller returns the effective caller stored in LogStats.Ctx
func (stats *LogStats) EffectiveCaller() string {
	return callerid.GetPrincipal(callerid.EffectiveCallerIDFromContext(stats.Ctx))
}

// EventTime returns the time the event was created.
func (stats *LogStats) EventTime() time.Time {
	return stats.EndTime
}

// AddRewrittenSQL adds a single sql statement to the rewritten list
func (stats *LogStats) AddRewrittenSQL(sql string, start time.Time) {
	stats.QuerySources |= QuerySourceMySQL
	stats.NumberOfQueries++
	stats.rewrittenSqls = append(stats.rewrittenSqls, sql)
	stats.MysqlResponseTime += time.Since(start)
}

// TotalTime returns how long this query has been running
func (stats *LogStats) TotalTime() time.Duration {
	return stats.EndTime.Sub(stats.StartTime)
}

// RewrittenSQL returns a semicolon separated list of SQL statements
// that were executed.
func (stats *LogStats) RewrittenSQL() string {
	return strings.Join(stats.rewrittenSqls, "; ")
}

// SizeOfResponse returns the approximate size of the response in
// bytes (this does not take in account protocol encoding). It will return
// 0 for streaming requests.
func (stats *LogStats) SizeOfResponse() int {
	if stats.Rows == nil {
		return 0
	}
	size := 0
	for _, row := range stats.Rows {
		for _, field := range row {
			size += field.Len()
		}
	}
	return size
}

// FmtQuerySources returns a comma separated list of query
// sources. If there were no query sources, it returns the string
// "none".
func (stats *LogStats) FmtQuerySources() string {
	if stats.QuerySources == 0 {
		return "none"
	}
	sources := make([]string, 2)
	n := 0
	if stats.QuerySources&QuerySourceMySQL != 0 {
		sources[n] = "mysql"
		n++
	}
	if stats.QuerySources&QuerySourceConsolidator != 0 {
		sources[n] = "consolidator"
		n++
	}
	return strings.Join(sources[:n], ",")
}

// ContextHTML returns the HTML version of the context that was used, or "".
// This is a method on LogStats instead of a field so that it doesn't need
// to be passed by value everywhere.
func (stats *LogStats) ContextHTML() safehtml.HTML {
	return callinfo.HTMLFromContext(stats.Ctx)
}

// ErrorStr returns the error string or ""
func (stats *LogStats) ErrorStr() string {
	if stats.Error != nil {
		return stats.Error.Error()
	}
	return ""
}

// CallInfo returns some parts of CallInfo if set
func (stats *LogStats) CallInfo() (string, string) {
	ci, ok := callinfo.FromContext(stats.Ctx)
	if !ok {
		return "", ""
	}
	return ci.Text(), ci.Username()
}

// Logf formats the log record to the given writer, either as
// tab-separated list of logged fields or as JSON.
func (stats *LogStats) Logf(w io.Writer, params url.Values) error {
	if !stats.Config.ShouldEmitLog(stats.OriginalSQL, uint64(stats.RowsAffected), uint64(len(stats.Rows)), stats.TotalTime(), stats.Error != nil) {
		return nil
	}

	_, fullBindParams := params["full"]
	// TODO: remove username here we fully enforce immediate caller id
	callInfo, username := stats.CallInfo()

	log := logstats.NewLogger()
	log.Init(stats.Config.Format == streamlog.QueryLogFormatJSON)
	log.Key("Method")
	log.StringUnquoted(stats.Method)
	log.Key("CallInfo")
	log.StringUnquoted(callInfo)
	log.Key("Username")
	log.StringUnquoted(username)
	log.Key("ImmediateCaller")
	log.StringSingleQuoted(stats.ImmediateCaller())
	log.Key("Effective Caller")
	log.StringSingleQuoted(stats.EffectiveCaller())
	log.Key("Start")
	log.Time(stats.StartTime)
	log.Key("End")
	log.Time(stats.EndTime)
	log.Key("TotalTime")
	log.Duration(stats.TotalTime())
	log.Key("PlanType")
	log.StringUnquoted(stats.PlanType)
	log.Key("OriginalSQL")
	log.String(stats.OriginalSQL)
	log.Key("BindVars")
	if stats.Config.RedactDebugUIQueries {
		log.Redacted()
	} else {
		log.BindVariables(stats.BindVariables, fullBindParams)
	}
	log.Key("Queries")
	log.Int(int64(stats.NumberOfQueries))
	log.Key("RewrittenSQL")
	if stats.Config.RedactDebugUIQueries {
		log.Redacted()
	} else {
		log.String(stats.RewrittenSQL())
	}
	log.Key("QuerySources")
	log.StringUnquoted(stats.FmtQuerySources())
	log.Key("MysqlTime")
	log.Duration(stats.MysqlResponseTime)
	log.Key("ConnWaitTime")
	log.Duration(stats.WaitingForConnection)
	log.Key("RowsAffected")
	log.Uint(uint64(stats.RowsAffected))
	log.Key("TransactionID")
	log.Int(stats.TransactionID)
	log.Key("ResponseSize")
	log.Int(int64(stats.SizeOfResponse()))
	log.Key("Error")
	log.String(stats.ErrorStr())

	// logstats from the vttablet are always tab-terminated; keep this for backwards
	// compatibility for existing parsers
	log.TabTerminated()

	return log.Flush(w)
}
