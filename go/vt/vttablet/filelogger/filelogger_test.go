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

package filelogger

import (
	"context"
	"os"
	"path"
	"testing"
	"time"

	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

// TestFileLog sends a stream of five query records to the plugin, and verifies that they are logged.
func TestFileLog(t *testing.T) {
	dir := t.TempDir()

	logPath := path.Join(dir, "test.log")
	logger, err := Init(logPath)
	defer logger.Stop()
	if err != nil {
		t.Fatalf("error setting up file logger: %v", err)
	}

	ctx := context.Background()

	log1 := &tabletenv.LogStats{
		Ctx:         ctx,
		OriginalSQL: "test 1",
	}
	log1.AddRewrittenSQL("test 1 PII", time.Time{})
	log1.MysqlResponseTime = 0
	tabletenv.StatsLogger.Send(log1)

	log2 := &tabletenv.LogStats{
		Ctx:         ctx,
		OriginalSQL: "test 2",
	}
	log2.AddRewrittenSQL("test 2 PII", time.Time{})
	log2.MysqlResponseTime = 0
	tabletenv.StatsLogger.Send(log2)

	// Allow time for propagation
	for i := 0; i < 10; i++ {
		time.Sleep(10 * time.Millisecond)

		want := "\t\t\t''\t''\t0001-01-01 00:00:00.000000\t0001-01-01 00:00:00.000000\t0.000000\t\t\"test 1\"\t{}\t1\t\"test 1 PII\"\tmysql\t0.000000\t0.000000\t0\t0\t0\t\"\"\t\n\t\t\t''\t''\t0001-01-01 00:00:00.000000\t0001-01-01 00:00:00.000000\t0.000000\t\t\"test 2\"\t{}\t1\t\"test 2 PII\"\tmysql\t0.000000\t0.000000\t0\t0\t0\t\"\"\t\n"
		contents, _ := os.ReadFile(logPath)
		got := string(contents)
		if want == got {
			return
		}
		// Last iteration.
		if i == 9 {
			t.Errorf("streamlog file: want %q got %q", want, got)
		}
	}
}

// TestFileLog sends a stream of five query records to the plugin, and verifies that they are logged.
func TestFileLogRedacted(t *testing.T) {
	dir := t.TempDir()

	logPath := path.Join(dir, "test.log")
	logger, err := Init(logPath)
	defer logger.Stop()
	if err != nil {
		t.Fatalf("error setting up file logger: %v", err)
	}

	ctx := context.Background()

	log1 := &tabletenv.LogStats{
		Ctx:         ctx,
		OriginalSQL: "test 1",
		Config:      streamlog.NewQueryLogConfigForTest(),
	}
	log1.Config.RedactDebugUIQueries = true
	log1.AddRewrittenSQL("test 1 PII", time.Time{})
	log1.MysqlResponseTime = 0
	tabletenv.StatsLogger.Send(log1)

	log2 := &tabletenv.LogStats{
		Ctx:         ctx,
		OriginalSQL: "test 2",
		Config:      streamlog.NewQueryLogConfigForTest(),
	}
	log2.Config.RedactDebugUIQueries = true
	log2.AddRewrittenSQL("test 2 PII", time.Time{})
	log2.MysqlResponseTime = 0
	tabletenv.StatsLogger.Send(log2)

	// Allow time for propagation
	time.Sleep(10 * time.Millisecond)

	want := "\t\t\t''\t''\t0001-01-01 00:00:00.000000\t0001-01-01 00:00:00.000000\t0.000000\t\t\"test 1\"\t\"[REDACTED]\"\t1\t\"[REDACTED]\"\tmysql\t0.000000\t0.000000\t0\t0\t0\t\"\"\t\n\t\t\t''\t''\t0001-01-01 00:00:00.000000\t0001-01-01 00:00:00.000000\t0.000000\t\t\"test 2\"\t\"[REDACTED]\"\t1\t\"[REDACTED]\"\tmysql\t0.000000\t0.000000\t0\t0\t0\t\"\"\t\n"
	contents, _ := os.ReadFile(logPath)
	got := string(contents)
	if want != string(got) {
		t.Errorf("streamlog file: want %q got %q", want, got)
	}
}
