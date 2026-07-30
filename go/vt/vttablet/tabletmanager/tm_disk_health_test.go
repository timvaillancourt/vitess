/*
Copyright 2026 The Vitess Authors.

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

package tabletmanager

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// mysqlDirsResult builds a SHOW GLOBAL VARIABLES result from the given
// variable/value pairs. Variables absent from the map are omitted from the
// result, mirroring a server that does not expose them.
func mysqlDirsResult(vars map[string]string) *sqltypes.Result {
	fields := sqltypes.MakeTestFields("Variable_name|Value", "varchar|varchar")
	rows := make([]string, 0, len(vars))
	for name, val := range vars {
		rows = append(rows, name+"|"+val)
	}
	return sqltypes.MakeTestResult(fields, rows...)
}

func newDiskHealthTestTM(t *testing.T) (*TabletManager, *mysqlctl.FakeMysqlDaemon, *tabletservermock.Controller) {
	t.Helper()

	db := fakesqldb.New(t)
	t.Cleanup(db.Close)
	mysqld := mysqlctl.NewFakeMysqlDaemon(db)
	t.Cleanup(func() { mysqld.Close() })
	controller := tabletservermock.NewController()
	tm := &TabletManager{
		BatchCtx:            t.Context(),
		MysqlDaemon:         mysqld,
		QueryServiceControl: controller,
	}
	return tm, mysqld, controller
}

func TestDetectMySQLDirs(t *testing.T) {
	tm, mysqld, _ := newDiskHealthTestTM(t)
	// tmpdir entries use the OS path-list separator (':' on Unix); the
	// *_basename values are file path prefixes whose directory is monitored;
	// innodb_log_group_home_dir defaults to "./", relative to datadir.
	// Duplicates are fine — the monitor dedupes by volume.
	mysqld.FetchSuperQueryMap = map[string]*sqltypes.Result{
		mysqlDirsQuery: mysqlDirsResult(map[string]string{
			"datadir":                   "/data/mysql",
			"tmpdir":                    "/tmp/one:/tmp/two",
			"log_bin_basename":          "/binlog/vt-0000000100-bin",
			"relay_log_basename":        "/relay/vt-0000000100-relay-bin",
			"innodb_log_group_home_dir": "./",
		}),
	}

	dirs, err := tm.detectMySQLDirs(t.Context())
	require.NoError(t, err)
	assert.Equal(t, []string{"/data/mysql", "/tmp/one", "/tmp/two", "/binlog", "/relay", "/data/mysql"}, dirs)
}

// TestDetectMySQLDirsTmpdirSemicolonIsPathChar proves a ';' inside a tmpdir
// path is treated as a path character on Unix, not an entry separator, so a
// legitimate directory name is not split into a phantom path.
func TestDetectMySQLDirsTmpdirSemicolonIsPathChar(t *testing.T) {
	tm, mysqld, _ := newDiskHealthTestTM(t)
	mysqld.FetchSuperQueryMap = map[string]*sqltypes.Result{
		mysqlDirsQuery: mysqlDirsResult(map[string]string{
			"tmpdir": "/tmp/one:/tmp/two;keep",
		}),
	}

	dirs, err := tm.detectMySQLDirs(t.Context())
	require.NoError(t, err)
	assert.Equal(t, []string{"/tmp/one", "/tmp/two;keep"}, dirs)
}

// TestDetectMySQLDirsRelativeTmpdir proves a relative tmpdir entry is resolved
// against datadir (mysqld's working directory), matching how the server
// interprets the raw option value; absolute entries are left unchanged.
func TestDetectMySQLDirsRelativeTmpdir(t *testing.T) {
	tm, mysqld, _ := newDiskHealthTestTM(t)
	mysqld.FetchSuperQueryMap = map[string]*sqltypes.Result{
		mysqlDirsQuery: mysqlDirsResult(map[string]string{
			"datadir": "/mysql/data",
			"tmpdir":  "tmp-one:/abs/tmp-two",
		}),
	}

	dirs, err := tm.detectMySQLDirs(t.Context())
	require.NoError(t, err)
	assert.Equal(t, []string{"/mysql/data", "/mysql/data/tmp-one", "/abs/tmp-two"}, dirs)
}

// TestDetectMySQLDirsMissingVariables proves a server that does not expose
// some of the variables still yields the directories it does expose, rather
// than failing detection entirely.
func TestDetectMySQLDirsMissingVariables(t *testing.T) {
	tm, mysqld, _ := newDiskHealthTestTM(t)
	mysqld.FetchSuperQueryMap = map[string]*sqltypes.Result{
		mysqlDirsQuery: mysqlDirsResult(map[string]string{
			"datadir": "/data/mysql",
			"tmpdir":  "/tmp/mysql",
		}),
	}

	dirs, err := tm.detectMySQLDirs(t.Context())
	require.NoError(t, err)
	assert.Equal(t, []string{"/data/mysql", "/tmp/mysql"}, dirs)
}

func TestDetectMySQLDirsQueryError(t *testing.T) {
	tm, mysqld, _ := newDiskHealthTestTM(t)
	mysqld.FetchSuperQueryMap = map[string]*sqltypes.Result{}

	_, err := tm.detectMySQLDirs(t.Context())
	require.ErrorContains(t, err, "failed to query MySQL directories")
}

func TestInitDiskHealthMonitor(t *testing.T) {
	tests := []struct {
		name          string
		enabled       bool
		explicitDirs  []string
		externalMySQL bool
		wantMonitor   bool
		wantAutoQuery bool
	}{
		{
			name:         "disabled without dirs does nothing",
			enabled:      false,
			explicitDirs: nil,
			wantMonitor:  false,
		},
		{
			name:         "explicit dirs monitor without the enable flag",
			enabled:      false,
			explicitDirs: []string{"/data/mysql"},
			wantMonitor:  true,
		},
		{
			name:          "explicit dirs suppress auto-detection",
			enabled:       true,
			explicitDirs:  []string{"/data/mysql"},
			wantMonitor:   true,
			wantAutoQuery: false,
		},
		{
			name:          "enabled without dirs auto-detects",
			enabled:       true,
			explicitDirs:  nil,
			wantMonitor:   true,
			wantAutoQuery: true,
		},
		{
			name:          "external MySQL skips auto-detection",
			enabled:       true,
			explicitDirs:  nil,
			externalMySQL: true,
			wantMonitor:   false,
			wantAutoQuery: false,
		},
		{
			name:          "external MySQL ignores explicit dirs",
			enabled:       true,
			explicitDirs:  []string{"/data/mysql"},
			externalMySQL: true,
			wantMonitor:   false,
			wantAutoQuery: false,
		},
		{
			name:          "empty explicit dir falls through to auto-detection",
			enabled:       true,
			explicitDirs:  []string{""},
			wantMonitor:   true,
			wantAutoQuery: true,
		},
		{
			name:         "empty explicit dir without enable does nothing",
			enabled:      false,
			explicitDirs: []string{""},
			wantMonitor:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tm, mysqld, controller := newDiskHealthTestTM(t)
			var queried atomic.Bool
			mysqld.FetchSuperQueryCallback = func(query string) (*sqltypes.Result, error) {
				queried.Store(true)
				return mysqlDirsResult(map[string]string{"datadir": "/data/mysql", "tmpdir": "/tmp/mysql"}), nil
			}

			require.NoError(t, tm.initDiskHealthMonitor(tt.enabled, tt.explicitDirs, tt.externalMySQL))

			if tt.wantMonitor && tt.wantAutoQuery {
				// Auto-detection runs in the background.
				assert.Eventually(t, func() bool { return controller.DiskHealthMonitor() != nil }, 30*time.Second, 10*time.Millisecond)
			} else if tt.wantMonitor {
				// Explicit dirs are wired synchronously.
				assert.NotNil(t, controller.DiskHealthMonitor())
			} else {
				assert.Nil(t, controller.DiskHealthMonitor())
			}
			assert.Equal(t, tt.wantAutoQuery, queried.Load())
		})
	}
}

// TestAutoDetectDiskHealthMonitorDirsRetries proves auto-detection keeps
// retrying until MySQL responds, covering a tablet that starts before mysqld.
func TestAutoDetectDiskHealthMonitorDirsRetries(t *testing.T) {
	origRetryInterval := diskHealthMonitorDetectRetryInterval
	diskHealthMonitorDetectRetryInterval = 10 * time.Millisecond
	t.Cleanup(func() { diskHealthMonitorDetectRetryInterval = origRetryInterval })

	tm, mysqld, controller := newDiskHealthTestTM(t)
	var attempts atomic.Int64
	mysqld.FetchSuperQueryCallback = func(query string) (*sqltypes.Result, error) {
		if attempts.Add(1) <= 2 {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "mysql not up yet")
		}
		return mysqlDirsResult(map[string]string{"datadir": "/data/mysql", "tmpdir": "/tmp/mysql"}), nil
	}

	require.NoError(t, tm.initDiskHealthMonitor(true, nil, false))

	assert.Eventually(t, func() bool { return controller.DiskHealthMonitor() != nil }, 30*time.Second, 10*time.Millisecond)
	assert.GreaterOrEqual(t, attempts.Load(), int64(3))
}

// TestAutoDetectDiskHealthMonitorDirsRetriesOnEmpty proves that a successful
// query returning no directories is retried rather than treated as success
// with a no-op monitor, covering a MySQL that answers before it is fully
// initialized.
func TestAutoDetectDiskHealthMonitorDirsRetriesOnEmpty(t *testing.T) {
	origRetryInterval := diskHealthMonitorDetectRetryInterval
	diskHealthMonitorDetectRetryInterval = 10 * time.Millisecond
	t.Cleanup(func() { diskHealthMonitorDetectRetryInterval = origRetryInterval })

	tm, mysqld, controller := newDiskHealthTestTM(t)
	var attempts atomic.Int64
	mysqld.FetchSuperQueryCallback = func(query string) (*sqltypes.Result, error) {
		if attempts.Add(1) <= 2 {
			return mysqlDirsResult(nil), nil
		}
		return mysqlDirsResult(map[string]string{"datadir": "/data/mysql"}), nil
	}

	require.NoError(t, tm.initDiskHealthMonitor(true, nil, false))

	assert.Eventually(t, func() bool { return controller.DiskHealthMonitor() != nil }, 30*time.Second, 10*time.Millisecond)
	assert.GreaterOrEqual(t, attempts.Load(), int64(3))
}
