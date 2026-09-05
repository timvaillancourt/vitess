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

// mysqlDirsResult omits variables absent from vars, matching `SHOW GLOBAL VARIABLES`.
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
	for _, variable := range []string{
		"innodb_data_file_path",
		"innodb_doublewrite",
		"innodb_doublewrite_dir",
		"innodb_parallel_doublewrite_path",
		"innodb_temp_data_file_path",
		"innodb_temp_tablespaces_dir",
		"innodb_undo_directory",
		"log_bin_index",
		"relay_log_index",
	} {
		assert.Contains(t, mysqlDirsQuery, "'"+variable+"'")
	}

	tests := []struct {
		name    string
		result  *sqltypes.Result
		want    []string
		wantErr string
	}{
		{
			name: "all supported variables",
			result: mysqlDirsResult(map[string]string{
				"datadir":                     "/data/mysql",
				"tmpdir":                      "tmp-one:/tmp/two;keep",
				"innodb_data_home_dir":        "innodb",
				"innodb_data_file_path":       "system/ibdata1:12M;../system-two/ibdata2:12M:autoextend",
				"innodb_doublewrite":          "ON",
				"innodb_doublewrite_dir":      "doublewrite",
				"innodb_temp_data_file_path":  "temp-system/ibtmp1:12M:autoextend",
				"innodb_temp_tablespaces_dir": "/innodb-temp",
				"innodb_undo_directory":       "undo",
				"log_bin_basename":            "/binlog/vt-0000000100-bin",
				"log_bin_index":               "/binlog-index/vt-bin.index",
				"relay_log_basename":          "/relay/vt-0000000100-relay-bin",
				"relay_log_index":             "/relay-index/vt-relay.index",
				"innodb_log_group_home_dir":   "./",
			}),
			want: []string{"/data/mysql", "/data/mysql/tmp-one", "/tmp/two;keep", "/data/mysql/innodb", "/data/mysql/innodb/system", "/data/mysql/system-two", "/data/mysql/innodb/temp-system", "/data/mysql/undo", "/innodb-temp", "/data/mysql/#doublewrite", "/binlog", "/binlog-index", "/relay", "/relay-index", "/data/mysql"},
		},
		{
			name:   "relative InnoDB file paths default to datadir",
			result: mysqlDirsResult(map[string]string{"datadir": "/mysql/data", "innodb_data_file_path": "../system/ibdata1:12M:autoextend", "innodb_temp_data_file_path": "temp-volume/ibtmp1:12M:autoextend", "innodb_doublewrite": "ON", "innodb_parallel_doublewrite_path": "doublewrite/xb_doublewrite"}),
			want:   []string{"/mysql/data", "/mysql/system", "/mysql/data/temp-volume", "/mysql/data/doublewrite"},
		},
		{
			name:   "explicit relative doublewrite directory",
			result: mysqlDirsResult(map[string]string{"datadir": "/data/mysql", "innodb_doublewrite": "ON", "innodb_doublewrite_dir": "./doublewrite"}),
			want:   []string{"/data/mysql", "/data/mysql/doublewrite"},
		},
		{
			name:   "disabled doublewrite paths",
			result: mysqlDirsResult(map[string]string{"datadir": "/data/mysql", "innodb_doublewrite": "OFF", "innodb_doublewrite_dir": "/unused-doublewrite", "innodb_parallel_doublewrite_path": "/retired-volume/xb_doublewrite"}),
			want:   []string{"/data/mysql"},
		},
		{
			name:   "new doublewrite directory supersedes deprecated parallel path",
			result: mysqlDirsResult(map[string]string{"datadir": "/data/mysql", "innodb_doublewrite": "ON", "innodb_doublewrite_dir": "/active-doublewrite", "innodb_parallel_doublewrite_path": "/retired-volume/xb_doublewrite"}),
			want:   []string{"/data/mysql", "/active-doublewrite"},
		},
		{
			name: "missing variables",
			result: mysqlDirsResult(map[string]string{
				"datadir": "/data/mysql",
				"tmpdir":  "/tmp/mysql",
			}),
			want: []string{"/data/mysql", "/tmp/mysql"},
		},
		{
			name:    "query error",
			wantErr: "failed to query MySQL directories",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tm, mysqld, _ := newDiskHealthTestTM(t)
			mysqld.FetchSuperQueryMap = map[string]*sqltypes.Result{}
			if tt.result != nil {
				mysqld.FetchSuperQueryMap[mysqlDirsQuery] = tt.result
			}

			dirs, err := tm.detectMySQLDirs(t.Context())
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, dirs)
		})
	}
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
		{name: "disabled without dirs does nothing"},
		{name: "explicit dirs monitor without the enable flag", explicitDirs: []string{"/data/mysql"}, wantMonitor: true},
		{name: "explicit dirs suppress auto-detection", enabled: true, explicitDirs: []string{"/data/mysql"}, wantMonitor: true},
		{name: "enabled without dirs auto-detects", enabled: true, wantMonitor: true, wantAutoQuery: true},
		{name: "external MySQL skips auto-detection", enabled: true, externalMySQL: true},
		{name: "external MySQL ignores explicit dirs", enabled: true, explicitDirs: []string{"/data/mysql"}, externalMySQL: true},
		{name: "empty explicit dir falls through to auto-detection", enabled: true, explicitDirs: []string{""}, wantMonitor: true, wantAutoQuery: true},
		{name: "empty explicit dir without enable does nothing", explicitDirs: []string{""}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tm, mysqld, controller := newDiskHealthTestTM(t)
			var queried atomic.Bool
			mysqld.FetchSuperQueryCallback = func(query string) (*sqltypes.Result, error) {
				queried.Store(true)
				return mysqlDirsResult(map[string]string{"datadir": "/data/mysql", "tmpdir": "/tmp/mysql"}), nil
			}

			tm.initDiskHealthMonitor(tt.enabled, tt.explicitDirs, tt.externalMySQL)

			if tt.wantMonitor && tt.wantAutoQuery {
				assert.Eventually(t, func() bool { return controller.DiskHealthMonitor() != nil }, 30*time.Second, 10*time.Millisecond)
			} else if tt.wantMonitor {
				assert.NotNil(t, controller.DiskHealthMonitor())
			} else {
				assert.Nil(t, controller.DiskHealthMonitor())
			}
			assert.Equal(t, tt.wantAutoQuery, queried.Load())
		})
	}
}

func TestStopDiskHealthMonitorStopsAutoDetection(t *testing.T) {
	origRetryInterval := diskHealthMonitorDetectRetryInterval
	diskHealthMonitorDetectRetryInterval = 10 * time.Millisecond
	t.Cleanup(func() { diskHealthMonitorDetectRetryInterval = origRetryInterval })

	tm, mysqld, _ := newDiskHealthTestTM(t)
	var attempts atomic.Int64
	mysqld.FetchSuperQueryCallback = func(string) (*sqltypes.Result, error) {
		attempts.Add(1)
		return nil, vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "mysql not up yet")
	}

	tm.initDiskHealthMonitor(true, nil, false)
	assert.Eventually(t, func() bool { return attempts.Load() >= 2 }, 30*time.Second, 10*time.Millisecond)
	tm.stopDiskHealthMonitor()
	tm.stopDiskHealthMonitor()
	stoppedAt := attempts.Load()
	assert.Never(t, func() bool { return attempts.Load() > stoppedAt }, 100*time.Millisecond, 10*time.Millisecond)
}

func TestAutoDetectDiskHealthMonitorDirsRetries(t *testing.T) {
	origRetryInterval := diskHealthMonitorDetectRetryInterval
	diskHealthMonitorDetectRetryInterval = 10 * time.Millisecond
	t.Cleanup(func() { diskHealthMonitorDetectRetryInterval = origRetryInterval })

	tm, mysqld, controller := newDiskHealthTestTM(t)
	var attempts atomic.Int64
	mysqld.FetchSuperQueryCallback = func(query string) (*sqltypes.Result, error) {
		switch attempts.Add(1) {
		case 1:
			return nil, vterrors.Errorf(vtrpcpb.Code_UNAVAILABLE, "mysql not up yet")
		case 2:
			return mysqlDirsResult(nil), nil
		default:
			return mysqlDirsResult(map[string]string{"datadir": "/data/mysql"}), nil
		}
	}

	tm.initDiskHealthMonitor(true, nil, false)

	assert.Eventually(t, func() bool { return controller.DiskHealthMonitor() != nil }, 30*time.Second, 10*time.Millisecond)
	assert.Equal(t, int64(3), attempts.Load())
}
