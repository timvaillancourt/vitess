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

package mysql

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	venv "vitess.io/vitess/go/vt/env"
	"vitess.io/vitess/go/vt/tlstest"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttls"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var selectRowsResult = &sqltypes.Result{
	Fields: []*querypb.Field{
		{
			Name:    "id",
			Type:    querypb.Type_INT32,
			Charset: collations.CollationBinaryID,
			Flags:   uint32(querypb.MySqlFlag_NUM_FLAG),
		},
		{
			Name:    "name",
			Type:    querypb.Type_VARCHAR,
			Charset: uint32(collations.CollationUtf8mb4ID),
		},
	},
	Rows: [][]sqltypes.Value{
		{
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte("10")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("nice name")),
		},
		{
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte("20")),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("nicer name")),
		},
	},
}

type testHandler struct {
	UnimplementedHandler
	mu       sync.Mutex
	lastConn *Conn
	result   *sqltypes.Result
	err      error
	warnings uint16
}

func (th *testHandler) LastConn() *Conn {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.lastConn
}

func (th *testHandler) Result() *sqltypes.Result {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.result
}

func (th *testHandler) SetErr(err error) {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.err = err
}

func (th *testHandler) Err() error {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.err
}

func (th *testHandler) SetWarnings(count uint16) {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.warnings = count
}

func (th *testHandler) NewConnection(c *Conn) {
	th.mu.Lock()
	defer th.mu.Unlock()
	th.lastConn = c
}

func (th *testHandler) ComQuery(c *Conn, query string, callback func(*sqltypes.Result) error) error {
	if result := th.Result(); result != nil {
		callback(result)
		return nil
	}

	switch query {
	case "error":
		return th.Err()
	case "panic":
		panic("test panic attack!")
	case "select rows":
		callback(selectRowsResult)
	case "error after send":
		callback(selectRowsResult)
		return th.Err()
	case "insert":
		callback(&sqltypes.Result{
			RowsAffected: 123,
			InsertID:     123456789,
		})
	case "schema echo":
		callback(&sqltypes.Result{
			Fields: []*querypb.Field{
				{
					Name:    "schema_name",
					Type:    querypb.Type_VARCHAR,
					Charset: uint32(collations.MySQL8().DefaultConnectionCharset()),
				},
			},
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(c.schemaName)),
				},
			},
		})
	case "ssl echo":
		value := "OFF"
		if c.Capabilities&CapabilityClientSSL > 0 {
			value = "ON"
		}
		callback(&sqltypes.Result{
			Fields: []*querypb.Field{
				{
					Name:    "ssl_flag",
					Type:    querypb.Type_VARCHAR,
					Charset: uint32(collations.MySQL8().DefaultConnectionCharset()),
				},
			},
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(value)),
				},
			},
		})
	case "userData echo":
		callback(&sqltypes.Result{
			Fields: []*querypb.Field{
				{
					Name:    "user",
					Type:    querypb.Type_VARCHAR,
					Charset: uint32(collations.MySQL8().DefaultConnectionCharset()),
				},
				{
					Name:    "user_data",
					Type:    querypb.Type_VARCHAR,
					Charset: uint32(collations.MySQL8().DefaultConnectionCharset()),
				},
			},
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(c.User)),
					sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(c.UserData.Get().Username)),
				},
			},
		})
	case "50ms delay":
		callback(&sqltypes.Result{
			Fields: []*querypb.Field{{
				Name:    "result",
				Type:    querypb.Type_VARCHAR,
				Charset: uint32(collations.MySQL8().DefaultConnectionCharset()),
			}},
		})
		time.Sleep(50 * time.Millisecond)
		callback(&sqltypes.Result{
			Rows: [][]sqltypes.Value{{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("delayed")),
			}},
		})
	default:
		if strings.HasPrefix(query, benchmarkQueryPrefix) {
			callback(&sqltypes.Result{
				Fields: []*querypb.Field{
					{
						Name:    "result",
						Type:    querypb.Type_VARCHAR,
						Charset: uint32(collations.MySQL8().DefaultConnectionCharset()),
					},
				},
				Rows: [][]sqltypes.Value{
					{
						sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(query)),
					},
				},
			})
		}

		callback(&sqltypes.Result{})
	}
	return nil
}

func (th *testHandler) ComQueryMulti(c *Conn, sql string, callback func(qr sqltypes.QueryResponse, more bool, firstPacket bool) error) error {
	qries, err := th.Env().Parser().SplitStatementToPieces(sql)
	if err != nil {
		return err
	}
	for i, query := range qries {
		firstPacket := true
		err = th.ComQuery(c, query, func(result *sqltypes.Result) error {
			err = callback(sqltypes.QueryResponse{QueryResult: result}, i < len(qries)-1, firstPacket)
			firstPacket = false
			return err
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (th *testHandler) ComPrepare(*Conn, string) ([]*querypb.Field, uint16, error) {
	return nil, 0, nil
}

func (th *testHandler) ComStmtExecute(c *Conn, prepare *PrepareData, callback func(*sqltypes.Result) error) error {
	return nil
}

func (th *testHandler) ComRegisterReplica(c *Conn, replicaHost string, replicaPort uint16, replicaUser string, replicaPassword string) error {
	return nil
}
func (th *testHandler) ComBinlogDump(c *Conn, logFile string, binlogPos uint32) error {
	return nil
}
func (th *testHandler) ComBinlogDumpGTID(c *Conn, logFile string, logPos uint64, gtidSet replication.GTIDSet) error {
	return nil
}

func (th *testHandler) WarningCount(c *Conn) uint16 {
	th.mu.Lock()
	defer th.mu.Unlock()
	return th.warnings
}

func (th *testHandler) Env() *vtenv.Environment {
	return vtenv.NewTestEnv()
}

func getHostPort(t *testing.T, a net.Addr) (string, int) {
	host := a.(*net.TCPAddr).IP.String()
	port := a.(*net.TCPAddr).Port
	t.Logf("listening on address '%v' port %v", host, port)
	return host, port
}

func TestConnectionFromListener(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	th := &testHandler{}

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	defer authServer.close()
	// Make sure we can create our own net.Listener for use with the mysql
	// listener
	listener, err := net.Listen("tcp", "127.0.0.1:")
	require.NoError(t, err, "net.Listener failed")

	l, err := NewFromListener(listener, authServer, th, 0, 0, false, 0, 0, false)
	require.NoError(t, err, "NewListener failed")
	host, port := getHostPort(t, l.Addr())
	fmt.Printf("host: %s, port: %d\n", host, port)
	// Setup the right parameters.
	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
	}
	go l.Accept()
	defer cleanupListener(ctx, l, params)

	c, err := Connect(ctx, params)
	require.NoError(t, err, "Should be able to connect to server")
	c.Close()
}

func TestConnectionWithoutSourceHost(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	th := &testHandler{}

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	defer authServer.close()

	l, err := NewListener("tcp", "127.0.0.1:", authServer, th, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err, "NewListener failed")
	host, port := getHostPort(t, l.Addr())
	// Setup the right parameters.
	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
	}
	go l.Accept()
	defer cleanupListener(ctx, l, params)

	c, err := Connect(ctx, params)
	require.NoError(t, err, "Should be able to connect to server")
	c.Close()
}

func TestConnectionWithSourceHost(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	th := &testHandler{}

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{
		{
			Password:   "password1",
			UserData:   "userData1",
			SourceHost: "localhost",
		},
	}
	defer authServer.close()

	l, err := NewListener("tcp", "127.0.0.1:", authServer, th, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err, "NewListener failed")
	host, port := getHostPort(t, l.Addr())
	// Setup the right parameters.
	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
	}
	go l.Accept()
	defer cleanupListener(ctx, l, params)

	_, err = Connect(ctx, params)
	// target is localhost, should not work from tcp connection
	require.EqualError(t, err, "Access denied for user 'user1' (errno 1045) (sqlstate 28000)", "Should not be able to connect to server")
}

func TestConnectionUseMysqlNativePasswordWithSourceHost(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	th := &testHandler{}

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{
		{
			MysqlNativePassword: "*9E128DA0C64A6FCCCDCFBDD0FC0A2C967C6DB36F",
			UserData:            "userData1",
			SourceHost:          "localhost",
		},
	}
	defer authServer.close()

	l, err := NewListener("tcp", "127.0.0.1:", authServer, th, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err, "NewListener failed")
	host, port := getHostPort(t, l.Addr())
	// Setup the right parameters.
	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "mysql_password",
	}
	go l.Accept()
	defer cleanupListener(ctx, l, params)

	_, err = Connect(ctx, params)
	// target is localhost, should not work from tcp connection
	require.EqualError(t, err, "Access denied for user 'user1' (errno 1045) (sqlstate 28000)", "Should not be able to connect to server")
}

func TestConnectionUnixSocket(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	th := &testHandler{}

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{
		{
			Password:   "password1",
			UserData:   "userData1",
			SourceHost: "localhost",
		},
	}
	defer authServer.close()

	unixSocket, err := os.CreateTemp("", "mysql_vitess_test.sock")
	require.NoError(t, err, "Failed to create temp file")

	os.Remove(unixSocket.Name())

	l, err := NewListener("unix", unixSocket.Name(), authServer, th, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err, "NewListener failed")
	// Setup the right parameters.
	params := &ConnParams{
		UnixSocket: unixSocket.Name(),
		Uname:      "user1",
		Pass:       "password1",
	}
	go l.Accept()
	defer cleanupListener(ctx, l, params)

	c, err := Connect(ctx, params)
	require.NoError(t, err, "Should be able to connect to server")
	c.Close()
}

func TestClientFoundRows(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	th := &testHandler{}

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	defer authServer.close()

	l, err := NewListener("tcp", "127.0.0.1:", authServer, th, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err, "NewListener failed")
	host, port := getHostPort(t, l.Addr())
	// Setup the right parameters.
	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
	}
	go l.Accept()
	defer cleanupListener(ctx, l, params)

	// Test without flag.
	c, err := Connect(ctx, params)
	require.NoError(t, err, "Connect failed")
	foundRows := th.LastConn().Capabilities & CapabilityClientFoundRows
	assert.Equal(t, uint32(0), foundRows, "FoundRows flag: %x, second bit must be 0", th.LastConn().Capabilities)
	c.Close()
	assert.True(t, c.IsClosed(), "IsClosed should be true on Close-d connection.")

	// Test with flag.
	params.Flags |= CapabilityClientFoundRows
	c, err = Connect(ctx, params)
	require.NoError(t, err, "Connect failed")
	foundRows = th.LastConn().Capabilities & CapabilityClientFoundRows
	assert.NotZero(t, foundRows, "FoundRows flag: %x, second bit must be set", th.LastConn().Capabilities)
	c.Close()
}

func TestConnCounts(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	th := &testHandler{}

	user := "anotherNotYetConnectedUser1"
	passwd := "password1"

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries[user] = []*AuthServerStaticEntry{{
		Password: passwd,
		UserData: "userData1",
	}}
	defer authServer.close()

	l, err := NewListener("tcp", "127.0.0.1:", authServer, th, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err, "NewListener failed")
	host, port := getHostPort(t, l.Addr())
	// Test with one new connection.
	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: user,
		Pass:  passwd,
	}
	go l.Accept()
	defer cleanupListener(ctx, l, params)

	c, err := Connect(ctx, params)
	require.NoError(t, err, "Connect failed")

	checkCountsForUser(t, user, 1)

	// Test with a second new connection.
	c2, err := Connect(ctx, params)
	require.NoError(t, err)
	checkCountsForUser(t, user, 2)

	// Test after closing connections.
	c.Close()
	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		checkCountsForUser(t, user, 1)
	}, 1*time.Second, 10*time.Millisecond)

	c2.Close()
	assert.EventuallyWithT(t, func(t *assert.CollectT) {
		checkCountsForUser(t, user, 0)
	}, 1*time.Second, 10*time.Millisecond)
}

func checkCountsForUser(t assert.TestingT, user string, expected int64) {
	connCounts := connCountPerUser.Counts()

	userCount, ok := connCounts[user]
	assert.True(t, ok, "No count found for user %s", user)
	assert.Equal(t, expected, userCount)
}

func TestServer(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	th := &testHandler{}

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	defer authServer.close()

	l, err := NewListener("tcp", "127.0.0.1:", authServer, th, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err)
	host, port := getHostPort(t, l.Addr())
	// Setup the right parameters.
	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
	}
	l.SlowConnectWarnThreshold.Store(time.Nanosecond.Nanoseconds())
	go l.Accept()
	defer cleanupListener(ctx, l, params)

	// Run a 'select rows' command with results.
	output, err := runMysqlWithErr(t, params, "select rows")
	require.NoError(t, err)

	assert.Contains(t, output, "nice name", "Unexpected output for 'select rows'")
	assert.Contains(t, output, "nicer name", "Unexpected output for 'select rows'")
	assert.Contains(t, output, "2 rows in set", "Unexpected output for 'select rows'")
	assert.NotContains(t, output, "warnings")

	// Run a 'select rows' command with warnings
	th.SetWarnings(13)
	output, err = runMysqlWithErr(t, params, "select rows")
	require.NoError(t, err)
	assert.Contains(t, output, "nice name", "Unexpected output for 'select rows'")
	assert.Contains(t, output, "nicer name", "Unexpected output for 'select rows'")
	assert.Contains(t, output, "2 rows in set", "Unexpected output for 'select rows'")
	assert.Contains(t, output, "13 warnings", "Unexpected output for 'select rows'")
	th.SetWarnings(0)

	// If there's an error after streaming has started,
	// we should get a 2013
	th.SetErr(sqlerror.NewSQLError(sqlerror.ERUnknownComError, sqlerror.SSNetError, "forced error after send"))
	output, err = runMysqlWithErr(t, params, "error after send")
	require.Error(t, err)
	assert.Contains(t, output, "ERROR 2013 (HY000)", "Unexpected output for 'panic'")
	// MariaDB might not print the MySQL bit here
	assert.Regexp(t, `Lost connection to( MySQL)? server during query`, output, "Unexpected output for 'panic': %v", output)

	// Run an 'insert' command, no rows, but rows affected.
	output, err = runMysqlWithErr(t, params, "insert")
	require.NoError(t, err)
	assert.Contains(t, output, "Query OK, 123 rows affected", "Unexpected output for 'insert'")

	// Run a 'schema echo' command, to make sure db name is right.
	params.DbName = "XXXfancyXXX"
	output, err = runMysqlWithErr(t, params, "schema echo")
	require.NoError(t, err)
	assert.Contains(t, output, params.DbName, "Unexpected output for 'schema echo'")

	// Sanity check: make sure this didn't go through SSL
	output, err = runMysqlWithErr(t, params, "ssl echo")
	require.NoError(t, err)
	assert.Contains(t, output, "ssl_flag")
	assert.Contains(t, output, "OFF")
	assert.Contains(t, output, "1 row in set", "Unexpected output for 'ssl echo': %v", output)

	// UserData check: checks the server user data is correct.
	output, err = runMysqlWithErr(t, params, "userData echo")
	require.NoError(t, err)
	assert.Contains(t, output, "user1")
	assert.Contains(t, output, "user_data")
	assert.Contains(t, output, "userData1", "Unexpected output for 'userData echo': %v", output)

	// Permissions check: check a bad password is rejected.
	params.Pass = "bad"
	output, err = runMysqlWithErr(t, params, "select rows")
	require.Error(t, err)
	assert.Contains(t, output, "1045")
	assert.Contains(t, output, "28000")
	assert.Contains(t, output, "Access denied", "Unexpected output for invalid password: %v", output)

	// Permissions check: check an unknown user is rejected.
	params.Pass = "password1"
	params.Uname = "user2"
	output, err = runMysqlWithErr(t, params, "select rows")
	require.Error(t, err)
	assert.Contains(t, output, "1045")
	assert.Contains(t, output, "28000")
	assert.Contains(t, output, "Access denied", "Unexpected output for invalid password: %v", output)

	// Uncomment to leave setup up for a while, to run tests manually.
	//	fmt.Printf("Listening to server on host '%v' port '%v'.\n", host, port)
	//	time.Sleep(60 * time.Minute)
}

func TestServerStats(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	th := &testHandler{}

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	defer authServer.close()

	l, err := NewListener("tcp", "127.0.0.1:", authServer, th, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err)
	host, port := getHostPort(t, l.Addr())
	// Setup the right parameters.
	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
	}
	l.SlowConnectWarnThreshold.Store(time.Nanosecond.Nanoseconds())
	go l.Accept()
	defer cleanupListener(ctx, l, params)

	timings.Reset()
	connCount.Reset()
	connAccept.Reset()
	connSlow.Reset()
	connRefuse.Reset()

	// Run an 'error' command.
	th.SetErr(sqlerror.NewSQLError(sqlerror.ERUnknownComError, sqlerror.SSNetError, "forced query error"))
	output, ok := runMysql(t, params, "error")
	require.False(t, ok, "mysql should have failed: %v", output)

	assert.Contains(t, output, "ERROR 1047 (08S01)")
	assert.Contains(t, output, "forced query error", "Unexpected output for 'error': %v", output)

	// Accept starts a goroutine to handle each incoming connection.
	// It's in that goroutine where live stats/gauges such as the
	// current connection counts are updated when the handle function
	// ends (e.g. connCount.Add(-1)).
	// So we wait for the expected value to avoid races and flakiness.
	// 1 second should be enough, but no reason to fail the test or
	// a CI workflow if the test is CPU starved.
	conditionWait := 10 * time.Second
	conditionTick := 10 * time.Millisecond
	assert.Eventually(t, func() bool {
		return connCount.Get() == int64(0)
	}, conditionWait, conditionTick, "connCount")
	assert.Eventually(t, func() bool {
		return connSlow.Get() == int64(1)
	}, conditionWait, conditionTick, "connSlow")

	assert.EqualValues(t, 1, connAccept.Get(), "connAccept")
	assert.EqualValues(t, 0, connRefuse.Get(), "connRefuse")

	expectedTimingDeltas := map[string]int64{
		"All":            2,
		connectTimingKey: 1,
		queryTimingKey:   1,
	}
	gotTimingCounts := timings.Counts()
	for key, got := range gotTimingCounts {
		expected := expectedTimingDeltas[key]
		assert.GreaterOrEqual(t, got, expected, "Expected Timing count delta %s should be >= %d, got %d", key, expected, got)
	}

	// Set the slow connect threshold to something high that we don't expect to trigger
	l.SlowConnectWarnThreshold.Store(time.Second.Nanoseconds())

	// Run a 'panic' command, other side should panic, recover and
	// close the connection.
	output, err = runMysqlWithErr(t, params, "panic")
	require.Error(t, err)
	assert.Contains(t, output, "ERROR 2013 (HY000)")
	// MariaDB might not print the MySQL bit here
	assert.Regexp(t, `Lost connection to( MySQL)? server during query`, output, "Unexpected output for 'panic': %v", output)

	assert.EqualValues(t, 0, connCount.Get(), "connCount")
	assert.EqualValues(t, 2, connAccept.Get(), "connAccept")
	assert.EqualValues(t, 1, connSlow.Get(), "connSlow")
	assert.EqualValues(t, 0, connRefuse.Get(), "connRefuse")
}

// TestClearTextServer creates a Server that needs clear text
// passwords from the client.
func TestClearTextServer(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	th := &testHandler{}

	authServer := NewAuthServerStaticWithAuthMethodDescription("", "", 0, MysqlClearPassword)
	authServer.entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	defer authServer.close()

	l, err := NewListener("tcp", "127.0.0.1:", authServer, th, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err)
	host, port := getHostPort(t, l.Addr())
	// Setup the right parameters.
	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
	}
	go l.Accept()
	defer cleanupListener(ctx, l, params)

	version, _ := runMysql(t, nil, "--version")
	isMariaDB := strings.Contains(version, "MariaDB")

	// Run a 'select rows' command with results.  This should fail
	// as clear text is not enabled by default on the client
	// (except MariaDB).
	l.AllowClearTextWithoutTLS.Store(true)
	sql := "select rows"
	output, ok := runMysql(t, params, sql)
	if ok {
		if isMariaDB {
			t.Logf("mysql should have failed but returned: %v\nbut letting it go on MariaDB", output)
		} else {
			require.Fail(t, "mysql should have failed but returned: %v", output)
		}
	} else {
		if strings.Contains(output, "No such file or directory") {
			t.Logf("skipping mysql clear text tests, as the clear text plugin cannot be loaded: %v", err)
			return
		}
		assert.Contains(t, output, "plugin not enabled", "Unexpected output for 'select rows': %v", output)
	}

	// Now enable clear text plugin in client, but server requires SSL.
	l.AllowClearTextWithoutTLS.Store(false)
	if !isMariaDB {
		sql = enableCleartextPluginPrefix + sql
	}
	output, ok = runMysql(t, params, sql)
	assert.False(t, ok, "mysql should have failed but returned: %v", output)
	assert.Contains(t, output, "Cannot use clear text authentication over non-SSL connections", "Unexpected output for 'select rows': %v", output)

	// Now enable clear text plugin, it should now work.
	l.AllowClearTextWithoutTLS.Store(true)
	output, ok = runMysql(t, params, sql)
	require.True(t, ok, "mysql failed: %v", output)

	assert.Contains(t, output, "nice name", "Unexpected output for 'select rows'")
	assert.Contains(t, output, "nicer name", "Unexpected output for 'select rows'")
	assert.Contains(t, output, "2 rows in set", "Unexpected output for 'select rows'")

	// Change password, make sure server rejects us.
	params.Pass = "bad"
	output, ok = runMysql(t, params, sql)
	assert.False(t, ok, "mysql should have failed but returned: %v", output)
	assert.Contains(t, output, "Access denied for user 'user1'", "Unexpected output for 'select rows': %v", output)
}

// TestDialogServer creates a Server that uses the dialog plugin on the client.
func TestDialogServer(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	th := &testHandler{}

	authServer := NewAuthServerStaticWithAuthMethodDescription("", "", 0, MysqlDialog)
	authServer.entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	defer authServer.close()

	l, err := NewListener("tcp", "127.0.0.1:", authServer, th, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err)
	l.AllowClearTextWithoutTLS.Store(true)
	host, port := getHostPort(t, l.Addr())
	// Setup the right parameters.
	params := &ConnParams{
		Host:    host,
		Port:    port,
		Uname:   "user1",
		Pass:    "password1",
		SslMode: vttls.Disabled,
	}
	go l.Accept()
	defer cleanupListener(ctx, l, params)

	sql := "select rows"
	output, ok := runMysql(t, params, sql)
	if strings.Contains(output, "No such file or directory") || strings.Contains(output, "Authentication plugin 'dialog' cannot be loaded") {
		t.Logf("skipping dialog plugin tests, as the dialog plugin cannot be loaded: %v", err)
		return
	}
	require.True(t, ok, "mysql failed: %v", output)
	assert.Contains(t, output, "nice name", "Unexpected output for 'select rows': %v", output)
	assert.Contains(t, output, "nicer name", "Unexpected output for 'select rows': %v", output)
	assert.Contains(t, output, "2 rows in set", "Unexpected output for 'select rows': %v", output)
}

// TestTLSServer creates a Server with TLS support, then uses mysql
// client to connect to it.
func TestTLSServer(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	th := &testHandler{}

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
	}}
	defer authServer.close()

	// Create the certs.
	root := t.TempDir()
	tlstest.CreateCA(root)
	tlstest.CreateSignedCert(root, tlstest.CA, "01", "server", "server.example.com")
	tlstest.CreateSignedCert(root, tlstest.CA, "02", "client", "Client Cert")

	// Create the listener, so we can get its host.
	// Below, we are enabling --ssl-verify-server-cert, which adds
	// a check that the common name of the certificate matches the
	// server host name we connect to.
	l, err := NewListener("tcp", "127.0.0.1:", authServer, th, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err)
	host := l.Addr().(*net.TCPAddr).IP.String()
	port := l.Addr().(*net.TCPAddr).Port
	// Setup the right parameters.
	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
		// SSL flags.
		SslMode:    vttls.VerifyIdentity,
		SslCa:      path.Join(root, "ca-cert.pem"),
		SslCert:    path.Join(root, "client-cert.pem"),
		SslKey:     path.Join(root, "client-key.pem"),
		ServerName: "server.example.com",
	}
	// Create the server with TLS config.
	serverConfig, err := vttls.ServerConfig(
		path.Join(root, "server-cert.pem"),
		path.Join(root, "server-key.pem"),
		path.Join(root, "ca-cert.pem"),
		"",
		"",
		tls.VersionTLS12)
	require.NoError(t, err)
	l.TLSConfig.Store(serverConfig)
	go l.Accept()
	defer cleanupListener(ctx, l, params)

	connCountByTLSVer.ResetAll()

	// Run a 'select rows' command with results.
	conn, err := Connect(ctx, params)
	// output, ok := runMysql(t, params, "select rows")
	require.NoError(t, err)
	results, err := conn.ExecuteFetch("select rows", 1000, true)
	require.NoError(t, err)
	output := ""
	for _, row := range results.Rows {
		r := make([]string, 0)
		for _, col := range row {
			r = append(r, col.String())
		}
		output = output + strings.Join(r, ",") + "\n"
	}

	assert.Equal(t, "nice name", results.Rows[0][1].ToString())
	assert.Equal(t, "nicer name", results.Rows[1][1].ToString())
	assert.Equal(t, 2, len(results.Rows))

	// make sure this went through SSL
	results, err = conn.ExecuteFetch("ssl echo", 1000, true)
	require.NoError(t, err)
	assert.Equal(t, "ON", results.Rows[0][0].ToString())

	// Find out which TLS version the connection actually used,
	// so we can check that the corresponding counter was incremented.
	tlsVersion := conn.conn.(*tls.Conn).ConnectionState().Version

	checkCountForTLSVer(t, tlsVersionToString(tlsVersion), 1)
	conn.Close()

}

// TestTLSRequired creates a Server with TLS required, then tests that an insecure mysql
// client is rejected
func TestTLSRequired(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	th := &testHandler{}

	// Create the certs.
	root := t.TempDir()
	tlstest.CreateCA(root)
	tlstest.CreateSignedCert(root, tlstest.CA, "01", "server", "server.example.com")
	tlstest.CreateSignedCert(root, tlstest.CA, "02", "client", "Client Cert")
	tlstest.CreateSignedCert(root, tlstest.CA, "03", "revoked-client", "Revoked Client Cert")
	tlstest.RevokeCertAndRegenerateCRL(root, tlstest.CA, "revoked-client")

	params := &ConnParams{
		Uname:      "user1",
		Pass:       "password1",
		SslMode:    vttls.Disabled, // TLS is disabled at first
		ServerName: "server.example.com",
	}

	// Create the server with TLS config.
	serverConfig, err := vttls.ServerConfig(
		path.Join(root, "server-cert.pem"),
		path.Join(root, "server-key.pem"),
		path.Join(root, "ca-cert.pem"),
		path.Join(root, "ca-crl.pem"),
		"",
		tls.VersionTLS12)
	require.NoError(t, err)

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
	}}
	defer authServer.close()

	var l *Listener
	setupServer := func() {
		// Create the listener, so we can get its host.
		// Below, we are enabling --ssl-verify-server-cert, which adds
		// a check that the common name of the certificate matches the
		// server host name we connect to.
		l, err = NewListener("tcp", "127.0.0.1:", authServer, th, 0, 0, false, false, 0, 0, false)
		require.NoError(t, err)
		host := l.Addr().(*net.TCPAddr).IP.String()
		port := l.Addr().(*net.TCPAddr).Port
		l.TLSConfig.Store(serverConfig)
		l.RequireSecureTransport = true
		go l.Accept()
		params.Host = host
		params.Port = port
	}
	setupServer()

	defer cleanupListener(ctx, l, params)

	// This test calls Connect multiple times so we add handling for when the
	// listener goes away for any reason.
	connectWithGoneServerHandling := func() (*Conn, error) {
		conn, err := Connect(ctx, params)
		if sqlErr, ok := sqlerror.NewSQLErrorFromError(err).(*sqlerror.SQLError); ok && sqlErr.Num == sqlerror.CRConnHostError {
			cleanupListener(ctx, l, params)
			setupServer()
			conn, err = Connect(ctx, params)
		}
		return conn, err
	}

	conn, err := connectWithGoneServerHandling()
	require.ErrorContains(t, err, "Code: UNAVAILABLE")
	require.ErrorContains(t, err, "server does not allow insecure connections, client must use SSL/TLS")
	require.ErrorContains(t, err, "(errno 1105) (sqlstate HY000)")
	if conn != nil {
		conn.Close()
	}

	// setup conn params with TLS
	params.SslMode = vttls.VerifyIdentity
	params.SslCa = path.Join(root, "ca-cert.pem")
	params.SslCert = path.Join(root, "client-cert.pem")
	params.SslKey = path.Join(root, "client-key.pem")

	conn, err = connectWithGoneServerHandling()
	require.NoError(t, err)
	if conn != nil {
		conn.Close()
	}

	// setup conn params with TLS, but with a revoked client certificate
	params.SslCert = path.Join(root, "revoked-client-cert.pem")
	params.SslKey = path.Join(root, "revoked-client-key.pem")
	conn, err = connectWithGoneServerHandling()
	require.ErrorContains(t, err, "remote error: tls: bad certificate")
	if conn != nil {
		conn.Close()
	}
}

func TestCachingSha2PasswordAuthWithTLS(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	th := &testHandler{}

	authServer := NewAuthServerStaticWithAuthMethodDescription("", "", 0, CachingSha2Password)
	authServer.entries["user1"] = []*AuthServerStaticEntry{
		{Password: "password1"},
	}
	defer authServer.close()

	// Create the certs.
	root := t.TempDir()
	tlstest.CreateCA(root)
	tlstest.CreateSignedCert(root, tlstest.CA, "01", "server", "server.example.com")
	tlstest.CreateSignedCert(root, tlstest.CA, "02", "client", "Client Cert")

	// Create the listener, so we can get its host.
	l, err := NewListener("tcp", "127.0.0.1:", authServer, th, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err, "NewListener failed: %v", err)
	host := l.Addr().(*net.TCPAddr).IP.String()
	port := l.Addr().(*net.TCPAddr).Port
	// Create the server with TLS config.
	serverConfig, err := vttls.ServerConfig(
		path.Join(root, "server-cert.pem"),
		path.Join(root, "server-key.pem"),
		path.Join(root, "ca-cert.pem"),
		"",
		"",
		tls.VersionTLS12)
	require.NoError(t, err, "TLSServerConfig failed: %v", err)
	// Setup the right parameters.
	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
		// SSL flags.
		SslMode:    vttls.VerifyIdentity,
		SslCa:      path.Join(root, "ca-cert.pem"),
		SslCert:    path.Join(root, "client-cert.pem"),
		SslKey:     path.Join(root, "client-key.pem"),
		ServerName: "server.example.com",
	}
	l.TLSConfig.Store(serverConfig)
	go l.Accept()
	defer cleanupListener(ctx, l, params)

	// Connection should fail, as server requires SSL for caching_sha2_password.
	conn, err := Connect(ctx, params)
	require.NoError(t, err, "unexpected connection error: %v", err)

	defer conn.Close()

	// Run a 'select rows' command with results.
	result, err := conn.ExecuteFetch("select rows", 10000, true)
	require.NoError(t, err, "ExecuteFetch failed: %v", err)

	utils.MustMatch(t, result, selectRowsResult)

	// Send a ComQuit to avoid the error message on the server side.
	conn.writeComQuit()
}

type alwaysFallbackAuth struct{}

func (a *alwaysFallbackAuth) UserEntryWithCacheHash(conn *Conn, salt []byte, user string, authResponse []byte, remoteAddr net.Addr) (Getter, CacheState, error) {
	return &StaticUserData{}, AuthNeedMoreData, nil
}

// newAuthServerAlwaysFallback returns a new empty AuthServerStatic
// which will always request more data to trigger fallback auth path
// for caching sha2.
func newAuthServerAlwaysFallback(file, jsonConfig string, reloadInterval time.Duration) *AuthServerStatic {
	a := &AuthServerStatic{
		file:           file,
		jsonConfig:     jsonConfig,
		reloadInterval: reloadInterval,
		entries:        make(map[string][]*AuthServerStaticEntry),
	}

	authMethod := NewSha2CachingAuthMethod(&alwaysFallbackAuth{}, a, a)
	a.methods = []AuthMethod{authMethod}

	return a
}

func TestCachingSha2PasswordAuthWithMoreData(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	th := &testHandler{}

	authServer := newAuthServerAlwaysFallback("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{
		{Password: "password1"},
	}
	defer authServer.close()

	// Create the certs.
	root := t.TempDir()
	tlstest.CreateCA(root)
	tlstest.CreateSignedCert(root, tlstest.CA, "01", "server", "server.example.com")
	tlstest.CreateSignedCert(root, tlstest.CA, "02", "client", "Client Cert")

	// Create the listener, so we can get its host.
	l, err := NewListener("tcp", "127.0.0.1:", authServer, th, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err, "NewListener failed: %v", err)
	host := l.Addr().(*net.TCPAddr).IP.String()
	port := l.Addr().(*net.TCPAddr).Port
	// Create the server with TLS config.
	serverConfig, err := vttls.ServerConfig(
		path.Join(root, "server-cert.pem"),
		path.Join(root, "server-key.pem"),
		path.Join(root, "ca-cert.pem"),
		"",
		"",
		tls.VersionTLS12)
	require.NoError(t, err, "TLSServerConfig failed: %v", err)
	// Setup the right parameters.
	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
		// SSL flags.
		SslMode:    vttls.VerifyIdentity,
		SslCa:      path.Join(root, "ca-cert.pem"),
		SslCert:    path.Join(root, "client-cert.pem"),
		SslKey:     path.Join(root, "client-key.pem"),
		ServerName: "server.example.com",
	}
	l.TLSConfig.Store(serverConfig)
	go l.Accept()
	defer cleanupListener(ctx, l, params)

	// Connection should fail, as server requires SSL for caching_sha2_password.
	conn, err := Connect(ctx, params)
	require.NoError(t, err, "unexpected connection error: %v", err)

	defer conn.Close()

	// Run a 'select rows' command with results.
	result, err := conn.ExecuteFetch("select rows", 10000, true)
	require.NoError(t, err, "ExecuteFetch failed: %v", err)

	utils.MustMatch(t, result, selectRowsResult)

	// Send a ComQuit to avoid the error message on the server side.
	conn.writeComQuit()
}

func TestCachingSha2PasswordAuthWithoutTLS(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	th := &testHandler{}

	authServer := NewAuthServerStaticWithAuthMethodDescription("", "", 0, CachingSha2Password)
	authServer.entries["user1"] = []*AuthServerStaticEntry{
		{Password: "password1"},
	}
	defer authServer.close()

	// Create the listener.
	l, err := NewListener("tcp", "127.0.0.1:", authServer, th, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err, "NewListener failed: %v", err)
	host := l.Addr().(*net.TCPAddr).IP.String()
	port := l.Addr().(*net.TCPAddr).Port
	// Setup the right parameters.
	params := &ConnParams{
		Host:    host,
		Port:    port,
		Uname:   "user1",
		Pass:    "password1",
		SslMode: vttls.Disabled,
	}
	go l.Accept()
	defer cleanupListener(ctx, l, params)

	// Connection should fail, as server requires SSL for caching_sha2_password.
	_, err = Connect(ctx, params)
	if err == nil || !strings.Contains(err.Error(), "No authentication methods available for authentication") {
		t.Fatalf("unexpected connection error: %v", err)
	}
}

func checkCountForTLSVer(t *testing.T, version string, expected int64) {
	connCounts := connCountByTLSVer.Counts()
	count, ok := connCounts[version]
	assert.True(t, ok, "No count found for version %s", version)
	assert.Equal(t, expected, count, "Unexpected connection count for version %s", version)
}

func TestErrorCodes(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	th := &testHandler{}

	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	defer authServer.close()

	l, err := NewListener("tcp", "127.0.0.1:", authServer, th, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err)
	host, port := getHostPort(t, l.Addr())
	// Setup the right parameters.
	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
	}
	go l.Accept()
	defer cleanupListener(ctx, l, params)

	client, err := Connect(ctx, params)
	require.NoError(t, err)

	// Test that the right mysql errno/sqlstate are returned for various
	// internal vitess errors
	tests := []struct {
		err      error
		code     sqlerror.ErrorCode
		sqlState string
		text     string
	}{
		{
			err: vterrors.Errorf(
				vtrpcpb.Code_INVALID_ARGUMENT,
				"invalid argument"),
			code:     sqlerror.ERUnknownError,
			sqlState: sqlerror.SSUnknownSQLState,
			text:     "invalid argument",
		},
		{
			err: vterrors.Errorf(
				vtrpcpb.Code_INVALID_ARGUMENT,
				"(errno %v) (sqlstate %v) invalid argument with errno", sqlerror.ERDupEntry, sqlerror.SSConstraintViolation),
			code:     sqlerror.ERDupEntry,
			sqlState: sqlerror.SSConstraintViolation,
			text:     "invalid argument with errno",
		},
		{
			err: vterrors.Errorf(
				vtrpcpb.Code_DEADLINE_EXCEEDED,
				"connection deadline exceeded"),
			code:     sqlerror.ERQueryInterrupted,
			sqlState: sqlerror.SSQueryInterrupted,
			text:     "deadline exceeded",
		},
		{
			err: vterrors.Errorf(
				vtrpcpb.Code_RESOURCE_EXHAUSTED,
				"query pool timeout"),
			code:     sqlerror.ERTooManyUserConnections,
			sqlState: sqlerror.SSClientError,
			text:     "resource exhausted",
		},
		{
			err:      vterrors.Wrap(vterrors.Errorf(vtrpcpb.Code_ABORTED, "Row count exceeded 10000"), "wrapped"),
			code:     sqlerror.ERQueryInterrupted,
			sqlState: sqlerror.SSQueryInterrupted,
			text:     "aborted",
		},
	}

	for _, test := range tests {
		t.Run(test.err.Error(), func(t *testing.T) {
			th.SetErr(sqlerror.NewSQLErrorFromError(test.err))
			rs, err := client.ExecuteFetch("error", 100, false)
			require.Error(t, err, "mysql should have failed but returned: %v", rs)
			serr, ok := err.(*sqlerror.SQLError)
			require.True(t, ok, "mysql should have returned a SQLError")

			assert.Equal(t, test.code, serr.Number(), "error in %s: want code %v got %v", test.text, test.code, serr.Number())
			assert.Equal(t, test.sqlState, serr.SQLState(), "error in %s: want sqlState %v got %v", test.text, test.sqlState, serr.SQLState())
			assert.Contains(t, serr.Error(), test.err.Error())
		})
	}
}

const enableCleartextPluginPrefix = "enable-cleartext-plugin: "

// runMysql forks a mysql command line process connecting to the provided server.
func runMysql(t *testing.T, params *ConnParams, command string) (string, bool) {
	output, err := runMysqlWithErr(t, params, command)
	if err != nil {
		return output, false
	}
	return output, true

}
func runMysqlWithErr(t *testing.T, params *ConnParams, command string) (string, error) {
	dir, err := venv.VtMysqlRoot()
	require.NoError(t, err)
	name, err := binaryPath(dir, "mysql")
	require.NoError(t, err)
	// The args contain '-v' 3 times, to switch to very verbose output.
	// In particular, it has the message:
	// Query OK, 1 row affected (0.00 sec)
	args := []string{
		"-v", "-v", "-v",
	}
	if strings.HasPrefix(command, enableCleartextPluginPrefix) {
		command = command[len(enableCleartextPluginPrefix):]
		args = append(args, "--enable-cleartext-plugin")
	}
	if command == "--version" {
		args = append(args, command)
	} else {
		args = append(args, "-e", command)
		if params.UnixSocket != "" {
			args = append(args, "-S", params.UnixSocket)
		} else {
			args = append(args,
				"-h", params.Host,
				"-P", fmt.Sprintf("%v", params.Port))
		}
		if params.Uname != "" {
			args = append(args, "-u", params.Uname)
		}
		if params.Pass != "" {
			args = append(args, "-p"+params.Pass)
		}
		if params.DbName != "" {
			args = append(args, "-D", params.DbName)
		}
		if params.SslEnabled() {
			args = append(args,
				"--ssl",
				"--ssl-ca", params.SslCa,
				"--ssl-cert", params.SslCert,
				"--ssl-key", params.SslKey,
				"--ssl-verify-server-cert")
		}
	}
	env := []string{
		"LD_LIBRARY_PATH=" + path.Join(dir, "lib/mysql"),
	}

	t.Logf("Running mysql command: %v %v", name, args)
	cmd := exec.Command(name, args...)
	cmd.Env = env
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	output := string(out)
	if err != nil {
		return output, err
	}
	return output, nil
}

// binaryPath does a limited path lookup for a command,
// searching only within sbin and bin in the given root.
//
// FIXME(alainjobart) move this to vt/env, and use it from
// go/vt/mysqlctl too.
func binaryPath(root, binary string) (string, error) {
	subdirs := []string{"sbin", "bin"}
	for _, subdir := range subdirs {
		binPath := path.Join(root, subdir, binary)
		if _, err := os.Stat(binPath); err == nil {
			return binPath, nil
		}
	}
	return "", fmt.Errorf("%s not found in any of %s/{%s}",
		binary, root, strings.Join(subdirs, ","))
}

func TestListenerShutdown(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	th := &testHandler{}
	authServer := NewAuthServerStatic("", "", 0)
	authServer.entries["user1"] = []*AuthServerStaticEntry{{
		Password: "password1",
		UserData: "userData1",
	}}
	defer authServer.close()

	l, err := NewListener("tcp", "127.0.0.1:", authServer, th, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err)
	host, port := getHostPort(t, l.Addr())
	// Setup the right parameters.
	params := &ConnParams{
		Host:  host,
		Port:  port,
		Uname: "user1",
		Pass:  "password1",
	}
	go l.Accept()
	defer cleanupListener(ctx, l, params)

	connRefuse.Reset()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	conn, err := Connect(ctx, params)
	require.NoError(t, err)

	err = conn.Ping()
	require.NoError(t, err)

	l.Shutdown()

	require.Eventually(t, func() bool {
		return connRefuse.Get() == 1
	}, 1*time.Minute, 100*time.Millisecond, "could not reach the desired connRefuse value")

	err = conn.Ping()
	require.EqualError(t, err, "Server shutdown in progress (errno 1053) (sqlstate 08S01)")
	sqlErr, ok := err.(*sqlerror.SQLError)
	require.True(t, ok, "Wrong error type: %T", err)

	require.Equal(t, sqlerror.ERServerShutdown, sqlErr.Number())
	require.Equal(t, sqlerror.SSNetError, sqlErr.SQLState())
	require.Equal(t, "Server shutdown in progress", sqlErr.Message)
}

func TestParseConnAttrs(t *testing.T) {
	expected := map[string]string{
		"_client_version": "8.0.11",
		"program_name":    "mysql",
		"_pid":            "22850",
		"_platform":       "x86_64",
		"_os":             "linux-glibc2.12",
		"_client_name":    "libmysql",
	}

	data := []byte{0x70, 0x04, 0x5f, 0x70, 0x69, 0x64, 0x05, 0x32, 0x32, 0x38, 0x35, 0x30, 0x09, 0x5f, 0x70, 0x6c,
		0x61, 0x74, 0x66, 0x6f, 0x72, 0x6d, 0x06, 0x78, 0x38, 0x36, 0x5f, 0x36, 0x34, 0x03, 0x5f, 0x6f,
		0x73, 0x0f, 0x6c, 0x69, 0x6e, 0x75, 0x78, 0x2d, 0x67, 0x6c, 0x69, 0x62, 0x63, 0x32, 0x2e, 0x31,
		0x32, 0x0c, 0x5f, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x08, 0x6c,
		0x69, 0x62, 0x6d, 0x79, 0x73, 0x71, 0x6c, 0x0f, 0x5f, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x5f,
		0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x06, 0x38, 0x2e, 0x30, 0x2e, 0x31, 0x31, 0x0c, 0x70,
		0x72, 0x6f, 0x67, 0x72, 0x61, 0x6d, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x05, 0x6d, 0x79, 0x73, 0x71, 0x6c}

	attrs, pos, err := parseConnAttrs(data, 0)
	require.NoError(t, err)
	require.Equal(t, 113, pos)
	for k, v := range expected {
		val, ok := attrs[k]
		require.True(t, ok, "Error reading key %s from connection attributes: attrs: %-v", k, attrs)
		require.Equal(t, v, val, "Unexpected value found in attrs for key %s", k)
	}
}

func TestServerFlush(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	mysqlServerFlushDelay := 10 * time.Millisecond
	th := &testHandler{}

	l, err := NewListener("tcp", "127.0.0.1:", NewAuthServerNone(), th, 0, 0, false, false, 0, mysqlServerFlushDelay, false)
	require.NoError(t, err)
	host, port := getHostPort(t, l.Addr())
	params := &ConnParams{
		Host: host,
		Port: port,
	}
	go l.Accept()
	defer cleanupListener(ctx, l, params)

	c, err := Connect(ctx, params)
	require.NoError(t, err)
	defer c.Close()

	start := time.Now()
	err = c.ExecuteStreamFetch("50ms delay")
	require.NoError(t, err)

	flds, err := c.Fields()
	require.NoError(t, err)
	if duration, want := time.Since(start), 20*time.Millisecond; duration < mysqlServerFlushDelay || duration > want {
		assert.Fail(t, "duration out of expected range", "duration: %v, want between %v and %v", duration.String(), (mysqlServerFlushDelay).String(), want.String())
	}
	want1 := []*querypb.Field{{
		Name:    "result",
		Type:    querypb.Type_VARCHAR,
		Charset: uint32(collations.MySQL8().DefaultConnectionCharset()),
	}}
	assert.Equal(t, want1, flds)

	row, err := c.FetchNext(nil)
	require.NoError(t, err)
	if duration, want := time.Since(start), 50*time.Millisecond; duration < want {
		assert.Fail(t, "duration is too low", "duration: %v, want > %v", duration, want)
	}
	want2 := []sqltypes.Value{sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("delayed"))}
	assert.Equal(t, want2, row)

	row, err = c.FetchNext(nil)
	require.NoError(t, err)
	assert.Nil(t, row)
}

func TestTcpKeepAlive(t *testing.T) {
	ctx := utils.LeakCheckContext(t)
	th := &testHandler{}

	l, err := NewListener("tcp", "127.0.0.1:", NewAuthServerNone(), th, 0, 0, false, false, 0, 0, false)
	require.NoError(t, err)
	host, port := getHostPort(t, l.Addr())
	params := &ConnParams{
		Host: host,
		Port: port,
	}
	go l.Accept()
	defer cleanupListener(ctx, l, params)

	// on connect, the tcp method should be called.
	c, err := Connect(ctx, params)
	require.NoError(t, err)
	defer c.Close()
	require.True(t, th.lastConn.keepAliveOn, "tcp property method not called")

	// close the connection
	th.lastConn.Close()

	// now calling this method should fail.
	err = setTcpConnProperties(th.lastConn.conn.(*net.TCPConn), 0)
	require.ErrorContains(t, err, "unable to enable keepalive on tcp connection")
}
