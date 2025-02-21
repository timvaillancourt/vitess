/*
Copyright 2020 The Vitess Authors.

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

package sqlerror

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func TestDemuxResourceExhaustedErrors(t *testing.T) {
	type testCase struct {
		msg  string
		want ErrorCode
	}

	cases := []testCase{
		{"misc", ERTooManyUserConnections},
		{"grpc: received message larger than max (99282 vs. 1234): trailer", ERNetPacketTooLarge},
		{"grpc: received message larger than max (1234 vs. 1234)", ERNetPacketTooLarge},
		{"header: grpc: received message larger than max (1234 vs. 1234)", ERNetPacketTooLarge},
		{"grpc: trying to send message larger than max (18015277 vs. 16777216)", ERNetPacketTooLarge},
		{"grpc: trying to send message larger than max (18015277 vs. 16777216): trailer", ERNetPacketTooLarge},
		{"header: grpc: trying to send message larger than max (18015277 vs. 16777216)", ERNetPacketTooLarge},
		// This should be explicitly handled by returning ERNetPacketTooLarge from the execturo directly
		// and therefore shouldn't need to be teased out of another error.
		{"in-memory row count exceeded allowed limit of 13", ERTooManyUserConnections},
		{"rpc error: code = ResourceExhausted desc = Transaction throttled", EROutOfResources},
	}

	for _, c := range cases {
		got := demuxResourceExhaustedErrors(c.msg)
		assert.Equalf(t, c.want, got, c.msg)
	}
}

func TestNewSQLErrorFromError(t *testing.T) {
	var tCases = []struct {
		err error
		num ErrorCode
		ha  HandlerErrorCode
		ss  string
	}{
		{
			err: vterrors.Errorf(vtrpc.Code_OK, "ok"),
			num: ERUnknownError,
			ss:  SSUnknownSQLState,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_CANCELED, "cancelled"),
			num: ERQueryInterrupted,
			ss:  SSQueryInterrupted,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_UNKNOWN, "unknown"),
			num: ERUnknownError,
			ss:  SSUnknownSQLState,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "invalid argument"),
			num: ERUnknownError,
			ss:  SSUnknownSQLState,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_DEADLINE_EXCEEDED, "deadline exceeded"),
			num: ERQueryInterrupted,
			ss:  SSQueryInterrupted,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_NOT_FOUND, "code not found"),
			num: ERUnknownError,
			ss:  SSUnknownSQLState,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_ALREADY_EXISTS, "already exists"),
			num: ERUnknownError,
			ss:  SSUnknownSQLState,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_PERMISSION_DENIED, "permission denied"),
			num: ERAccessDeniedError,
			ss:  SSAccessDeniedError,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_UNAUTHENTICATED, "unauthenticated"),
			num: ERAccessDeniedError,
			ss:  SSAccessDeniedError,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_RESOURCE_EXHAUSTED, "resource exhausted"),
			num: ERTooManyUserConnections,
			ss:  SSClientError,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "failed precondition"),
			num: ERUnknownError,
			ss:  SSUnknownSQLState,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_ABORTED, "aborted"),
			num: ERQueryInterrupted,
			ss:  SSQueryInterrupted,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_OUT_OF_RANGE, "out of range"),
			num: ERUnknownError,
			ss:  SSUnknownSQLState,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "unimplemented"),
			num: ERNotSupportedYet,
			ss:  SSClientError,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_INTERNAL, "internal"),
			num: ERInternalError,
			ss:  SSUnknownSQLState,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_UNAVAILABLE, "unavailable"),
			num: ERUnknownError,
			ss:  SSUnknownSQLState,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_DATA_LOSS, "data loss"),
			num: ERUnknownError,
			ss:  SSUnknownSQLState,
		},
		{
			err: vterrors.NewErrorf(vtrpc.Code_ALREADY_EXISTS, vterrors.DbCreateExists, "create db exists"),
			num: ERDbCreateExists,
			ss:  SSUnknownSQLState,
		},
		{
			err: vterrors.NewErrorf(vtrpc.Code_FAILED_PRECONDITION, vterrors.NoDB, "no db selected"),
			num: ERNoDb,
			ss:  SSNoDB,
		},
		{
			err: fmt.Errorf("just some random text here"),
			num: ERUnknownError,
			ss:  SSUnknownSQLState,
		},
		{
			err: fmt.Errorf("task error: Column 'val' cannot be null (errno 1048) (sqlstate 23000) during query: insert into _edf4846d_ab65_11ed_abb1_0a43f95f28a3_20230213061619_vrepl(id,val,ts) values (1,2,'2023-02-13 04:46:16'), (2,3,'2023-02-13 04:46:16'), (3,null,'2023-02-13 04:46:16')"),
			num: ERBadNullError,
			ss:  SSConstraintViolation,
		},
		{
			err: vterrors.Wrapf(fmt.Errorf("Column 'val' cannot be null (errno 1048) (sqlstate 23000) during query: insert into _edf4846d_ab65_11ed_abb1_0a43f95f28a3_20230213061619_vrepl(id,val,ts) values (1,2,'2023-02-13 04:46:16'), (2,3,'2023-02-13 04:46:16'), (3,null,'2023-02-13 04:46:16')"), "task error: %d", 17),
			num: ERBadNullError,
			ss:  SSConstraintViolation,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_RESOURCE_EXHAUSTED, "vttablet: rpc error: code = ResourceExhausted desc = Transaction throttled"),
			num: EROutOfResources,
			ss:  SSUnknownSQLState,
		},
		{
			err: vterrors.Errorf(vtrpc.Code_RESOURCE_EXHAUSTED, "vttablet: rpc error: code = AlreadyExists desc = Duplicate entry '1' for key 'PRIMARY' (errno 1062) (sqlstate 23000) (CallerID: userData1): Sql: \"insert into test(id, `name`) values (:vtg1 /* INT64 */, :vtg2 /* VARCHAR */)\", BindVars: {vtg1: \"type:INT64 value:\\\"1\\\"\"vtg2: \"type:VARCHAR value:\\\"(errno 1366) (sqlstate 10000)\\\"\"}"),
			num: ERDupEntry,
			ss:  SSConstraintViolation,
		},
		{
			err: fmt.Errorf("ERROR HY000: Got error 204 - 'No more room in disk' during COMMIT"),
			num: ERUnknownError,
			ss:  SSUnknownSQLState,
			ha:  HaErrDiskFullNowait,
		},
		{
			err: fmt.Errorf("COMMIT failed w/ error: Got error 204 - 'No more room in disk' during COMMIT (errno 1180) (sqlstate HY000) during query: commit"),
			num: ERErrorDuringCommit,
			ss:  SSUnknownSQLState,
			ha:  HaErrDiskFullNowait,
		},
		{
			err: fmt.Errorf("COMMIT failed w/ error: Got error 149 - 'Lock deadlock; Retry transaction' during COMMIT (errno 1180) (sqlstate HY000) during query: commit"),
			num: ERErrorDuringCommit,
			ss:  SSUnknownSQLState,
			ha:  HaErrLockDeadlock,
		},
	}

	for _, tc := range tCases {
		t.Run(tc.err.Error(), func(t *testing.T) {
			var err *SQLError
			require.ErrorAs(t, NewSQLErrorFromError(tc.err), &err)
			assert.Equal(t, tc.num, err.Number())
			assert.Equal(t, tc.ss, err.SQLState())
			ha := err.HaErrorCode()
			assert.Equal(t, tc.ha, ha)
		})
	}
}
