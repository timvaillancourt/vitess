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

package grpcvtgateconn

import (
	"context"
	"io"
	"net"
	"os"
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/utils"
	"vitess.io/vitess/go/vt/vtgate/grpcvtgateservice"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

// TestGRPCVTGateConn makes sure the grpc service works
func TestGRPCVTGateConn(t *testing.T) {
	// fake service
	service := CreateFakeServer(t)

	// listen on a random port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// Create a gRPC server and listen on the port
	server := grpc.NewServer()
	grpcvtgateservice.RegisterForTest(server, service)
	go server.Serve(listener)

	// Create a Go RPC client connecting to the server
	ctx := context.Background()
	client, err := dial(ctx, listener.Addr().String())
	require.NoError(t, err)
	RegisterTestDialProtocol(client)

	// run the test suite
	RunTests(t, client, service)
	RunErrorTests(t, service)

	// and clean up
	client.Close()
}

// TestGRPCVTGateConnAuth makes sure the grpc with auth plugin works
func TestGRPCVTGateConnAuth(t *testing.T) {
	var opts []grpc.ServerOption
	// fake service
	service := CreateFakeServer(t)

	// listen on a random port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Cannot listen: %v", err)
	}

	// add auth interceptors
	opts = append(opts, grpc.StreamInterceptor(servenv.FakeAuthStreamInterceptor))
	opts = append(opts, grpc.UnaryInterceptor(servenv.FakeAuthUnaryInterceptor))

	// Create a gRPC server and listen on the port
	server := grpc.NewServer(opts...)
	grpcvtgateservice.RegisterForTest(server, service)
	go server.Serve(listener)

	authJSON := `{
         "Username": "valid",
         "Password": "valid"
        }`

	f, err := os.CreateTemp("", "static_auth_creds.json")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	_, err = io.WriteString(f, authJSON)
	require.NoError(t, err)
	err = f.Close()
	require.NoError(t, err)

	// Create a Go RPC client connecting to the server
	ctx := context.Background()
	fs := pflag.NewFlagSet("", pflag.ContinueOnError)
	grpcclient.RegisterFlags(fs)

	grpcclient.ResetStaticAuth()
	authStaticClientCredsFlag := utils.GetFlagVariantForTests("--grpc-auth-static-client-creds")

	// Parse the flag using the chosen variant
	err = fs.Parse([]string{
		authStaticClientCredsFlag,
		f.Name(),
	})

	require.NoError(t, err, "failed to set `%s=%s`", authStaticClientCredsFlag, f.Name())
	client, err := dial(ctx, listener.Addr().String())
	require.NoError(t, err)
	RegisterTestDialProtocol(client)

	// run the test suite
	RunTests(t, client, service)
	RunErrorTests(t, service)

	// and clean up
	client.Close()

	invalidAuthJSON := `{
	 "Username": "invalid",
	 "Password": "valid"
	}`

	f, err = os.CreateTemp("", "static_auth_creds.json")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	_, err = io.WriteString(f, invalidAuthJSON)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	// Create a Go RPC client connecting to the server
	ctx = context.Background()
	fs = pflag.NewFlagSet("", pflag.ContinueOnError)
	grpcclient.RegisterFlags(fs)

	grpcclient.ResetStaticAuth()
	authStaticClientCredsFlag = utils.GetFlagVariantForTests("--grpc-auth-static-client-creds")
	err = fs.Parse([]string{
		authStaticClientCredsFlag,
		f.Name(),
	})

	require.NoError(t, err, "failed to set `%s=%s`", authStaticClientCredsFlag, f.Name())

	client, err = dial(ctx, listener.Addr().String())
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	RegisterTestDialProtocol(client)
	conn, _ := vtgateconn.DialProtocol(context.Background(), "test", "")
	// run the test suite
	_, err = conn.Session("", nil).Execute(context.Background(), "select * from t", nil, false)
	want := "rpc error: code = Unauthenticated desc = username and password must be provided"
	if err == nil || err.Error() != want {
		t.Errorf("expected auth failure:\n%v, want\n%s", err, want)
	}
	// and clean up again
	client.Close()
}
