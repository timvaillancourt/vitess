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

package tabletconn

import (
	"context"
	"sync"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/utils"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/queryservice"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	// ConnClosed is returned when the underlying connection was closed.
	ConnClosed = vterrors.New(vtrpcpb.Code_UNAVAILABLE, "vttablet: Connection Closed")

	tabletProtocol = "grpc"
)

// RegisterFlags registers the tabletconn flags on a given flagset. It is
// exported for tests that need to inject a particular TabletProtocol.
func RegisterFlags(fs *pflag.FlagSet) {
	utils.SetFlagStringVar(fs, &tabletProtocol, "tablet-protocol", "grpc", "Protocol to use to make queryservice RPCs to vttablets.")
}

func init() {
	for _, cmd := range []string{
		"vtcombo",
		"vtctl",
		"vtctld",
		"vtctldclient",
		"vtgate",
		"vttablet",
	} {
		servenv.OnParseFor(cmd, RegisterFlags)
	}
}

// TabletDialer represents a function that will return a QueryService
// object that can communicate with a tablet. Only the tablet's
// HostName and PortMap should be used (and maybe the alias for debug
// messages).
//
// timeout represents the connection timeout. If set to 0, this
// connection should be established in the background and the
// TabletDialer should return right away.
type TabletDialer func(ctx context.Context, tablet *topodatapb.Tablet, failFast grpcclient.FailFast) (queryservice.QueryService, error)

var dialers = make(map[string]TabletDialer)

// mu This mutex helps us prevent data races when registering / getting dialers
var mu sync.Mutex

// RegisterDialer is meant to be used by TabletDialer implementations
// to self register.
func RegisterDialer(name string, dialer TabletDialer) {
	mu.Lock()
	defer mu.Unlock()
	if _, ok := dialers[name]; ok {
		log.Fatalf("Dialer %s already exists", name)
	}
	dialers[name] = dialer
}

// GetDialer returns the dialer to use, described by the command line flag
func GetDialer() TabletDialer {
	mu.Lock()
	defer mu.Unlock()
	td, ok := dialers[tabletProtocol]
	if !ok {
		log.Exitf("No dialer registered for tablet protocol %s", tabletProtocol)
	}
	return td
}
