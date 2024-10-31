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

// package logutil provides some utilities for logging using glog and
// redirects the stdlib logging to glog.

package logutil

import (
	stdlog "log"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/log"
)

// RegisterFlags installs logutil flags on the given FlagSet.
//
// `go/cmd/*` entrypoints should either use servenv.ParseFlags(WithArgs)? which
// calls this function, or call this function directly before parsing
// command-line arguments.
func RegisterFlags(fs *pflag.FlagSet) {
	fs.DurationVar(&keepLogsByCtime, "keep_logs", keepLogsByCtime, "keep logs for this long (using ctime) (zero to keep forever)")
	fs.DurationVar(&keepLogsByMtime, "keep_logs_by_mtime", keepLogsByMtime, "keep logs for this long (using mtime) (zero to keep forever)")
	fs.DurationVar(&purgeLogsInterval, "purge_logs_interval", purgeLogsInterval, "how often try to remove old logs")
}

// RegisterLogFlags installs logutils flags for tablets.
func RegisterTabletFlags(fs *pflag.FlagSet) {
	fs.StringVar(&tabletFormatterName, "tablet-log-formatter", tabletFormatterName, "format to log tablets")
}

type logShim struct{}

func (shim *logShim) Write(buf []byte) (n int, err error) {
	log.Info(string(buf))
	return len(buf), nil
}

func init() {
	stdlog.SetPrefix("log: ")
	stdlog.SetFlags(0)
	stdlog.SetOutput(new(logShim))
}
