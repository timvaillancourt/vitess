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

// You can modify this file to hook up a different logging library instead of glog.
// If you adapt to a different logging framework, you may need to use that
// framework's equivalent of *Depth() functions so the file and line number printed
// point to the real caller instead of your adapter function.

package log

import (
	"fmt"
	"os"
	//"strconv"
	//"sync/atomic"

	//"github.com/spf13/pflag"
	"go.uber.org/zap"
)

var logger *zap.SugaredLogger

func init() {
	logCfg := zap.NewProductionConfig()
	lz, err := logCfg.Build()
	if err != nil {
		fmt.Printf("failed to configure logger: %+v", err)
		os.Exit(1)
	}
	logger = lz.Sugar()
}

var (
	// Flush ensures any pending I/O is written.
	Flush = func() {
		_ = logger.Sync()
		return
	}
	Sync = logger.Sync

	// Debug formats arguments like fmt.Print.
	Debug = logger.Debug
	// Debugf formats arguments like fmt.Printf.
	Debugf = logger.Debugf

	// Info formats arguments like fmt.Print.
	Info = logger.Info
	// Infof formats arguments like fmt.Printf.
	Infof = logger.Infof

	// Warn formats arguments like fmt.Print.
	Warn = logger.Warn
	// Warnf formats arguments like fmt.Printf.
	Warnf = logger.Warnf
	// Warning formats arguments like fmt.Print.
	Warning = logger.Warn
	// Warningf formats arguments like fmt.Printf.
	Warningf = logger.Warnf

	// Error formats arguments like fmt.Print.
	Error = logger.Error
	// Errorf formats arguments like fmt.Printf.
	Errorf = logger.Errorf

	// Exit formats arguments like fmt.Print.
	Exit = logger.Fatal
	// Exitf formats arguments like fmt.Printf.
	Exitf = logger.Fatalf
	// Fatal formats arguments like fmt.Print.
	Fatal = logger.Fatal
	// Fatalf formats arguments like fmt.Printf
	Fatalf = logger.Fatalf
)

// RegisterFlags installs log flags on the given FlagSet.
//
// `go/cmd/*` entrypoints should either use servenv.ParseFlags(WithArgs)? which
// calls this function, or call this function directly before parsing
// command-line arguments.
/*
func RegisterFlags(fs *pflag.FlagSet) {
	flagVal := logRotateMaxSize{
		val: fmt.Sprintf("%d", atomic.LoadUint64(&glog.MaxSize)),
	}
	fs.Var(&flagVal, "log_rotate_max_size", "size in bytes at which logs are rotated (glog.MaxSize)")
}

// logRotateMaxSize implements pflag.Value and is used to
// try and provide thread-safe access to glog.MaxSize.
type logRotateMaxSize struct {
	val string
}

func (lrms *logRotateMaxSize) Set(s string) error {
	maxSize, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return err
	}
	atomic.StoreUint64(&glog.MaxSize, maxSize)
	lrms.val = s
	return nil
}

func (lrms *logRotateMaxSize) String() string {
	return lrms.val
}

func (lrms *logRotateMaxSize) Type() string {
	return "uint64"
}
*/
