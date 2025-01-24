/*
Copyright 2025 The Vitess Authors.

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

package health

import (
	"context"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/servenv"
)

var (
	stalledDiskWriteDir      = ""
	stalledDiskWriteTimeout  = 30 * time.Second
	stalledDiskWriteInterval = 5 * time.Second
)

func registerDiskMonitorFlags(fs *pflag.FlagSet) {
	fs.StringVar(&stalledDiskWriteDir, "disk-write-dir", stalledDiskWriteDir, "if provided, tablet will attempt to write a file to this directory to check if the disk is stalled")
	fs.DurationVar(&stalledDiskWriteTimeout, "disk-write-timeout", stalledDiskWriteTimeout, "if writes exceed this duration, the disk is considered stalled")
	fs.DurationVar(&stalledDiskWriteInterval, "disk-write-interval", stalledDiskWriteInterval, "how often to write to the disk to check whether it is stalled")
}

func init() {
	servenv.OnParseFor("vtcombo", registerDiskMonitorFlags)
	servenv.OnParseFor("vttablet", registerDiskMonitorFlags)
}

type DiskMonitor interface {
	// IsDiskStalled returns true if the disk is stalled or rejecting writes.
	IsDiskStalled() bool
}

func NewDiskMonitor(ctx context.Context) DiskMonitor {
	if stalledDiskWriteDir == "" {
		return newNoopDiskMonitor()
	}

	return newPollingDiskMonitor(ctx, attemptFileWrite, stalledDiskWriteInterval, stalledDiskWriteTimeout)
}

type writeFunction func() error

func attemptFileWrite() error {
	file, err := os.Create(path.Join(stalledDiskWriteDir, ".stalled_disk_check"))
	if err != nil {
		return err
	}
	_, err = file.WriteString(strconv.FormatInt(time.Now().UnixNano(), 10))
	if err != nil {
		return err
	}
	err = file.Sync()
	if err != nil {
		return err
	}
	return file.Close()
}

type pollingDiskMonitor struct {
	stalledMutex         sync.RWMutex
	stalled              bool
	writeInProgressMutex sync.RWMutex
	writeInProgress      bool
	writeFunc            writeFunction
	pollingInterval      time.Duration
	writeTimeout         time.Duration
}

var _ DiskMonitor = &pollingDiskMonitor{}

func newPollingDiskMonitor(ctx context.Context, writeFunc writeFunction, pollingInterval, writeTimeout time.Duration) *pollingDiskMonitor {
	fs := &pollingDiskMonitor{
		stalledMutex:         sync.RWMutex{},
		stalled:              false,
		writeInProgressMutex: sync.RWMutex{},
		writeInProgress:      false,
		writeFunc:            writeFunc,
		pollingInterval:      pollingInterval,
		writeTimeout:         writeTimeout,
	}
	go fs.poll(ctx)
	return fs
}

func (fs *pollingDiskMonitor) poll(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(fs.pollingInterval):
			if fs.isWriteInProgress() {
				continue
			}

			ch := make(chan error, 1)
			go func() {
				fs.setIsWriteInProgress(true)
				err := fs.writeFunc()
				fs.setIsWriteInProgress(false)
				ch <- err
			}()

			select {
			case <-time.After(fs.writeTimeout):
				fs.setIsDiskStalled(true)
			case err := <-ch:
				fs.setIsDiskStalled(err != nil)
			}
		}
	}
}

func (fs *pollingDiskMonitor) IsDiskStalled() bool {
	fs.stalledMutex.RLock()
	defer fs.stalledMutex.RUnlock()
	return fs.stalled
}

func (fs *pollingDiskMonitor) setIsDiskStalled(isStalled bool) {
	fs.stalledMutex.Lock()
	defer fs.stalledMutex.Unlock()
	fs.stalled = isStalled
}

func (fs *pollingDiskMonitor) isWriteInProgress() bool {
	fs.writeInProgressMutex.RLock()
	defer fs.writeInProgressMutex.RUnlock()
	return fs.writeInProgress
}

func (fs *pollingDiskMonitor) setIsWriteInProgress(isInProgress bool) {
	fs.writeInProgressMutex.Lock()
	defer fs.writeInProgressMutex.Unlock()
	fs.writeInProgress = isInProgress
}

type noopDiskMonitor struct{}

var _ DiskMonitor = &noopDiskMonitor{}

func newNoopDiskMonitor() DiskMonitor {
	return &noopDiskMonitor{}
}

func (fs *noopDiskMonitor) IsDiskStalled() bool {
	return false
}
