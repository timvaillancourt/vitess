/*
Copyright 2024 The Vitess Authors.

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

package tabletserver

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
)

var (
	diskWriteDirs            []string
	stalledDiskWriteTimeout  = 30 * time.Second
	stalledDiskWriteInterval = 5 * time.Second
	enableDiskHealthMonitor  = false
)

func init() {
	servenv.OnParseFor("vtcombo", registerInitFlags)
	servenv.OnParseFor("vttablet", registerInitFlags)
}

func registerInitFlags(fs *pflag.FlagSet) {
	fs.StringArrayVar(&diskWriteDirs, "disk-write-dir", diskWriteDirs, "tablet will attempt to write a file to this directory to check if the disk is stalled (repeat for multiple directories); if unset and --enable-disk-health-monitor is set, directories are auto-detected from MySQL")
	fs.DurationVar(&stalledDiskWriteTimeout, "disk-write-timeout", stalledDiskWriteTimeout, "if probe writes exceed this duration, the disk is considered stalled")
	fs.DurationVar(&stalledDiskWriteInterval, "disk-write-interval", stalledDiskWriteInterval, "how often to write to the disk to check whether it is stalled")
	fs.BoolVar(&enableDiskHealthMonitor, "enable-disk-health-monitor", enableDiskHealthMonitor, "enable the disk health monitor; when enabled and --disk-write-dir is unset, the monitored directories are auto-detected from MySQL")
}

type DiskHealthMonitor interface {
	// IsDiskStalled returns true if the disk is stalled or rejecting writes.
	IsDiskStalled() bool
}

// DiskHealthMonitorConfig returns the disk health monitor flag values.
func DiskHealthMonitorConfig() (enabled bool, explicitDirs []string) {
	return enableDiskHealthMonitor, diskWriteDirs
}

// NewDiskHealthMonitor returns a monitor for the given directories, deduplicated by filesystem volume.
func NewDiskHealthMonitor(ctx context.Context, dirs []string, onStateChange func()) DiskHealthMonitor {
	dirGroups := groupDiskHealthDirs(ctx, dirs)
	if len(dirGroups) == 0 {
		return newNoopDiskHealthMonitor()
	}

	writers := make([]*dirWriter, 0, len(dirGroups))
	for _, dirs := range dirGroups {
		writers = append(writers, &dirWriter{
			dirs:    dirs,
			write:   attemptFileWrite,
			stalled: make([]bool, len(dirs)),
		})
	}
	return newPollingDiskHealthMonitor(ctx, writers, stalledDiskWriteInterval, stalledDiskWriteTimeout, onStateChange)
}

// statDeviceID returns the filesystem device ID of dir. Indirected so tests
// can stub device assignment.
var statDeviceID = func(dir string) (uint64, error) {
	var st syscall.Stat_t
	if err := syscall.Stat(dir, &st); err != nil {
		return 0, err
	}
	return uint64(st.Dev), nil //nolint:unconvert // Stat_t.Dev is int32 on darwin, uint64 on linux
}

// groupDiskHealthDirs groups directories by filesystem volume.
// Directories that cannot be statted are kept in separate groups so their probe can report the failure.
func groupDiskHealthDirs(ctx context.Context, dirs []string) [][]string {
	seenPaths := make(map[string]bool, len(dirs))
	candidates := make([]string, 0, len(dirs))
	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		dir = filepath.Clean(dir)
		if seenPaths[dir] {
			continue
		}
		seenPaths[dir] = true
		candidates = append(candidates, dir)
	}

	type statResult struct {
		index int
		dev   uint64
		err   error
	}
	results := make(chan statResult, len(candidates))
	stat := statDeviceID
	for index, dir := range candidates {
		go func() {
			dev, err := stat(dir)
			results <- statResult{index: index, dev: dev, err: err}
		}()
	}

	stats := make([]statResult, len(candidates))
	completed := make([]bool, len(candidates))
	timer := time.NewTimer(stalledDiskWriteTimeout)
	defer timer.Stop()
	remaining := len(candidates)
	for remaining > 0 {
		select {
		case result := <-results:
			stats[result.index] = result
			completed[result.index] = true
			remaining--
		case <-ctx.Done():
			remaining = 0
		case <-timer.C:
			remaining = 0
		}
	}

	seenDevices := make(map[uint64]int, len(candidates))
	groups := make([][]string, 0, len(candidates))
	for index, dir := range candidates {
		result := stats[index]
		if !completed[index] {
			result.err = ctx.Err()
			if result.err == nil {
				result.err = context.DeadlineExceeded
			}
		}
		if result.err != nil {
			log.Warn(
				"disk health monitor: could not determine device ID, keeping directory",
				slog.String("dir", dir),
				slog.Any("error", result.err),
			)
			groups = append(groups, []string{dir})
			continue
		}
		if group, ok := seenDevices[result.dev]; ok {
			log.Info(
				"disk health monitor: directory shares a volume with an already-monitored directory, grouping",
				slog.String("dir", dir),
				slog.String("monitored_dir", groups[group][0]),
			)
			groups[group] = append(groups[group], dir)
			continue
		}
		seenDevices[result.dev] = len(groups)
		groups = append(groups, []string{dir})
	}
	return groups
}

type writeFunction func() error

func attemptFileWrite(dir string) error {
	// Auto-detected paths include `tmpdir`; a unique name prevents a planted symlink from redirecting the probe.
	file, err := os.CreateTemp(dir, ".stalled_disk_check-*")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)

	if _, err := file.WriteString(strconv.FormatInt(time.Now().UnixNano(), 10)); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

// dirWriter rotates through paths on one volume and keeps each path's last result.
type dirWriter struct {
	dirs            []string
	write           func(string) error
	next            int
	writeInProgress atomic.Bool
	stalled         []bool
}

type pollingDiskHealthMonitor struct {
	stalled         atomic.Bool
	writers         []*dirWriter
	pollingInterval time.Duration
	writeTimeout    time.Duration
	onStateChange   func()
}

var _ DiskHealthMonitor = &pollingDiskHealthMonitor{}

func newPollingDiskHealthMonitor(ctx context.Context, writers []*dirWriter, pollingInterval, writeTimeout time.Duration, onStateChange func()) *pollingDiskHealthMonitor {
	m := &pollingDiskHealthMonitor{
		writers:         writers,
		pollingInterval: pollingInterval,
		writeTimeout:    writeTimeout,
		onStateChange:   onStateChange,
	}
	go m.poll(ctx)
	return m
}

func (m *pollingDiskHealthMonitor) poll(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(m.pollingInterval):
			m.checkAll(ctx)
		}
	}
}

func (m *pollingDiskHealthMonitor) checkAll(ctx context.Context) {
	var wg sync.WaitGroup
	for _, w := range m.writers {
		wg.Go(func() {
			m.checkOne(ctx, w)
		})
	}
	wg.Wait()
	if ctx.Err() != nil {
		return
	}

	stalled := false
	for _, w := range m.writers {
		for _, dirStalled := range w.stalled {
			if dirStalled {
				stalled = true
				break
			}
		}
		if stalled {
			break
		}
	}
	m.setStalled(stalled)
}

func (m *pollingDiskHealthMonitor) checkOne(ctx context.Context, w *dirWriter) {
	if ctx.Err() != nil || w.writeInProgress.Load() {
		// The previous probe hasn't returned yet; keep the last state.
		return
	}

	dirIndex := w.next
	w.next = (w.next + 1) % len(w.dirs)
	dir := w.dirs[dirIndex]
	ch := make(chan error, 1)
	w.writeInProgress.Store(true)
	go func() {
		err := w.write(dir)
		w.writeInProgress.Store(false)
		ch <- err
	}()

	var stalled bool
	select {
	case <-ctx.Done():
		return
	case <-time.After(m.writeTimeout):
		stalled = true
	case err := <-ch:
		stalled = err != nil
	}

	wasStalled := w.stalled[dirIndex]
	w.stalled[dirIndex] = stalled
	if wasStalled != stalled {
		if stalled {
			log.Error("disk health monitor: stalled disk detected", slog.String("dir", dir))
		} else {
			log.Info("disk health monitor: stalled disk recovered", slog.String("dir", dir))
		}
	}
	if stalled {
		m.setStalled(true)
	}
}

func (m *pollingDiskHealthMonitor) setStalled(stalled bool) {
	if m.stalled.Swap(stalled) != stalled && m.onStateChange != nil {
		m.onStateChange()
	}
}

func (m *pollingDiskHealthMonitor) IsDiskStalled() bool {
	return m.stalled.Load()
}

type noopDiskHealthMonitor struct{}

var _ DiskHealthMonitor = &noopDiskHealthMonitor{}

func newNoopDiskHealthMonitor() DiskHealthMonitor {
	return &noopDiskHealthMonitor{}
}

func (fs *noopDiskHealthMonitor) IsDiskStalled() bool {
	return false
}
