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
	"errors"
	"fmt"
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
	fs.DurationVar(&stalledDiskWriteTimeout, "disk-write-timeout", stalledDiskWriteTimeout, "if a probe write or a startup directory stat exceeds this duration, the disk is considered stalled; an explicitly configured --disk-write-dir that is already stalled at startup fails vttablet startup")
	fs.DurationVar(&stalledDiskWriteInterval, "disk-write-interval", stalledDiskWriteInterval, "how often to write to the disk to check whether it is stalled")
	fs.BoolVar(&enableDiskHealthMonitor, "enable-disk-health-monitor", enableDiskHealthMonitor, "enable the disk health monitor; when enabled and --disk-write-dir is unset, the monitored directories are auto-detected from MySQL")
}

type DiskHealthMonitor interface {
	// IsDiskStalled returns true if the disk is stalled or rejecting writes.
	IsDiskStalled() bool
}

// DiskHealthMonitorEnabled reports whether --enable-disk-health-monitor is set.
func DiskHealthMonitorEnabled() bool {
	return enableDiskHealthMonitor
}

// DiskHealthMonitorExplicitDirs returns the explicitly configured
// --disk-write-dir values, if any.
func DiskHealthMonitorExplicitDirs() []string {
	return diskWriteDirs
}

// NewDiskHealthMonitor returns a monitor that polls the given directories,
// reporting a stalled disk when a probe write to any of them times out or
// fails. Directories sharing a filesystem volume are deduplicated to a
// single probe. An empty dirs slice returns a no-op monitor. An error is
// returned when a directory's stat(2) does not return within
// --disk-write-timeout — the filesystem already appears stalled — so callers
// can fail startup instead of hanging.
func NewDiskHealthMonitor(ctx context.Context, dirs []string) (DiskHealthMonitor, error) {
	dirs, err := dedupeDiskHealthDirs(dirs)
	if err != nil {
		return nil, err
	}
	if len(dirs) == 0 {
		return newNoopDiskHealthMonitor(), nil
	}

	writers := make([]*dirWriter, 0, len(dirs))
	for _, dir := range dirs {
		writers = append(writers, &dirWriter{dir: dir, write: func() error { return attemptFileWrite(dir) }})
	}
	return newPollingDiskHealthMonitor(ctx, writers, stalledDiskWriteInterval, stalledDiskWriteTimeout), nil
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

// ErrDiskStatTimedOut signals stat(2) of a monitored directory did not return
// within --disk-write-timeout: the filesystem itself appears stalled.
var ErrDiskStatTimedOut = errors.New("stat timed out, filesystem may be stalled")

// statDeviceIDWithTimeout bounds statDeviceID: a stat(2) on a hung filesystem
// can block indefinitely, which must not hang the caller.
func statDeviceIDWithTimeout(dir string, timeout time.Duration) (uint64, error) {
	type statResult struct {
		dev uint64
		err error
	}
	ch := make(chan statResult, 1)
	// Snapshot statDeviceID: on the timeout path this goroutine is abandoned
	// and may outlive the caller, so it must not read a package var that a
	// test (or future caller) could reassign concurrently.
	stat := statDeviceID
	go func() {
		dev, err := stat(dir)
		ch <- statResult{dev: dev, err: err}
	}()

	select {
	case r := <-ch:
		return r.dev, r.err
	case <-time.After(timeout):
		// fmt.Errorf with %w rather than vterrors.Wrapf: the sentinel must
		// stay reachable via errors.Is, and vterrors wrapping does not
		// implement Unwrap.
		return 0, fmt.Errorf("stat of %s exceeded %v: %w", dir, timeout, ErrDiskStatTimedOut)
	}
}

// dedupeDiskHealthDirs returns dirs with at most one entry per underlying
// filesystem volume, using the stat(2) device ID. Deduplication is an
// optimization: a dir whose device ID cannot be determined is kept so the
// probe write itself can surface the problem. Empty entries are dropped.
// A stat that times out is a hard error: the filesystem is already stalled.
func dedupeDiskHealthDirs(dirs []string) ([]string, error) {
	seen := make(map[uint64]string, len(dirs))
	seenPaths := make(map[string]bool, len(dirs))
	unique := make([]string, 0, len(dirs))
	skipped := make(map[string][]string, len(dirs))
	for _, dir := range dirs {
		if dir == "" {
			continue
		}
		// Several MySQL variables commonly name the same directory; drop
		// exact duplicates silently — only distinct paths sharing a volume
		// are worth logging below.
		dir = filepath.Clean(dir)
		if seenPaths[dir] {
			continue
		}
		seenPaths[dir] = true
		dev, err := statDeviceIDWithTimeout(dir, stalledDiskWriteTimeout)
		switch {
		case errors.Is(err, ErrDiskStatTimedOut):
			return nil, err
		case err != nil:
			log.Warn(
				"disk health monitor: could not determine device ID, keeping directory",
				slog.String("dir", dir),
				slog.Any("error", err),
			)
			unique = append(unique, dir)
			continue
		}
		if monitoredDir, ok := seen[dev]; ok {
			skipped[monitoredDir] = append(skipped[monitoredDir], dir)
			continue
		}
		seen[dev] = dir
		unique = append(unique, dir)
	}
	for _, monitoredDir := range unique {
		if len(skipped[monitoredDir]) == 0 {
			continue
		}
		log.Info(
			"disk health monitor: monitoring one directory for a volume, skipping others sharing it",
			slog.String("monitored_dir", monitoredDir),
			slog.Any("skipped_dirs", skipped[monitoredDir]),
		)
	}
	return unique, nil
}

type writeFunction func() error

func attemptFileWrite(dir string) error {
	// Use a unique per-probe filename via os.CreateTemp (O_CREATE|O_EXCL with a
	// random suffix), not a fixed name opened with os.Create. Auto-detected
	// directories include the world-writable tmpdir, where a local user could
	// pre-plant a predictable probe name as a symlink; os.Create follows it and
	// either clobbers the target or fails the probe (a false stall). A random
	// name an attacker cannot pre-create closes that vector.
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

// dirWriter probes a single directory. stalled holds the result of the last
// completed probe; it is left untouched while a slow probe is still in
// flight, so a stall keeps being reported until a probe succeeds again.
type dirWriter struct {
	dir             string
	write           writeFunction
	writeInProgress atomic.Bool
	stalled         atomic.Bool
}

type pollingDiskHealthMonitor struct {
	stalled         atomic.Bool
	writers         []*dirWriter
	pollingInterval time.Duration
	writeTimeout    time.Duration
}

var _ DiskHealthMonitor = &pollingDiskHealthMonitor{}

func newPollingDiskHealthMonitor(ctx context.Context, writers []*dirWriter, pollingInterval, writeTimeout time.Duration) *pollingDiskHealthMonitor {
	m := &pollingDiskHealthMonitor{
		writers:         writers,
		pollingInterval: pollingInterval,
		writeTimeout:    writeTimeout,
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
			m.checkAll()
		}
	}
}

// checkAll probes all directories concurrently: the disk is stalled if any
// directory is stalled. checkOne publishes a stall as soon as its own probe
// fails, so a fast failure is not masked by a slow sibling probe still in
// flight. The post-wait pass is authoritative for recovery — it clears the
// stalled flag only once every directory's latest probe has succeeded.
func (m *pollingDiskHealthMonitor) checkAll() {
	var wg sync.WaitGroup
	for _, w := range m.writers {
		wg.Go(func() {
			m.checkOne(w)
		})
	}
	wg.Wait()

	stalled := false
	for _, w := range m.writers {
		if w.stalled.Load() {
			stalled = true
			break
		}
	}
	m.stalled.Store(stalled)
}

func (m *pollingDiskHealthMonitor) checkOne(w *dirWriter) {
	if w.writeInProgress.Load() {
		// The previous probe hasn't returned yet; keep the last state.
		return
	}

	ch := make(chan error, 1)
	w.writeInProgress.Store(true)
	go func() {
		err := w.write()
		w.writeInProgress.Store(false)
		ch <- err
	}()

	var stalled bool
	select {
	case <-time.After(m.writeTimeout):
		stalled = true
	case err := <-ch:
		stalled = err != nil
	}

	wasStalled := w.stalled.Swap(stalled)
	if wasStalled != stalled {
		if stalled {
			log.Error("disk health monitor: stalled disk detected", slog.String("dir", w.dir))
		} else {
			log.Info("disk health monitor: stalled disk recovered", slog.String("dir", w.dir))
		}
	}
	if stalled {
		// Publish immediately so a fast probe failure is visible without
		// waiting for slower sibling probes in the same tick to finish;
		// checkAll's post-wait pass clears the flag once all recover.
		m.stalled.Store(true)
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
