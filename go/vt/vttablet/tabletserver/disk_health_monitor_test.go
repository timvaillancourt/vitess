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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testWriters wraps write functions into dirWriters for the polling monitor.
func testWriters(fns ...writeFunction) []*dirWriter {
	writers := make([]*dirWriter, 0, len(fns))
	for i, fn := range fns {
		writers = append(writers, &dirWriter{dir: fmt.Sprintf("dir-%d", i), write: fn})
	}
	return writers
}

func TestDiskHealthMonitor_noStall(t *testing.T) {
	ctx := t.Context()
	mockFileWriter := &sequencedMockWriter{}
	diskHealthMonitor := newPollingDiskHealthMonitor(ctx, testWriters(mockFileWriter.mockWriteFunction), 50*time.Millisecond, 25*time.Millisecond)

	time.Sleep(300 * time.Millisecond)
	totalCreateCalls := mockFileWriter.getTotalCreateCalls()
	require.GreaterOrEqualf(t, totalCreateCalls, 4, "expected at least 4 calls to createFile, got %d", totalCreateCalls)
	require.False(t, diskHealthMonitor.IsDiskStalled(), "expected isStalled to be false")
}

func TestDiskHealthMonitor_stallAndRecover(t *testing.T) {
	ctx := t.Context()
	mockFileWriter := &sequencedMockWriter{sequencedWriteFunctions: []writeFunction{delayedWriteFunction(10*time.Millisecond, nil), delayedWriteFunction(300*time.Millisecond, nil)}}
	diskHealthMonitor := newPollingDiskHealthMonitor(ctx, testWriters(mockFileWriter.mockWriteFunction), 50*time.Millisecond, 25*time.Millisecond)

	time.Sleep(300 * time.Millisecond)
	totalCreateCalls := mockFileWriter.getTotalCreateCalls()
	require.Equalf(t, 2, totalCreateCalls, "expected 2 calls to createFile, got %d", totalCreateCalls)
	require.True(t, diskHealthMonitor.IsDiskStalled(), "expected isStalled to be true")

	time.Sleep(300 * time.Millisecond)
	totalCreateCalls = mockFileWriter.getTotalCreateCalls()
	require.GreaterOrEqualf(t, totalCreateCalls, 5, "expected at least 5 calls to createFile, got %d", totalCreateCalls)
	require.False(t, diskHealthMonitor.IsDiskStalled(), "expected isStalled to be false")
}

func TestDiskHealthMonitor_stallDetected(t *testing.T) {
	ctx := t.Context()
	mockFileWriter := &sequencedMockWriter{defaultWriteFunction: delayedWriteFunction(10*time.Millisecond, errors.New("test error"))}
	diskHealthMonitor := newPollingDiskHealthMonitor(ctx, testWriters(mockFileWriter.mockWriteFunction), 50*time.Millisecond, 25*time.Millisecond)

	time.Sleep(300 * time.Millisecond)
	totalCreateCalls := mockFileWriter.getTotalCreateCalls()
	require.GreaterOrEqualf(t, totalCreateCalls, 4, "expected at least 4 calls to createFile, got %d", totalCreateCalls)
	require.True(t, diskHealthMonitor.IsDiskStalled(), "expected isStalled to be true")
}

// TestDiskHealthMonitor_multiDirAnyStalled proves the disk is reported
// stalled when only one of several monitored directories fails its probes.
func TestDiskHealthMonitor_multiDirAnyStalled(t *testing.T) {
	ctx := t.Context()
	healthyWriter := &sequencedMockWriter{}
	failingWriter := &sequencedMockWriter{defaultWriteFunction: delayedWriteFunction(10*time.Millisecond, errors.New("test error"))}
	diskHealthMonitor := newPollingDiskHealthMonitor(ctx, testWriters(healthyWriter.mockWriteFunction, failingWriter.mockWriteFunction), 50*time.Millisecond, 25*time.Millisecond)

	assert.Eventually(t, diskHealthMonitor.IsDiskStalled, 30*time.Second, 10*time.Millisecond, "expected isStalled to be true with one failing dir")
	assert.Positive(t, healthyWriter.getTotalCreateCalls(), "expected probes of the healthy dir")
}

// TestDiskHealthMonitor_multiDirRecovery proves the disk recovers once the
// only failing directory starts succeeding again.
func TestDiskHealthMonitor_multiDirRecovery(t *testing.T) {
	ctx := t.Context()
	healthyWriter := &sequencedMockWriter{}
	flakyWriter := &sequencedMockWriter{sequencedWriteFunctions: []writeFunction{
		delayedWriteFunction(10*time.Millisecond, errors.New("test error")),
		delayedWriteFunction(10*time.Millisecond, errors.New("test error")),
	}}
	diskHealthMonitor := newPollingDiskHealthMonitor(ctx, testWriters(healthyWriter.mockWriteFunction, flakyWriter.mockWriteFunction), 50*time.Millisecond, 25*time.Millisecond)

	assert.Eventually(t, diskHealthMonitor.IsDiskStalled, 30*time.Second, 10*time.Millisecond, "expected isStalled to be true while probes fail")
	assert.Eventually(t, func() bool { return !diskHealthMonitor.IsDiskStalled() }, 30*time.Second, 10*time.Millisecond, "expected isStalled to be false after probes recover")
}

// TestDiskHealthMonitor_fastFailureNotMaskedBySlowProbe proves a probe that
// fails immediately marks the disk stalled without waiting for a slow sibling
// probe in the same tick to finish, so the stall is visible promptly.
func TestDiskHealthMonitor_fastFailureNotMaskedBySlowProbe(t *testing.T) {
	failing := func() error { return errors.New("write rejected") }
	blocked := make(chan struct{})
	t.Cleanup(func() { close(blocked) })
	slow := func() error {
		<-blocked
		return nil
	}
	m := &pollingDiskHealthMonitor{
		writers:      testWriters(failing, slow),
		writeTimeout: 30 * time.Second,
	}

	go m.checkAll()
	assert.Eventually(t, m.IsDiskStalled, 30*time.Second, 5*time.Millisecond, "a fast probe failure must be visible without waiting for a slow sibling probe")
}

// TestAttemptFileWrite_unaffectedByPlantedSymlink proves the probe uses a
// unique filename, so a symlink pre-planted at the predictable probe name in a
// world-writable directory (e.g. an auto-detected tmpdir) cannot redirect or
// fail the write. It also confirms the probe removes its own file.
func TestAttemptFileWrite_unaffectedByPlantedSymlink(t *testing.T) {
	dir := t.TempDir()
	// A local user plants the fixed probe name as a symlink to an uncreatable
	// target; the old os.Create(fixed-name) probe would follow it and fail.
	planted := filepath.Join(dir, ".stalled_disk_check")
	require.NoError(t, os.Symlink(filepath.Join(dir, "nope", "target"), planted))

	require.NoError(t, attemptFileWrite(dir), "probe must be unaffected by a planted symlink")

	// The probe cleans up its unique temp file, leaving only the planted link.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1, "probe must not leave temp files behind")
	assert.Equal(t, ".stalled_disk_check", entries[0].Name())
}

func TestNewDiskHealthMonitor_noDirs(t *testing.T) {
	monitor, err := NewDiskHealthMonitor(t.Context(), nil)
	require.NoError(t, err)
	require.IsType(t, &noopDiskHealthMonitor{}, monitor)
	require.False(t, monitor.IsDiskStalled())

	monitor, err = NewDiskHealthMonitor(t.Context(), []string{""})
	require.NoError(t, err)
	require.IsType(t, &noopDiskHealthMonitor{}, monitor)
}

// TestNewDiskHealthMonitor_statTimeout proves a stat(2) blocking on an
// already-stalled filesystem surfaces as an error instead of hanging the
// caller, so vttablet startup can fail instead of blocking.
func TestNewDiskHealthMonitor_statTimeout(t *testing.T) {
	origStatDeviceID := statDeviceID
	origWriteTimeout := stalledDiskWriteTimeout
	t.Cleanup(func() {
		statDeviceID = origStatDeviceID
		stalledDiskWriteTimeout = origWriteTimeout
	})
	stalledDiskWriteTimeout = 25 * time.Millisecond
	statDeviceID = func(dir string) (uint64, error) {
		time.Sleep(10 * time.Second)
		return 0, nil
	}

	done := make(chan error, 1)
	go func() {
		_, err := NewDiskHealthMonitor(t.Context(), []string{"/hung"})
		done <- err
	}()
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrDiskStatTimedOut)
	case <-time.After(30 * time.Second):
		require.FailNow(t, "NewDiskHealthMonitor did not return, stat timeout is not enforced")
	}
}

func TestDedupeDiskHealthDirs(t *testing.T) {
	origStatDeviceID := statDeviceID
	t.Cleanup(func() { statDeviceID = origStatDeviceID })

	devices := map[string]uint64{
		"/data":      1,
		"/data/tmp":  1,
		"/other":     2,
		"/other/sub": 2,
	}
	statDeviceID = func(dir string) (uint64, error) {
		dev, ok := devices[dir]
		if !ok {
			return 0, errors.New("stat failed")
		}
		return dev, nil
	}

	tests := []struct {
		name string
		dirs []string
		want []string
	}{
		{
			name: "dedupes dirs sharing a volume",
			dirs: []string{"/data", "/data/tmp", "/other", "/other/sub"},
			want: []string{"/data", "/other"},
		},
		{
			name: "drops empty entries",
			dirs: []string{"", "/data", ""},
			want: []string{"/data"},
		},
		{
			name: "drops exact duplicate paths",
			dirs: []string{"/data", "/data", "/data/", "/other"},
			want: []string{"/data", "/other"},
		},
		{
			name: "keeps dirs whose device ID cannot be determined",
			dirs: []string{"/data", "/missing"},
			want: []string{"/data", "/missing"},
		},
		{
			name: "nil input",
			dirs: nil,
			want: []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := dedupeDiskHealthDirs(tt.dirs)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

type sequencedMockWriter struct {
	defaultWriteFunction    writeFunction
	sequencedWriteFunctions []writeFunction

	totalCreateCalls      int
	totalCreateCallsMutex sync.RWMutex
}

func (smw *sequencedMockWriter) mockWriteFunction() error {
	functionIndex := smw.getTotalCreateCalls()
	smw.incrementTotalCreateCalls()

	if functionIndex >= len(smw.sequencedWriteFunctions) {
		if smw.defaultWriteFunction != nil {
			return smw.defaultWriteFunction()
		}
		return delayedWriteFunction(10*time.Millisecond, nil)()
	}

	return smw.sequencedWriteFunctions[functionIndex]()
}

func (smw *sequencedMockWriter) incrementTotalCreateCalls() {
	smw.totalCreateCallsMutex.Lock()
	defer smw.totalCreateCallsMutex.Unlock()
	smw.totalCreateCalls += 1
}

func (smw *sequencedMockWriter) getTotalCreateCalls() int {
	smw.totalCreateCallsMutex.RLock()
	defer smw.totalCreateCallsMutex.RUnlock()
	return smw.totalCreateCalls
}

func delayedWriteFunction(delay time.Duration, err error) writeFunction {
	return func() error {
		time.Sleep(delay)
		return err
	}
}
