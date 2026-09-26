//go:build goexperiment.simd && (amd64 || arm64)

/*
Copyright 2026 The Vitess Authors.

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

package uca

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEqualASCIIPrefixMatchesReference pins the vectorized kernel to the
// scalar block loop for every input long enough to take the vector path.
func TestEqualASCIIPrefixMatchesReference(t *testing.T) {
	for _, tc := range prefixCases() {
		if min(len(tc.p1), len(tc.p2)) < simdThreshold {
			continue
		}
		require.Equalf(t, refEqualASCIIPrefix(tc.p1, tc.p2), equalASCIIPrefix(tc.p1, tc.p2), tc.name)
	}
}

func FuzzEqualASCIIPrefix(f *testing.F) {
	f.Add(asciiRun(64), asciiRun(64))
	f.Add(asciiRun(33), append(asciiRun(32), 0xC3))
	f.Add(asciiRun(20), asciiRun(17))
	f.Fuzz(func(t *testing.T, p1, p2 []byte) {
		got := equalASCIIPrefix(p1, p2)
		ref := refEqualASCIIPrefix(p1, p2)
		if min(len(p1), len(p2)) < simdThreshold {
			if got != 0 {
				t.Fatalf("short input took the vector path: got %d", got)
			}
			return
		}
		if got != ref {
			t.Fatalf("equalASCIIPrefix(%q, %q) = %d, want %d", p1, p2, got, ref)
		}
	})
}
