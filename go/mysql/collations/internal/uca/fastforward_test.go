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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// refEqualASCIIPrefix is the 4-byte block loop FastForward32 runs, reduced
// to the question equalASCIIPrefix answers: how many leading bytes are in
// blocks that are byte-equal and all ASCII.
func refEqualASCIIPrefix(p1, p2 []byte) int {
	i := 0
	for i+4 <= len(p1) && i+4 <= len(p2) {
		for j := range 4 {
			if p1[i+j] != p2[i+j] || p1[i+j]&0x80 != 0 || p2[i+j]&0x80 != 0 {
				return i
			}
		}
		i += 4
	}
	return i
}

func asciiRun(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = 'a' + byte(i%26)
	}
	return b
}

// prefixCases returns pairs that share an equal-ASCII prefix and then
// diverge in every way the kernel has to notice: a differing byte, a
// non-ASCII byte on one side or both, or the shorter input ending.
func prefixCases() []struct {
	name   string
	p1, p2 []byte
} {
	var cases []struct {
		name   string
		p1, p2 []byte
	}
	add := func(name string, p1, p2 []byte) {
		cases = append(cases, struct {
			name   string
			p1, p2 []byte
		}{name, p1, p2})
	}
	for _, n := range []int{0, 3, 4, 8, 15, 16, 17, 20, 31, 32, 33, 47, 48, 63, 64, 65, 100, 128, 129} {
		add(fmt.Sprintf("equal %d", n), asciiRun(n), asciiRun(n))
		add(fmt.Sprintf("equal %d vs %d", n, n+3), asciiRun(n), asciiRun(n+3))
		for pos := range n {
			p2 := asciiRun(n)
			p2[pos] = 'A' + byte(pos%26)
			add(fmt.Sprintf("differ at %d of %d", pos, n), asciiRun(n), p2)
			p2 = asciiRun(n)
			p2[pos] = 0xC3
			add(fmt.Sprintf("non-ascii right at %d of %d", pos, n), asciiRun(n), p2)
			p1 := asciiRun(n)
			p1[pos] = 0xE2
			add(fmt.Sprintf("non-ascii left at %d of %d", pos, n), p1, asciiRun(n))
			p1, p2 = asciiRun(n), asciiRun(n)
			p1[pos], p2[pos] = 0xC3, 0xC3
			add(fmt.Sprintf("non-ascii both at %d of %d", pos, n), p1, p2)
		}
	}
	return cases
}

// TestEqualASCIIPrefixInvariants holds in every build: the noasm build
// returns 0 and the simd build returns the reference count for inputs long
// enough to vectorize, so the result is one of those two, a multiple of 4,
// and never claims a prefix that is not equal ASCII.
func TestEqualASCIIPrefixInvariants(t *testing.T) {
	for _, tc := range prefixCases() {
		got := equalASCIIPrefix(tc.p1, tc.p2)
		ref := refEqualASCIIPrefix(tc.p1, tc.p2)
		require.Truef(t, got == 0 || got == ref, "%s: got %d, reference %d", tc.name, got, ref)
		require.Zerof(t, got%4, "%s: %d is not a multiple of 4", tc.name, got)
		require.LessOrEqual(t, got, len(tc.p1), tc.name)
		require.LessOrEqual(t, got, len(tc.p2), tc.name)
		for i := range got {
			require.Equalf(t, tc.p1[i], tc.p2[i], "%s: byte %d differs inside the reported prefix", tc.name, i)
			require.Zerof(t, tc.p1[i]&0x80, "%s: byte %d is non-ASCII inside the reported prefix", tc.name, i)
		}
	}
}
