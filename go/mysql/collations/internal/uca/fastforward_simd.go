//go:build simd && goexperiment.simd && (amd64 || arm64)

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
	"math"
	"simd"

	"vitess.io/vitess/go/bytes2"
)

// simdThreshold is the shortest input the vectorized prefix skip is used
// for. The 4-byte block loop it replaces already runs at several GB/s, so
// one 16-byte vector block does not pay for its broadcasts and mask
// extraction: measured on arm64, 16-byte inputs got 11-16% slower at a
// threshold of 16 and 64-byte inputs 4% faster. init raises the threshold
// past any length when the simd package is emulating vectors in software.
var simdThreshold = 32

func init() {
	if simd.Emulated() {
		simdThreshold = math.MaxInt
	}
}

// equalASCIIPrefix returns the length, a multiple of 4, of the leading run
// of 4-byte blocks that are byte-equal in p1 and p2 and all ASCII. Those are
// the blocks FastForward32 skips without a weight lookup, so the caller can
// advance past them in one step and leave the first block that differs, or
// carries a non-ASCII byte, to the scalar loop that knows how to weigh it.
//
// A block that differs in bytes but has equal weights (a case-insensitive
// match, say) stops the skip; the scalar loop resolves it and carries on, so
// the result is the same as before, only slower for that input.
//
// The vector body lives in equalASCIIPrefixSIMD so this wrapper stays small
// enough to inline into FastForward32: without the split, every call on a
// short input paid for a function call just to learn it was short.
func equalASCIIPrefix(p1, p2 []byte) int {
	if min(len(p1), len(p2)) < simdThreshold {
		return 0
	}
	return equalASCIIPrefixSIMD(p1, p2)
}

func equalASCIIPrefixSIMD(p1, p2 []byte) int {
	n := min(len(p1), len(p2))
	// The lane count comes from the zero vector so the "shorter than one
	// vector" check runs before the broadcast: on the wider amd64 lanes the
	// threshold lets 32..w-1 byte inputs through, and the overlapping tail
	// below needs at least one full block.
	var zero simd.Uint8s
	w := zero.Len()
	if n < w {
		return 0
	}
	hi := simd.BroadcastUint8s(0x80)
	var tmp bytes2.LaneBuf

	i := 0
	for ; i+w <= n; i += w {
		v1 := simd.LoadUint8s(p1[i : i+w])
		v2 := simd.LoadUint8s(p2[i : i+w])
		flag := v1.Or(v2).And(hi).NotEqual(zero).Or(v1.NotEqual(v2))
		if k := bytes2.FirstLane(flag, w, &tmp); k >= 0 {
			return (i + k) &^ 3
		}
	}
	if i < n {
		// Read the tail as an overlapping full block; bytes the loop cleared
		// cannot flag, and avoiding a partial load also avoids spilling the
		// vectors. With no flag the scalar loop's 4-byte bound applies.
		start := n - w
		v1 := simd.LoadUint8s(p1[start:n])
		v2 := simd.LoadUint8s(p2[start:n])
		flag := v1.Or(v2).And(hi).NotEqual(zero).Or(v1.NotEqual(v2))
		if k := bytes2.FirstLane(flag, w, &tmp); k >= 0 {
			return (start + k) &^ 3
		}
		return n &^ 3
	}
	return i
}
