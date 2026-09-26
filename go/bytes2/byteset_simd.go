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

package bytes2

import (
	"math"
	"math/bits"
	"simd"
)

// simdThreshold is the input length below which the scalar path wins: the
// broadcasts and the mask extraction cost more than a table walk over a few
// bytes. It is a single comparison rather than a separate "is SIMD usable"
// flag so the exported wrappers stay under the inlining budget; init raises
// it past any length when the simd package is emulating vectors in
// software, where the scalar paths are always faster.
var simdThreshold = 16

func init() {
	if simd.Emulated() {
		simdThreshold = math.MaxInt
	}
}

// laneBuf holds a stored byte mask: 64 bytes covers the widest vector. The
// caller owns one per call rather than per block, since Store overwrites the
// words that are read and zeroing 64 bytes per block is measurable.
type laneBuf [8]uint64

// firstLane returns the index of the first true lane of m, given n lanes, or
// -1 if none is set. The simd package has no movemask, so the mask is stored
// as 0xFF-per-true-lane bytes and scanned a word at a time.
func firstLane(m simd.Mask8s, n int, tmp *laneBuf) int {
	m.ToInt8s().ToBits().ReshapeToUint64s().Store(tmp[:])
	for j := 0; j < n/8; j++ {
		if w := tmp[j]; w != 0 {
			return j*8 + bits.TrailingZeros64(w)/8
		}
	}
	return -1
}

// match8 reports the lanes of x equal to any of the eight broadcast set
// members. It is a plain function with vector parameters rather than a
// closure over the members: the compiler cannot clone closures or methods
// that hold vector values for the experiment (go1.27.1 fails with an
// internal error, "missing Types entry"), and a plain function inlines.
func match8(x, v0, v1, v2, v3, v4, v5, v6, v7 simd.Uint8s) simd.Mask8s {
	return x.Equal(v0).Or(x.Equal(v1)).Or(x.Equal(v2)).Or(x.Equal(v3)).
		Or(x.Equal(v4)).Or(x.Equal(v5)).Or(x.Equal(v6)).Or(x.Equal(v7))
}

// Index returns the index of the first byte of b that is in s, or -1 if none
// is.
//
// The vector work lives in indexSIMD, a plain function, because the go1.27.1
// compiler fails with an internal error when a method with a receiver uses
// simd vector values directly (see match8). A method that only calls such a
// function is fine.
func (s *ByteSet) Index(b []byte) int {
	if len(b) < simdThreshold {
		return s.indexScalar(b)
	}
	return indexSIMD(s, b)
}

func indexSIMD(s *ByteSet, b []byte) int {
	v0 := simd.BroadcastUint8s(s.vals[0])
	v1 := simd.BroadcastUint8s(s.vals[1])
	v2 := simd.BroadcastUint8s(s.vals[2])
	v3 := simd.BroadcastUint8s(s.vals[3])
	v4 := simd.BroadcastUint8s(s.vals[4])
	v5 := simd.BroadcastUint8s(s.vals[5])
	v6 := simd.BroadcastUint8s(s.vals[6])
	v7 := simd.BroadcastUint8s(s.vals[7])
	n := v0.Len()
	var tmp laneBuf

	i := 0
	for ; i+n <= len(b); i += n {
		x := simd.LoadUint8s(b[i : i+n])
		if k := firstLane(match8(x, v0, v1, v2, v3, v4, v5, v6, v7), n, &tmp); k >= 0 {
			return i + k
		}
	}
	if i < len(b) {
		// The partial load zero-fills the lanes past the input, and 0x00 may
		// be a member of the set, so a hit in a fill lane is not a hit.
		x, got := simd.LoadUint8sPart(b[i:])
		if k := firstLane(match8(x, v0, v1, v2, v3, v4, v5, v6, v7), n, &tmp); k >= 0 && k < got {
			return i + k
		}
	}
	return -1
}
