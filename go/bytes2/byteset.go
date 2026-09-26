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

import "bytes"

// byteSetMax is the largest set a ByteSet can hold. It is the number of
// broadcast compares the vectorized Index does per block, so it is kept small
// enough that a block costs less than the scalar table walk it replaces.
const byteSetMax = 8

// ByteSet is a set of up to eight byte values that Index scans for. It is the
// "find the first byte that needs escaping" primitive behind the SQL literal
// encoders: the escaping loop asks where the next special byte is and copies
// the clean run in front of it in one write, instead of testing and writing
// one byte at a time.
//
// A ByteSet is built once and shared; Index is safe for concurrent use.
type ByteSet struct {
	// vals is the set padded to byteSetMax with its first member, so the
	// vectorized Index always has eight values to broadcast. Duplicates are
	// harmless there because the compares are combined with OR.
	vals [byteSetMax]byte
	// table is the membership table the scalar Index walks.
	table [256]bool
}

// NewByteSet returns the set of vals. It panics if vals is empty or has more
// than eight members.
func NewByteSet(vals ...byte) *ByteSet {
	if len(vals) == 0 || len(vals) > byteSetMax {
		panic("bytes2.NewByteSet: a set needs between 1 and 8 members")
	}
	s := &ByteSet{}
	for i := range s.vals {
		s.vals[i] = vals[0]
	}
	for i, v := range vals {
		s.vals[i] = v
		s.table[v] = true
	}
	return s
}

// indexScalar is the reference Index: a table lookup per byte.
func (s *ByteSet) indexScalar(b []byte) int {
	for i, c := range b {
		if s.table[c] {
			return i
		}
	}
	return -1
}

// indexAny2Scalar is the reference IndexAny2. It leans on bytes.IndexByte,
// which is vectorized in the standard library on every architecture Vitess
// builds for, so the fallback is two bounded SIMD scans rather than a byte
// loop: the second scan only covers the bytes in front of the first hit.
func indexAny2Scalar(b []byte, a, c byte) int {
	i := bytes.IndexByte(b, a)
	if i < 0 {
		return bytes.IndexByte(b, c)
	}
	if j := bytes.IndexByte(b[:i], c); j >= 0 {
		return j
	}
	return i
}
