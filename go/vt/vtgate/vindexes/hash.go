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

package vindexes

import (
	"bytes"
	"context"
	"crypto/cipher"
	"crypto/des"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

var (
	_ SingleColumn    = (*Hash)(nil)
	_ Reversible      = (*Hash)(nil)
	_ Hashing         = (*Hash)(nil)
	_ ParamValidating = (*Hash)(nil)
)

// Hash defines vindex that hashes an int64 to a KeyspaceId
// by using null-key DES hash. It's Unique, Reversible and
// Functional.
// Note that at once stage we used a 3DES-based hash here,
// but for a null key as in our case, they are completely equivalent.
type Hash struct {
	name          string
	unknownParams []string
}

// newHash creates a new Hash.
func newHash(name string, params map[string]string) (Vindex, error) {
	return &Hash{
		name:          name,
		unknownParams: FindUnknownParams(params, nil),
	}, nil
}

// String returns the name of the vindex.
func (vind *Hash) String() string {
	return vind.name
}

// Cost returns the cost of this index as 1.
func (vind *Hash) Cost() int {
	return 1
}

// IsUnique returns true since the Vindex is unique.
func (vind *Hash) IsUnique() bool {
	return true
}

// NeedsVCursor satisfies the Vindex interface.
func (vind *Hash) NeedsVCursor() bool {
	return false
}

// Map can map ids to key.ShardDestination objects.
func (vind *Hash) Map(ctx context.Context, vcursor VCursor, ids []sqltypes.Value) ([]key.ShardDestination, error) {
	out := make([]key.ShardDestination, len(ids))
	for i, id := range ids {
		ksid, err := vind.Hash(id)
		if err != nil {
			out[i] = key.DestinationNone{}
			continue
		}
		out[i] = key.DestinationKeyspaceID(ksid)
	}
	return out, nil
}

// Verify returns true if ids maps to ksids.
func (vind *Hash) Verify(ctx context.Context, vcursor VCursor, ids []sqltypes.Value, ksids [][]byte) ([]bool, error) {
	out := make([]bool, len(ids))
	for i := range ids {
		num, err := ids[i].ToCastUint64()
		if err != nil {
			return nil, err
		}
		out[i] = bytes.Equal(vhash(num), ksids[i])
	}
	return out, nil
}

// ReverseMap returns the ids from ksids.
func (vind *Hash) ReverseMap(_ VCursor, ksids [][]byte) ([]sqltypes.Value, error) {
	reverseIds := make([]sqltypes.Value, 0, len(ksids))
	for _, keyspaceID := range ksids {
		val, err := vunhash(keyspaceID)
		if err != nil {
			return reverseIds, err
		}
		reverseIds = append(reverseIds, sqltypes.NewUint64(val))
	}
	return reverseIds, nil
}

func (vind *Hash) Hash(id sqltypes.Value) ([]byte, error) {
	var num uint64
	var err error

	if id.IsSigned() {
		// This is ToUint64 with no check on negative values.
		str := id.ToString()
		var ival int64
		ival, err = strconv.ParseInt(str, 10, 64)
		num = uint64(ival)
	} else {
		num, err = id.ToCastUint64()
	}

	if err != nil {
		return nil, err
	}
	return vhash(num), nil
}

// UnknownParams implements the ParamValidating interface.
func (vind *Hash) UnknownParams() []string {
	return vind.unknownParams
}

var blockDES cipher.Block

func init() {
	var err error
	blockDES, err = des.NewCipher(make([]byte, 8))
	if err != nil {
		panic(err)
	}
	Register("hash", newHash)
}

func vhash(shardKey uint64) []byte {
	var keybytes, hashed [8]byte
	binary.BigEndian.PutUint64(keybytes[:], shardKey)
	blockDES.Encrypt(hashed[:], keybytes[:])
	return hashed[:]
}

func vunhash(k []byte) (uint64, error) {
	if len(k) != 8 {
		return 0, fmt.Errorf("invalid keyspace id: %v", hex.EncodeToString(k))
	}
	var unhashed [8]byte
	blockDES.Decrypt(unhashed[:], k)
	return binary.BigEndian.Uint64(unhashed[:]), nil
}
