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

package sqltypes

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func BenchmarkEncodeSQLStringBuilder(b *testing.B) {
	type input struct {
		name string
		data string
	}
	var inputs []input
	for _, size := range []int{0, 1, 16, 64, 256, 4096} {
		for _, pattern := range []input{
			{name: "plain", data: "a"},
			{name: "sparse", data: strings.Repeat("a", 63) + "'"},
			{name: "medium", data: strings.Repeat("a", 7) + "'"},
			{name: "dense", data: "'\\\x00\n"},
		} {
			if (size == 0 && pattern.name != "plain") ||
				((pattern.name == "sparse" || pattern.name == "medium") && size < len(pattern.data)) {
				continue
			}
			inputs = append(inputs, input{
				name: fmt.Sprintf("%s/bytes=%d", pattern.name, size),
				data: strings.Repeat(pattern.data, size/len(pattern.data)+1)[:size],
			})
		}
	}
	inputs = append(inputs,
		input{name: "utf8", data: strings.Repeat("é漢💾", 32)},
		input{name: "arbitrary_bytes", data: strings.Repeat("\xff\x80\x00a", 64)},
		input{name: "like_escapes", data: strings.Repeat(`prefix\%middle\_suffix`, 16)},
		input{name: "all_escapes", data: "\x00'\"\b\n\r\t\x1a\\"},
	)
	for _, in := range inputs {
		for _, typ := range []Type{VarChar, VarBinary} {
			for _, reserve := range []bool{false, true} {
				b.Run(fmt.Sprintf("%s/%s/reserve=%t", in.name, typ, reserve), func(b *testing.B) {
					value := MakeTrusted(typ, []byte(in.data))
					var reference bytes.Buffer
					// EncodeSQL uses the independent bytes2 encoder for these types.
					value.EncodeSQL(&reference)
					want := reference.String()
					var got string
					b.SetBytes(int64(len(in.data)))
					b.ReportAllocs()
					for b.Loop() {
						var buf strings.Builder
						if reserve {
							buf.Grow(len(want))
						}
						value.EncodeSQLStringBuilder(&buf)
						got = buf.String()
					}
					require.Equal(b, want, got)
				})
			}
		}
	}
}
