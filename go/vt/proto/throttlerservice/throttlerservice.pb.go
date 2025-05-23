//
//Copyright 2019 The Vitess Authors.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

// gRPC RPC interface for the internal resharding throttler (go/vt/throttler)
// which is used by vreplication.

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.36.6
// 	protoc        v3.21.3
// source: throttlerservice.proto

package throttlerservice

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	unsafe "unsafe"
	throttlerdata "vitess.io/vitess/go/vt/proto/throttlerdata"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

var File_throttlerservice_proto protoreflect.FileDescriptor

const file_throttlerservice_proto_rawDesc = "" +
	"\n" +
	"\x16throttlerservice.proto\x12\x10throttlerservice\x1a\x13throttlerdata.proto2\xf3\x03\n" +
	"\tThrottler\x12M\n" +
	"\bMaxRates\x12\x1e.throttlerdata.MaxRatesRequest\x1a\x1f.throttlerdata.MaxRatesResponse\"\x00\x12S\n" +
	"\n" +
	"SetMaxRate\x12 .throttlerdata.SetMaxRateRequest\x1a!.throttlerdata.SetMaxRateResponse\"\x00\x12e\n" +
	"\x10GetConfiguration\x12&.throttlerdata.GetConfigurationRequest\x1a'.throttlerdata.GetConfigurationResponse\"\x00\x12n\n" +
	"\x13UpdateConfiguration\x12).throttlerdata.UpdateConfigurationRequest\x1a*.throttlerdata.UpdateConfigurationResponse\"\x00\x12k\n" +
	"\x12ResetConfiguration\x12(.throttlerdata.ResetConfigurationRequest\x1a).throttlerdata.ResetConfigurationResponse\"\x00B/Z-vitess.io/vitess/go/vt/proto/throttlerserviceb\x06proto3"

var file_throttlerservice_proto_goTypes = []any{
	(*throttlerdata.MaxRatesRequest)(nil),             // 0: throttlerdata.MaxRatesRequest
	(*throttlerdata.SetMaxRateRequest)(nil),           // 1: throttlerdata.SetMaxRateRequest
	(*throttlerdata.GetConfigurationRequest)(nil),     // 2: throttlerdata.GetConfigurationRequest
	(*throttlerdata.UpdateConfigurationRequest)(nil),  // 3: throttlerdata.UpdateConfigurationRequest
	(*throttlerdata.ResetConfigurationRequest)(nil),   // 4: throttlerdata.ResetConfigurationRequest
	(*throttlerdata.MaxRatesResponse)(nil),            // 5: throttlerdata.MaxRatesResponse
	(*throttlerdata.SetMaxRateResponse)(nil),          // 6: throttlerdata.SetMaxRateResponse
	(*throttlerdata.GetConfigurationResponse)(nil),    // 7: throttlerdata.GetConfigurationResponse
	(*throttlerdata.UpdateConfigurationResponse)(nil), // 8: throttlerdata.UpdateConfigurationResponse
	(*throttlerdata.ResetConfigurationResponse)(nil),  // 9: throttlerdata.ResetConfigurationResponse
}
var file_throttlerservice_proto_depIdxs = []int32{
	0, // 0: throttlerservice.Throttler.MaxRates:input_type -> throttlerdata.MaxRatesRequest
	1, // 1: throttlerservice.Throttler.SetMaxRate:input_type -> throttlerdata.SetMaxRateRequest
	2, // 2: throttlerservice.Throttler.GetConfiguration:input_type -> throttlerdata.GetConfigurationRequest
	3, // 3: throttlerservice.Throttler.UpdateConfiguration:input_type -> throttlerdata.UpdateConfigurationRequest
	4, // 4: throttlerservice.Throttler.ResetConfiguration:input_type -> throttlerdata.ResetConfigurationRequest
	5, // 5: throttlerservice.Throttler.MaxRates:output_type -> throttlerdata.MaxRatesResponse
	6, // 6: throttlerservice.Throttler.SetMaxRate:output_type -> throttlerdata.SetMaxRateResponse
	7, // 7: throttlerservice.Throttler.GetConfiguration:output_type -> throttlerdata.GetConfigurationResponse
	8, // 8: throttlerservice.Throttler.UpdateConfiguration:output_type -> throttlerdata.UpdateConfigurationResponse
	9, // 9: throttlerservice.Throttler.ResetConfiguration:output_type -> throttlerdata.ResetConfigurationResponse
	5, // [5:10] is the sub-list for method output_type
	0, // [0:5] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_throttlerservice_proto_init() }
func file_throttlerservice_proto_init() {
	if File_throttlerservice_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: unsafe.Slice(unsafe.StringData(file_throttlerservice_proto_rawDesc), len(file_throttlerservice_proto_rawDesc)),
			NumEnums:      0,
			NumMessages:   0,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_throttlerservice_proto_goTypes,
		DependencyIndexes: file_throttlerservice_proto_depIdxs,
	}.Build()
	File_throttlerservice_proto = out.File
	file_throttlerservice_proto_goTypes = nil
	file_throttlerservice_proto_depIdxs = nil
}
