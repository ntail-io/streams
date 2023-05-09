// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.28.1
// 	protoc        v4.23.0
// source: v1/row.proto

package v1

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Row struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	MsgIdTime     *int64  `protobuf:"varint,1,req,name=msg_id_time,json=msgIdTime" json:"msg_id_time,omitempty"`
	MsgIdClockSeq *int32  `protobuf:"varint,2,req,name=msg_id_clock_seq,json=msgIdClockSeq" json:"msg_id_clock_seq,omitempty"`
	MsgIdNodeId   []byte  `protobuf:"bytes,3,req,name=msg_id_node_id,json=msgIdNodeId" json:"msg_id_node_id,omitempty"`
	MsgTime       *int64  `protobuf:"varint,4,req,name=msg_time,json=msgTime" json:"msg_time,omitempty"`
	Hash          *uint32 `protobuf:"varint,5,req,name=hash" json:"hash,omitempty"`
	Key           *string `protobuf:"bytes,6,req,name=key" json:"key,omitempty"`
	Data          []byte  `protobuf:"bytes,7,req,name=data" json:"data,omitempty"`
}

func (x *Row) Reset() {
	*x = Row{}
	if protoimpl.UnsafeEnabled {
		mi := &file_v1_row_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Row) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Row) ProtoMessage() {}

func (x *Row) ProtoReflect() protoreflect.Message {
	mi := &file_v1_row_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Row.ProtoReflect.Descriptor instead.
func (*Row) Descriptor() ([]byte, []int) {
	return file_v1_row_proto_rawDescGZIP(), []int{0}
}

func (x *Row) GetMsgIdTime() int64 {
	if x != nil && x.MsgIdTime != nil {
		return *x.MsgIdTime
	}
	return 0
}

func (x *Row) GetMsgIdClockSeq() int32 {
	if x != nil && x.MsgIdClockSeq != nil {
		return *x.MsgIdClockSeq
	}
	return 0
}

func (x *Row) GetMsgIdNodeId() []byte {
	if x != nil {
		return x.MsgIdNodeId
	}
	return nil
}

func (x *Row) GetMsgTime() int64 {
	if x != nil && x.MsgTime != nil {
		return *x.MsgTime
	}
	return 0
}

func (x *Row) GetHash() uint32 {
	if x != nil && x.Hash != nil {
		return *x.Hash
	}
	return 0
}

func (x *Row) GetKey() string {
	if x != nil && x.Key != nil {
		return *x.Key
	}
	return ""
}

func (x *Row) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

var File_v1_row_proto protoreflect.FileDescriptor

var file_v1_row_proto_rawDesc = []byte{
	0x0a, 0x0c, 0x76, 0x31, 0x2f, 0x72, 0x6f, 0x77, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x10,
	0x6e, 0x74, 0x61, 0x69, 0x6c, 0x2e, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x73, 0x2e, 0x76, 0x31,
	0x22, 0xc8, 0x01, 0x0a, 0x03, 0x52, 0x6f, 0x77, 0x12, 0x1e, 0x0a, 0x0b, 0x6d, 0x73, 0x67, 0x5f,
	0x69, 0x64, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x02, 0x28, 0x03, 0x52, 0x09, 0x6d,
	0x73, 0x67, 0x49, 0x64, 0x54, 0x69, 0x6d, 0x65, 0x12, 0x27, 0x0a, 0x10, 0x6d, 0x73, 0x67, 0x5f,
	0x69, 0x64, 0x5f, 0x63, 0x6c, 0x6f, 0x63, 0x6b, 0x5f, 0x73, 0x65, 0x71, 0x18, 0x02, 0x20, 0x02,
	0x28, 0x05, 0x52, 0x0d, 0x6d, 0x73, 0x67, 0x49, 0x64, 0x43, 0x6c, 0x6f, 0x63, 0x6b, 0x53, 0x65,
	0x71, 0x12, 0x23, 0x0a, 0x0e, 0x6d, 0x73, 0x67, 0x5f, 0x69, 0x64, 0x5f, 0x6e, 0x6f, 0x64, 0x65,
	0x5f, 0x69, 0x64, 0x18, 0x03, 0x20, 0x02, 0x28, 0x0c, 0x52, 0x0b, 0x6d, 0x73, 0x67, 0x49, 0x64,
	0x4e, 0x6f, 0x64, 0x65, 0x49, 0x64, 0x12, 0x19, 0x0a, 0x08, 0x6d, 0x73, 0x67, 0x5f, 0x74, 0x69,
	0x6d, 0x65, 0x18, 0x04, 0x20, 0x02, 0x28, 0x03, 0x52, 0x07, 0x6d, 0x73, 0x67, 0x54, 0x69, 0x6d,
	0x65, 0x12, 0x12, 0x0a, 0x04, 0x68, 0x61, 0x73, 0x68, 0x18, 0x05, 0x20, 0x02, 0x28, 0x0d, 0x52,
	0x04, 0x68, 0x61, 0x73, 0x68, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x06, 0x20, 0x02,
	0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x12, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18,
	0x07, 0x20, 0x02, 0x28, 0x0c, 0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x42, 0x26, 0x5a, 0x24, 0x67,
	0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x6e, 0x74, 0x61, 0x69, 0x6c, 0x2d,
	0x69, 0x6f, 0x2f, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x73, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x2f, 0x76, 0x31,
}

var (
	file_v1_row_proto_rawDescOnce sync.Once
	file_v1_row_proto_rawDescData = file_v1_row_proto_rawDesc
)

func file_v1_row_proto_rawDescGZIP() []byte {
	file_v1_row_proto_rawDescOnce.Do(func() {
		file_v1_row_proto_rawDescData = protoimpl.X.CompressGZIP(file_v1_row_proto_rawDescData)
	})
	return file_v1_row_proto_rawDescData
}

var file_v1_row_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_v1_row_proto_goTypes = []interface{}{
	(*Row)(nil), // 0: ntail.streams.v1.Row
}
var file_v1_row_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_v1_row_proto_init() }
func file_v1_row_proto_init() {
	if File_v1_row_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_v1_row_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Row); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_v1_row_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_v1_row_proto_goTypes,
		DependencyIndexes: file_v1_row_proto_depIdxs,
		MessageInfos:      file_v1_row_proto_msgTypes,
	}.Build()
	File_v1_row_proto = out.File
	file_v1_row_proto_rawDesc = nil
	file_v1_row_proto_goTypes = nil
	file_v1_row_proto_depIdxs = nil
}
