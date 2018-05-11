// Code generated by protoc-gen-go. DO NOT EDIT.
// source: kv.proto

/*
Package kvp is a generated protocol buffer package.

It is generated from these files:
	kv.proto

It has these top-level messages:
	Kv
*/
package multiraft

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion2 // please upgrade the proto package

type Kv struct {
	Opt uint32 `protobuf:"varint,1,opt,name=opt" json:"opt,omitempty"`
	K   string `protobuf:"bytes,2,opt,name=k" json:"k,omitempty"`
	V   []byte `protobuf:"bytes,3,opt,name=v,proto3" json:"v,omitempty"`
}

func (m *Kv) Reset()                    { *m = Kv{} }
func (m *Kv) String() string            { return proto.CompactTextString(m) }
func (*Kv) ProtoMessage()               {}
func (*Kv) Descriptor() ([]byte, []int) { return fileDescriptor0, []int{0} }

func (m *Kv) GetOpt() uint32 {
	if m != nil {
		return m.Opt
	}
	return 0
}

func (m *Kv) GetK() string {
	if m != nil {
		return m.K
	}
	return ""
}

func (m *Kv) GetV() []byte {
	if m != nil {
		return m.V
	}
	return nil
}

func init() {
	proto.RegisterType((*Kv)(nil), "kvp.kv")
}

func init() { proto.RegisterFile("kv.proto", fileDescriptor0) }

var fileDescriptor0 = []byte{
	// 92 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0xc8, 0x2e, 0xd3, 0x2b,
	0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62, 0xce, 0x2e, 0x2b, 0x50, 0x32, 0xe2, 0x62, 0xca, 0x2e, 0x13,
	0x12, 0xe0, 0x62, 0xce, 0x2f, 0x28, 0x91, 0x60, 0x54, 0x60, 0xd4, 0xe0, 0x0d, 0x02, 0x31, 0x85,
	0x78, 0xb8, 0x18, 0xb3, 0x25, 0x98, 0x14, 0x18, 0x35, 0x38, 0x83, 0x18, 0xb3, 0x41, 0xbc, 0x32,
	0x09, 0x66, 0x05, 0x46, 0x0d, 0x9e, 0x20, 0xc6, 0xb2, 0x24, 0x36, 0xb0, 0x7e, 0x63, 0x40, 0x00,
	0x00, 0x00, 0xff, 0xff, 0xb9, 0xe2, 0x29, 0x13, 0x4b, 0x00, 0x00, 0x00,
}