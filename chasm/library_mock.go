// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// Code generated by MockGen. DO NOT EDIT.
// Source: library.go
//
// Generated by this command:
//
//	mockgen -copyright_file ../LICENSE -package chasm -source library.go -destination library_mock.go
//

// Package chasm is a generated GoMock package.
package chasm

import (
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockLibrary is a mock of Library interface.
type MockLibrary struct {
	ctrl     *gomock.Controller
	recorder *MockLibraryMockRecorder
}

// MockLibraryMockRecorder is the mock recorder for MockLibrary.
type MockLibraryMockRecorder struct {
	mock *MockLibrary
}

// NewMockLibrary creates a new mock instance.
func NewMockLibrary(ctrl *gomock.Controller) *MockLibrary {
	mock := &MockLibrary{ctrl: ctrl}
	mock.recorder = &MockLibraryMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockLibrary) EXPECT() *MockLibraryMockRecorder {
	return m.recorder
}

// Components mocks base method.
func (m *MockLibrary) Components() []*RegistrableComponent {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Components")
	ret0, _ := ret[0].([]*RegistrableComponent)
	return ret0
}

// Components indicates an expected call of Components.
func (mr *MockLibraryMockRecorder) Components() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Components", reflect.TypeOf((*MockLibrary)(nil).Components))
}

// Name mocks base method.
func (m *MockLibrary) Name() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Name")
	ret0, _ := ret[0].(string)
	return ret0
}

// Name indicates an expected call of Name.
func (mr *MockLibraryMockRecorder) Name() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Name", reflect.TypeOf((*MockLibrary)(nil).Name))
}

// Tasks mocks base method.
func (m *MockLibrary) Tasks() []*RegistrableTask {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Tasks")
	ret0, _ := ret[0].([]*RegistrableTask)
	return ret0
}

// Tasks indicates an expected call of Tasks.
func (mr *MockLibraryMockRecorder) Tasks() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Tasks", reflect.TypeOf((*MockLibrary)(nil).Tasks))
}

// mustEmbedUnimplementedLibrary mocks base method.
func (m *MockLibrary) mustEmbedUnimplementedLibrary() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "mustEmbedUnimplementedLibrary")
}

// mustEmbedUnimplementedLibrary indicates an expected call of mustEmbedUnimplementedLibrary.
func (mr *MockLibraryMockRecorder) mustEmbedUnimplementedLibrary() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "mustEmbedUnimplementedLibrary", reflect.TypeOf((*MockLibrary)(nil).mustEmbedUnimplementedLibrary))
}
