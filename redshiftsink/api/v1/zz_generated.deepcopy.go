// +build !ignore_autogenerated

/*


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

// Code generated by controller-gen. DO NOT EDIT.

package v1

import (
	runtime "k8s.io/apimachinery/pkg/runtime"
)

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *RedshiftBatcherSpec) DeepCopyInto(out *RedshiftBatcherSpec) {
	*out = *in
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new RedshiftBatcherSpec.
func (in *RedshiftBatcherSpec) DeepCopy() *RedshiftBatcherSpec {
	if in == nil {
		return nil
	}
	out := new(RedshiftBatcherSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *RedshiftSink) DeepCopyInto(out *RedshiftSink) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = in.Spec
	out.Status = in.Status
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new RedshiftSink.
func (in *RedshiftSink) DeepCopy() *RedshiftSink {
	if in == nil {
		return nil
	}
	out := new(RedshiftSink)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *RedshiftSink) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *RedshiftSinkList) DeepCopyInto(out *RedshiftSinkList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]RedshiftSink, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new RedshiftSinkList.
func (in *RedshiftSinkList) DeepCopy() *RedshiftSinkList {
	if in == nil {
		return nil
	}
	out := new(RedshiftSinkList)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *RedshiftSinkList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *RedshiftSinkSpec) DeepCopyInto(out *RedshiftSinkSpec) {
	*out = *in
	out.Batcher = in.Batcher
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new RedshiftSinkSpec.
func (in *RedshiftSinkSpec) DeepCopy() *RedshiftSinkSpec {
	if in == nil {
		return nil
	}
	out := new(RedshiftSinkSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *RedshiftSinkStatus) DeepCopyInto(out *RedshiftSinkStatus) {
	*out = *in
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new RedshiftSinkStatus.
func (in *RedshiftSinkStatus) DeepCopy() *RedshiftSinkStatus {
	if in == nil {
		return nil
	}
	out := new(RedshiftSinkStatus)
	in.DeepCopyInto(out)
	return out
}
