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
	corev1 "k8s.io/api/core/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
)

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *Group) DeepCopyInto(out *Group) {
	*out = *in
	if in.LoaderCurrentOffset != nil {
		in, out := &in.LoaderCurrentOffset, &out.LoaderCurrentOffset
		*out = new(int64)
		**out = **in
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new Group.
func (in *Group) DeepCopy() *Group {
	if in == nil {
		return nil
	}
	out := new(Group)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *MaskStatus) DeepCopyInto(out *MaskStatus) {
	*out = *in
	if in.CurrentMaskStatus != nil {
		in, out := &in.CurrentMaskStatus, &out.CurrentMaskStatus
		*out = make(map[string]TopicMaskStatus, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
	if in.DesiredMaskStatus != nil {
		in, out := &in.DesiredMaskStatus, &out.DesiredMaskStatus
		*out = make(map[string]TopicMaskStatus, len(*in))
		for key, val := range *in {
			(*out)[key] = val
		}
	}
	if in.CurrentMaskVersion != nil {
		in, out := &in.CurrentMaskVersion, &out.CurrentMaskVersion
		*out = new(string)
		**out = **in
	}
	if in.DesiredMaskVersion != nil {
		in, out := &in.DesiredMaskVersion, &out.DesiredMaskVersion
		*out = new(string)
		**out = **in
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new MaskStatus.
func (in *MaskStatus) DeepCopy() *MaskStatus {
	if in == nil {
		return nil
	}
	out := new(MaskStatus)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *RedshiftBatcherSpec) DeepCopyInto(out *RedshiftBatcherSpec) {
	*out = *in
	if in.PodTemplate != nil {
		in, out := &in.PodTemplate, &out.PodTemplate
		*out = new(RedshiftPodTemplateSpec)
		(*in).DeepCopyInto(*out)
	}
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
func (in *RedshiftLoaderSpec) DeepCopyInto(out *RedshiftLoaderSpec) {
	*out = *in
	if in.RedshiftMaxOpenConns != nil {
		in, out := &in.RedshiftMaxOpenConns, &out.RedshiftMaxOpenConns
		*out = new(int)
		**out = **in
	}
	if in.RedshiftMaxIdleConns != nil {
		in, out := &in.RedshiftMaxIdleConns, &out.RedshiftMaxIdleConns
		*out = new(int)
		**out = **in
	}
	if in.RedshiftGroup != nil {
		in, out := &in.RedshiftGroup, &out.RedshiftGroup
		*out = new(string)
		**out = **in
	}
	if in.PodTemplate != nil {
		in, out := &in.PodTemplate, &out.PodTemplate
		*out = new(RedshiftPodTemplateSpec)
		(*in).DeepCopyInto(*out)
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new RedshiftLoaderSpec.
func (in *RedshiftLoaderSpec) DeepCopy() *RedshiftLoaderSpec {
	if in == nil {
		return nil
	}
	out := new(RedshiftLoaderSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *RedshiftPodTemplateSpec) DeepCopyInto(out *RedshiftPodTemplateSpec) {
	*out = *in
	if in.Image != nil {
		in, out := &in.Image, &out.Image
		*out = new(string)
		**out = **in
	}
	if in.Resources != nil {
		in, out := &in.Resources, &out.Resources
		*out = new(corev1.ResourceRequirements)
		(*in).DeepCopyInto(*out)
	}
	if in.Tolerations != nil {
		in, out := &in.Tolerations, &out.Tolerations
		*out = new([]corev1.Toleration)
		if **in != nil {
			in, out := *in, *out
			*out = make([]corev1.Toleration, len(*in))
			for i := range *in {
				(*in)[i].DeepCopyInto(&(*out)[i])
			}
		}
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new RedshiftPodTemplateSpec.
func (in *RedshiftPodTemplateSpec) DeepCopy() *RedshiftPodTemplateSpec {
	if in == nil {
		return nil
	}
	out := new(RedshiftPodTemplateSpec)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *RedshiftSink) DeepCopyInto(out *RedshiftSink) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
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
	if in.SecretRefName != nil {
		in, out := &in.SecretRefName, &out.SecretRefName
		*out = new(string)
		**out = **in
	}
	if in.SecretRefNamespace != nil {
		in, out := &in.SecretRefNamespace, &out.SecretRefNamespace
		*out = new(string)
		**out = **in
	}
	in.Batcher.DeepCopyInto(&out.Batcher)
	in.Loader.DeepCopyInto(&out.Loader)
	if in.ReleaseCondition != nil {
		in, out := &in.ReleaseCondition, &out.ReleaseCondition
		*out = new(ReleaseCondition)
		(*in).DeepCopyInto(*out)
	}
	if in.TopicReleaseCondition != nil {
		in, out := &in.TopicReleaseCondition, &out.TopicReleaseCondition
		*out = make(map[string]ReleaseCondition, len(*in))
		for key, val := range *in {
			(*out)[key] = *val.DeepCopy()
		}
	}
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
	if in.MaskStatus != nil {
		in, out := &in.MaskStatus, &out.MaskStatus
		*out = new(MaskStatus)
		(*in).DeepCopyInto(*out)
	}
	if in.TopicGroup != nil {
		in, out := &in.TopicGroup, &out.TopicGroup
		*out = make(map[string]Group, len(*in))
		for key, val := range *in {
			(*out)[key] = *val.DeepCopy()
		}
	}
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

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *ReleaseCondition) DeepCopyInto(out *ReleaseCondition) {
	*out = *in
	if in.MaxBatcherLag != nil {
		in, out := &in.MaxBatcherLag, &out.MaxBatcherLag
		*out = new(int64)
		**out = **in
	}
	if in.MaxLoaderLag != nil {
		in, out := &in.MaxLoaderLag, &out.MaxLoaderLag
		*out = new(int64)
		**out = **in
	}
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new ReleaseCondition.
func (in *ReleaseCondition) DeepCopy() *ReleaseCondition {
	if in == nil {
		return nil
	}
	out := new(ReleaseCondition)
	in.DeepCopyInto(out)
	return out
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *TopicMaskStatus) DeepCopyInto(out *TopicMaskStatus) {
	*out = *in
}

// DeepCopy is an autogenerated deepcopy function, copying the receiver, creating a new TopicMaskStatus.
func (in *TopicMaskStatus) DeepCopy() *TopicMaskStatus {
	if in == nil {
		return nil
	}
	out := new(TopicMaskStatus)
	in.DeepCopyInto(out)
	return out
}
