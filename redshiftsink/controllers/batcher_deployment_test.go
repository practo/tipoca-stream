package controllers

import (
	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	"github.com/practo/tipoca-stream/redshiftsink/pkg/redshiftbatcher"
	corev1 "k8s.io/api/core/v1"
	resource "k8s.io/apimachinery/pkg/api/resource"
	"reflect"
	"testing"
)

func TestApplyBatcherSinkGroupDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		rsk          tipocav1.RedshiftSink
		sgType       string
		defaultImage string
		expectedSpec tipocav1.SinkGroupSpec
	}{
		{
			name: "testAllSinkGroupSpecApplies",
			rsk: tipocav1.RedshiftSink{
				Spec: tipocav1.RedshiftSinkSpec{
					Batcher: tipocav1.RedshiftBatcherSpec{
						SinkGroup: &tipocav1.SinkGroup{
							All: &tipocav1.SinkGroupSpec{
								DeploymentUnit: &tipocav1.DeploymentUnit{
									PodTemplate: &tipocav1.RedshiftPodTemplateSpec{
										Tolerations: &[]corev1.Toleration{
											corev1.Toleration{
												Key:      "dedicated",
												Operator: corev1.TolerationOpExists,
												Value:    "redshiftsink",
												Effect:   corev1.TaintEffectNoSchedule,
											},
										},
									},
								},
							},
						},
					},
				},
			},
			sgType:       ReloadDupeSinkGroup,
			defaultImage: "abc",
			expectedSpec: tipocav1.SinkGroupSpec{
				MaxSizePerBatch:   toQuantityPtr(resource.MustParse("0.5Mi")),
				MaxWaitSeconds:    toIntPtr(60),
				MaxConcurrency:    toIntPtr(10),
				MaxProcessingTime: &redshiftbatcher.DefaultMaxProcessingTime,
				MaxReloadingUnits: toInt32Ptr(10),
				DeploymentUnit: &tipocav1.DeploymentUnit{
					PodTemplate: &tipocav1.RedshiftPodTemplateSpec{
						Image:     toStrPtr("abc"),
						Resources: nil,
						Tolerations: &[]corev1.Toleration{
							corev1.Toleration{
								Key:      "dedicated",
								Operator: corev1.TolerationOpExists,
								Value:    "redshiftsink",
								Effect:   corev1.TaintEffectNoSchedule,
							},
						},
					},
				},
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			gotSpec := applyBatcherSinkGroupDefaults(
				&tc.rsk, tc.sgType, tc.defaultImage,
			)
			if !reflect.DeepEqual(tc.expectedSpec, gotSpec) {
				t.Errorf("\nexpected: %+v\ngot: %+v\n", tc.expectedSpec, gotSpec)
				t.Errorf("\nexpected.MaxConcurrency: %+v\ngot.MaxConcurrency: %+v\n", *tc.expectedSpec.MaxConcurrency, *gotSpec.MaxConcurrency)
				t.Errorf("\nexpected.DeploymentUnit: %+v\ngot.DeploymentUnit: %+v\n", *tc.expectedSpec.DeploymentUnit.PodTemplate.Tolerations, *gotSpec.DeploymentUnit.PodTemplate.Tolerations)
			}
		})
	}
}
