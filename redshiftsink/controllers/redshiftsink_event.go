package controllers

import (
	"fmt"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
)

const (
	K8sEventTypeNormal  = "Normal"
	K8sEventTypeWarning = "Warning"
)

// ReconcilerEvent represents the action of the operator
// having actually done anything. Any meaningful change should
// result in one of these.
type ReconcilerEvent interface {

	// Record this into an event recorder as a Kubernetes API event
	Record(recorder record.EventRecorder)
}

type DeploymentCreatedEvent struct {
	Object runtime.Object
	Name   string
}

func (d DeploymentCreatedEvent) Record(recorder record.EventRecorder) {
	recorder.Event(d.Object,
		K8sEventTypeNormal,
		"DeploymentCreated",
		fmt.Sprintf("Created deployment: %q", d.Name))
}

type DeploymentUpdateEvent struct {
	Object runtime.Object
	Name   string
}

func (d DeploymentUpdateEvent) Record(recorder record.EventRecorder) {
	recorder.Event(d.Object,
		K8sEventTypeNormal,
		"DeploymentUpdated",
		fmt.Sprintf("Updated deployment: %q", d.Name))
}
