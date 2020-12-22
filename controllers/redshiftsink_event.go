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
		fmt.Sprintf("Created deployment: %s", d.Name))
}

type DeploymentUpdatedEvent struct {
	Object runtime.Object
	Name   string
}

func (d DeploymentUpdatedEvent) Record(recorder record.EventRecorder) {
	recorder.Event(d.Object,
		K8sEventTypeNormal,
		"DeploymentUpdated",
		fmt.Sprintf("Updated deployment: %s", d.Name))
}

type TopicReleasedEvent struct {
	Object  runtime.Object
	Topic   string
	Version string
}

func (d TopicReleasedEvent) Record(recorder record.EventRecorder) {
	recorder.Event(d.Object,
		K8sEventTypeNormal,
		"TopicReleased",
		fmt.Sprintf("Released topic: %s, maskVersion: %s", d.Topic, d.Version))
}
