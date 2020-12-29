package controllers

import (
	tipocav1 "github.com/practo/tipoca-stream/redshiftsink/api/v1"
	"reflect"
	"testing"
)

func TestComputeConsumerGroups(t *testing.T) {
	topicGroups := map[string]tipocav1.Group{
		"db.inventory.customers": tipocav1.Group{
			ID:                "6c5571",
			LoaderTopicPrefix: "loader-6c5571-",
		},
		"db.inventory.justifications": tipocav1.Group{
			ID:                "6c5571",
			LoaderTopicPrefix: "loader-6c5571-",
		},
		"db.inventory.orders": tipocav1.Group{
			ID:                "6c5571",
			LoaderTopicPrefix: "loader-6c5571-",
		},
	}
	topics := []string{
		"db.inventory.customers",
		"db.inventory.justifications",
		"db.inventory.orders",
	}

	consumerGroups, err := computeConsumerGroups(topicGroups, topics)
	if err != nil {
		t.Error(err)
	}

	expectedConsumerGroups := map[string]consumerGroup{
		"6c5571": consumerGroup{
			topics:            topics,
			loaderTopicPrefix: "loader-6c5571-",
		},
	}

	if !reflect.DeepEqual(consumerGroups, expectedConsumerGroups) {
		t.Errorf(
			"expected: %+v, got: %+v\n",
			expectedConsumerGroups,
			consumerGroups,
		)
	}
}
