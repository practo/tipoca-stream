package controllers

import (
	"reflect"
	"testing"
)

func TestAllocateReloadingUnits(t *testing.T) {
	// t.Parallel()

	tests := []struct {
		name                   string
		topics                 []string
		realtime               []string
		topicsLast             []topicLast
		maxReloadingUnits      int32
		currentReloadingTopics []string
		units                  []deploymentUnit
	}{
		{
			name:     "FirstCase",
			topics:   []string{"db.inventory.t1", "db.inventory.t2", "db.inventory.t3", "db.inventory.t4"},
			realtime: []string{},
			topicsLast: []topicLast{
				topicLast{
					topic: "db.inventory.t1",
					last:  1500,
				},
				topicLast{
					topic: "db.inventory.t2",
					last:  1500,
				},
				topicLast{
					topic: "db.inventory.t3",
					last:  1400,
				},
				topicLast{
					topic: "db.inventory.t4",
					last:  1400,
				},
			},
			maxReloadingUnits:      1,
			currentReloadingTopics: []string{},
			units: []deploymentUnit{
				deploymentUnit{
					id:     "t3",
					topics: []string{"db.inventory.t3"},
				},
			},
		},
		{
			name:     "SecondCaseMax3",
			topics:   []string{"db.inventory.t1", "db.inventory.t2", "db.inventory.t3", "db.inventory.t4"},
			realtime: []string{},
			topicsLast: []topicLast{
				topicLast{
					topic: "db.inventory.t1",
					last:  1500,
				},
				topicLast{
					topic: "db.inventory.t2",
					last:  1500,
				},
				topicLast{
					topic: "db.inventory.t3",
					last:  1400,
				},
				topicLast{
					topic: "db.inventory.t4",
					last:  1400,
				},
			},
			maxReloadingUnits:      3,
			currentReloadingTopics: []string{},
			units: []deploymentUnit{
				deploymentUnit{
					id:     "t3",
					topics: []string{"db.inventory.t3"},
				},
				deploymentUnit{
					id:     "t4",
					topics: []string{"db.inventory.t4"},
				},
				deploymentUnit{
					id:     "t1",
					topics: []string{"db.inventory.t1"},
				},
			},
		},
		{
			name:     "ThirdCaseCurrentThere",
			topics:   []string{"db.inventory.t1", "db.inventory.t2", "db.inventory.t3", "db.inventory.t4"},
			realtime: []string{},
			topicsLast: []topicLast{
				topicLast{
					topic: "db.inventory.t1",
					last:  1500,
				},
				topicLast{
					topic: "db.inventory.t2",
					last:  1500,
				},
				topicLast{
					topic: "db.inventory.t3",
					last:  1400,
				},
				topicLast{
					topic: "db.inventory.t4",
					last:  1400,
				},
			},
			maxReloadingUnits:      3,
			currentReloadingTopics: []string{"db.inventory.t1", "db.inventory.t2", "db.inventory.t3"},
			units: []deploymentUnit{
				deploymentUnit{
					id:     "t1",
					topics: []string{"db.inventory.t1"},
				},
				deploymentUnit{
					id:     "t2",
					topics: []string{"db.inventory.t2"},
				},
				deploymentUnit{
					id:     "t3",
					topics: []string{"db.inventory.t3"},
				},
			},
		},
		{
			name:     "FourthCaseLagChangedShouldNotChangeAnything",
			topics:   []string{"db.inventory.t1", "db.inventory.t2", "db.inventory.t3", "db.inventory.t4"},
			realtime: []string{},
			topicsLast: []topicLast{
				topicLast{
					topic: "db.inventory.t1",
					last:  1500,
				},
				topicLast{
					topic: "db.inventory.t2",
					last:  1500,
				},
				topicLast{
					topic: "db.inventory.t3",
					last:  2,
				},
				topicLast{
					topic: "db.inventory.t4",
					last:  1,
				},
			},
			maxReloadingUnits:      3,
			currentReloadingTopics: []string{"db.inventory.t1", "db.inventory.t2", "db.inventory.t3"},
			units: []deploymentUnit{
				deploymentUnit{
					id:     "t1",
					topics: []string{"db.inventory.t1"},
				},
				deploymentUnit{
					id:     "t2",
					topics: []string{"db.inventory.t2"},
				},
				deploymentUnit{
					id:     "t3",
					topics: []string{"db.inventory.t3"},
				},
			},
		},
		{
			name:     "FifthCaseOneRealtimeOneMovesin",
			topics:   []string{"db.inventory.t1", "db.inventory.t2", "db.inventory.t3", "db.inventory.t4"},
			realtime: []string{"db.inventory.t3"},
			topicsLast: []topicLast{
				topicLast{
					topic: "db.inventory.t1",
					last:  1500,
				},
				topicLast{
					topic: "db.inventory.t2",
					last:  1500,
				},
				topicLast{
					topic: "db.inventory.t3",
					last:  2,
				},
				topicLast{
					topic: "db.inventory.t4",
					last:  1,
				},
			},
			maxReloadingUnits:      3,
			currentReloadingTopics: []string{"db.inventory.t1", "db.inventory.t2", "db.inventory.t3"},
			units: []deploymentUnit{
				deploymentUnit{
					id:     "t1",
					topics: []string{"db.inventory.t1"},
				},
				deploymentUnit{
					id:     "t2",
					topics: []string{"db.inventory.t2"},
				},
				deploymentUnit{
					id:     "t4",
					topics: []string{"db.inventory.t4"},
				},
				deploymentUnit{
					id:     "realtime",
					topics: []string{"db.inventory.t3"},
				},
			},
		},
		{
			name:     "SixthCaseAllRealtime",
			topics:   []string{"db.inventory.t1", "db.inventory.t2", "db.inventory.t3", "db.inventory.t4"},
			realtime: []string{"db.inventory.t1", "db.inventory.t2", "db.inventory.t3", "db.inventory.t4"},
			topicsLast: []topicLast{
				topicLast{
					topic: "db.inventory.t1",
					last:  1,
				},
				topicLast{
					topic: "db.inventory.t2",
					last:  1,
				},
				topicLast{
					topic: "db.inventory.t3",
					last:  2,
				},
				topicLast{
					topic: "db.inventory.t4",
					last:  1,
				},
			},
			maxReloadingUnits:      3,
			currentReloadingTopics: []string{"db.inventory.t1", "db.inventory.t2", "db.inventory.t4", "db.inventory.t3"},
			units: []deploymentUnit{
				deploymentUnit{
					id:     "realtime",
					topics: []string{"db.inventory.t1", "db.inventory.t2", "db.inventory.t3", "db.inventory.t4"},
				},
			},
		},
		{
			name:     "K8sNameCompatibility",
			topics:   []string{"db.inventory.t1_aks"},
			realtime: []string{},
			topicsLast: []topicLast{
				topicLast{
					topic: "db.inventory.t1_aks",
					last:  1,
				},
			},
			maxReloadingUnits:      3,
			currentReloadingTopics: []string{},
			units: []deploymentUnit{
				deploymentUnit{
					id:     "t1-aks",
					topics: []string{"db.inventory.t1_aks"},
				},
			},
		},
		{
			name:     "UnitsGoingAboveMax",
			topics:   []string{"db.inventory.t1", "db.inventory.t2", "db.inventory.t3", "db.inventory.t4", "db.inventory.t5", "db.inventory.t6", "db.inventory.t7", "db.inventory.t8", "db.inventory.t9"},
			realtime: []string{"db.inventory.t1"},
			topicsLast: []topicLast{
				topicLast{
					topic: "db.inventory.t1",
					last:  1,
				},
				topicLast{
					topic: "db.inventory.t2",
					last:  10,
				},
				topicLast{
					topic: "db.inventory.t3",
					last:  100,
				},
				topicLast{
					topic: "db.inventory.t4",
					last:  1000,
				},
				topicLast{
					topic: "db.inventory.t5",
					last:  10000,
				},
				topicLast{
					topic: "db.inventory.t6",
					last:  20000,
				},
				topicLast{
					topic: "db.inventory.t7",
					last:  100000,
				},
			},
			maxReloadingUnits:      5,
			currentReloadingTopics: []string{"db.inventory.t1", "db.inventory.t2", "db.inventory.t3", "db.inventory.t4", "db.inventory.t5"},
			units: []deploymentUnit{
				deploymentUnit{
					id:     "t2",
					topics: []string{"db.inventory.t2"},
				},
				deploymentUnit{
					id:     "t3",
					topics: []string{"db.inventory.t3"},
				},
				deploymentUnit{
					id:     "t4",
					topics: []string{"db.inventory.t4"},
				},
				deploymentUnit{
					id:     "t5",
					topics: []string{"db.inventory.t5"},
				},
				deploymentUnit{
					id:     "t6",
					topics: []string{"db.inventory.t6"},
				},
				deploymentUnit{
					id:     "realtime",
					topics: []string{"db.inventory.t1"},
				},
			},
		},
		{
			name:     "UnitsGoingAboveMaxCase2",
			topics:   []string{"db.inventory.t1", "db.inventory.t2", "db.inventory.t3", "db.inventory.t4", "db.inventory.t5", "db.inventory.t6", "db.inventory.t7", "db.inventory.t8", "db.inventory.t9"},
			realtime: []string{"db.inventory.t3", "db.inventory.t4"},
			topicsLast: []topicLast{
				topicLast{
					topic: "db.inventory.t1",
					last:  1,
				},
				topicLast{
					topic: "db.inventory.t2",
					last:  10,
				},
				topicLast{
					topic: "db.inventory.t3",
					last:  100,
				},
				topicLast{
					topic: "db.inventory.t4",
					last:  1000,
				},
				topicLast{
					topic: "db.inventory.t5",
					last:  10000,
				},
				topicLast{
					topic: "db.inventory.t6",
					last:  20000,
				},
				topicLast{
					topic: "db.inventory.t7",
					last:  100000,
				},
			},
			maxReloadingUnits:      5,
			currentReloadingTopics: []string{"db.inventory.t1", "db.inventory.t2", "db.inventory.t3", "db.inventory.t4", "db.inventory.t5"},
			units: []deploymentUnit{
				deploymentUnit{
					id:     "t1",
					topics: []string{"db.inventory.t1"},
				},
				deploymentUnit{
					id:     "t2",
					topics: []string{"db.inventory.t2"},
				},
				deploymentUnit{
					id:     "t5",
					topics: []string{"db.inventory.t5"},
				},
				deploymentUnit{
					id:     "t6",
					topics: []string{"db.inventory.t6"},
				},
				deploymentUnit{
					id:     "t7",
					topics: []string{"db.inventory.t7"},
				},
				deploymentUnit{
					id:     "realtime",
					topics: []string{"db.inventory.t3", "db.inventory.t4"},
				},
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			allocator := newUnitAllocator(
				"testrsk",
				tc.topics,
				tc.realtime,
				tc.topicsLast,
				tc.maxReloadingUnits,
				tc.currentReloadingTopics,
				nil, // TODO add test cases for them also
				nil,
			)
			allocator.allocateReloadingUnits()
			if !reflect.DeepEqual(allocator.units, tc.units) {
				t.Errorf("\nexpected (%v): %+v\ngot (%v): %+v\n", len(tc.units), tc.units, len(allocator.units), allocator.units)
			}
		})
	}
}
