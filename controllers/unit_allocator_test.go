package controllers

import (
	"reflect"
	"testing"
)

func TestAllocateReloadingUnits(t *testing.T) {
	t.Parallel()

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
			name:                   "RealFirstCaseWhenTopicLagEmpty",
			topics:                 []string{"t1", "t2"},
			realtime:               []string{},
			topicsLast:             []topicLast{},
			maxReloadingUnits:      3,
			currentReloadingTopics: []string{},
			units: []deploymentUnit{
				deploymentUnit{
					id:     "t1",
					topics: []string{"t1"},
				},
				deploymentUnit{
					id:     "t2",
					topics: []string{"t2"},
				},
			},
		},
		{
			name:     "FirstCase",
			topics:   []string{"t1", "t2", "t3", "t4"},
			realtime: []string{},
			topicsLast: []topicLast{
				topicLast{
					topic: "t1",
					lag:   1500,
				},
				topicLast{
					topic: "t2",
					lag:   1500,
				},
				topicLast{
					topic: "t3",
					lag:   1400,
				},
				topicLast{
					topic: "t4",
					lag:   1400,
				},
			},
			maxReloadingUnits:      1,
			currentReloadingTopics: []string{},
			units: []deploymentUnit{
				deploymentUnit{
					id:     "t3",
					topics: []string{"t3"},
				},
			},
		},
		{
			name:     "SecondCaseMax3",
			topics:   []string{"t1", "t2", "t3", "t4"},
			realtime: []string{},
			topicsLast: []topicLast{
				topicLast{
					topic: "t1",
					lag:   1500,
				},
				topicLast{
					topic: "t2",
					lag:   1500,
				},
				topicLast{
					topic: "t3",
					lag:   1400,
				},
				topicLast{
					topic: "t4",
					lag:   1400,
				},
			},
			maxReloadingUnits:      3,
			currentReloadingTopics: []string{},
			units: []deploymentUnit{
				deploymentUnit{
					id:     "t3",
					topics: []string{"t3"},
				},
				deploymentUnit{
					id:     "t4",
					topics: []string{"t4"},
				},
				deploymentUnit{
					id:     "t1",
					topics: []string{"t1"},
				},
			},
		},
		{
			name:     "ThirdCaseCurrentThere",
			topics:   []string{"t1", "t2", "t3", "t4"},
			realtime: []string{},
			topicsLast: []topicLast{
				topicLast{
					topic: "t1",
					lag:   1500,
				},
				topicLast{
					topic: "t2",
					lag:   1500,
				},
				topicLast{
					topic: "t3",
					lag:   1400,
				},
				topicLast{
					topic: "t4",
					lag:   1400,
				},
			},
			maxReloadingUnits:      3,
			currentReloadingTopics: []string{"t1", "t2", "t3"},
			units: []deploymentUnit{
				deploymentUnit{
					id:     "t1",
					topics: []string{"t1"},
				},
				deploymentUnit{
					id:     "t2",
					topics: []string{"t2"},
				},
				deploymentUnit{
					id:     "t3",
					topics: []string{"t3"},
				},
			},
		},
		{
			name:     "FourthCaseLagChangedShouldNotChangeAnything",
			topics:   []string{"t1", "t2", "t3", "t4"},
			realtime: []string{},
			topicsLast: []topicLast{
				topicLast{
					topic: "t1",
					lag:   1500,
				},
				topicLast{
					topic: "t2",
					lag:   1500,
				},
				topicLast{
					topic: "t3",
					lag:   2,
				},
				topicLast{
					topic: "t4",
					lag:   1,
				},
			},
			maxReloadingUnits:      3,
			currentReloadingTopics: []string{"t1", "t2", "t3"},
			units: []deploymentUnit{
				deploymentUnit{
					id:     "t1",
					topics: []string{"t1"},
				},
				deploymentUnit{
					id:     "t2",
					topics: []string{"t2"},
				},
				deploymentUnit{
					id:     "t3",
					topics: []string{"t3"},
				},
			},
		},
		{
			name:     "FifthCaseOneRealtimeOneMovesin",
			topics:   []string{"t1", "t2", "t3", "t4"},
			realtime: []string{"t3"},
			topicsLast: []topicLast{
				topicLast{
					topic: "t1",
					lag:   1500,
				},
				topicLast{
					topic: "t2",
					lag:   1500,
				},
				topicLast{
					topic: "t3",
					lag:   2,
				},
				topicLast{
					topic: "t4",
					lag:   1,
				},
			},
			maxReloadingUnits:      3,
			currentReloadingTopics: []string{"t1", "t2", "t3"},
			units: []deploymentUnit{
				deploymentUnit{
					id:     "t1",
					topics: []string{"t1"},
				},
				deploymentUnit{
					id:     "t2",
					topics: []string{"t2"},
				},
				deploymentUnit{
					id:     "t4",
					topics: []string{"t4"},
				},
				deploymentUnit{
					id:     "realtime",
					topics: []string{"t3"},
				},
			},
		},
		{
			name:     "SixthCaseAllRealtime",
			topics:   []string{"t1", "t2", "t3", "t4"},
			realtime: []string{"t1", "t2", "t3", "t4"},
			topicsLast: []topicLast{
				topicLast{
					topic: "t1",
					lag:   1,
				},
				topicLast{
					topic: "t2",
					lag:   1,
				},
				topicLast{
					topic: "t3",
					lag:   2,
				},
				topicLast{
					topic: "t4",
					lag:   1,
				},
			},
			maxReloadingUnits:      3,
			currentReloadingTopics: []string{"t1", "t2", "t4", "t3"},
			units: []deploymentUnit{
				deploymentUnit{
					id:     "realtime",
					topics: []string{"t1", "t2", "t3", "t4"},
				},
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			allocator := newUnitAllocator(
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
				t.Errorf("expected: %+v, got: %+v\n", tc.units, allocator.units)
			}
		})
	}
}
