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
		topicsLag              []topicLag
		maxReloadingUnits      int32
		currentReloadingTopics []string
		units                  []deploymentUnit
	}{
		{
			name:     "allReloadingFirstCase",
			topics:   []string{"t1", "t2", "t3", "t4"},
			realtime: []string{},
			topicsLag: []topicLag{
				topicLag{
					topic: "t1",
					lag:   1500,
				},
				topicLag{
					topic: "t2",
					lag:   1500,
				},
				topicLag{
					topic: "t3",
					lag:   1400,
				},
				topicLag{
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
			name:     "allReloadingSecondCaseMax3",
			topics:   []string{"t1", "t2", "t3", "t4"},
			realtime: []string{},
			topicsLag: []topicLag{
				topicLag{
					topic: "t1",
					lag:   1500,
				},
				topicLag{
					topic: "t2",
					lag:   1500,
				},
				topicLag{
					topic: "t3",
					lag:   1400,
				},
				topicLag{
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
			name:     "allReloadingThirdCaseCurrentThere",
			topics:   []string{"t1", "t2", "t3", "t4"},
			realtime: []string{},
			topicsLag: []topicLag{
				topicLag{
					topic: "t1",
					lag:   1500,
				},
				topicLag{
					topic: "t2",
					lag:   1500,
				},
				topicLag{
					topic: "t3",
					lag:   1400,
				},
				topicLag{
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
			name:     "allReloadingFourthCaseLagChangedShouldNotChangeAnything",
			topics:   []string{"t1", "t2", "t3", "t4"},
			realtime: []string{},
			topicsLag: []topicLag{
				topicLag{
					topic: "t1",
					lag:   1500,
				},
				topicLag{
					topic: "t2",
					lag:   1500,
				},
				topicLag{
					topic: "t3",
					lag:   2,
				},
				topicLag{
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
			name:     "allReloadingFifthCaseOneRealtimeOneMovesin",
			topics:   []string{"t1", "t2", "t3", "t4"},
			realtime: []string{"t3"},
			topicsLag: []topicLag{
				topicLag{
					topic: "t1",
					lag:   1500,
				},
				topicLag{
					topic: "t2",
					lag:   1500,
				},
				topicLag{
					topic: "t3",
					lag:   2,
				},
				topicLag{
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
			name:     "allReloadingSixthCaseAllRealtime",
			topics:   []string{"t1", "t2", "t3", "t4"},
			realtime: []string{"t1", "t2", "t3", "t4"},
			topicsLag: []topicLag{
				topicLag{
					topic: "t1",
					lag:   1,
				},
				topicLag{
					topic: "t2",
					lag:   1,
				},
				topicLag{
					topic: "t3",
					lag:   2,
				},
				topicLag{
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
				tc.topicsLag,
				tc.maxReloadingUnits,
				tc.currentReloadingTopics,
			)
			allocator.allocateReloadingUnits()
			if !reflect.DeepEqual(allocator.units, tc.units) {
				t.Errorf("expected: %+v, got: %+v\n", tc.units, allocator.units)
			}
		})
	}
}
