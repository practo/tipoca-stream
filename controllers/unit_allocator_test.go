package controllers

import (
	"reflect"
	"testing"
)

func TestAllocateUnitChunks(t *testing.T) {
	t.Parallel()
	//go test -v ./controllers/... -run ^TestAllocateUnitChunks

	tests := []struct {
		name      string
		topics    []string
		chunkSize int
		units     []deploymentUnit
	}{
		{
			name:      "singleChunk",
			topics:    []string{"db.inventory.t1", "db.inventory.t2", "db.inventory.t3", "db.inventory.t4"},
			chunkSize: 100,
			units: []deploymentUnit{
				deploymentUnit{
					id:     "0",
					topics: []string{"db.inventory.t1", "db.inventory.t2", "db.inventory.t3", "db.inventory.t4"},
				},
			},
		},
		{
			name:      "multiChunk",
			topics:    []string{"t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10"},
			chunkSize: 3,
			units: []deploymentUnit{
				deploymentUnit{
					id:     "0",
					topics: []string{"t1", "t2", "t3"},
				},
				deploymentUnit{
					id:     "1",
					topics: []string{"t4", "t5", "t6"},
				},
				deploymentUnit{
					id:     "2",
					topics: []string{"t7", "t8", "t9"},
				},
				deploymentUnit{
					id:     "3",
					topics: []string{"t10"},
				},
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			gotUnits := allocateUnitWithChunks(tc.topics, nil, tc.chunkSize)
			if !reflect.DeepEqual(gotUnits, tc.units) {
				t.Errorf("\nexpected (%v): %+v\ngot (%v): %+v\n", len(tc.units), tc.units, len(gotUnits), gotUnits)
			}
		})
	}
}

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
		{
			name:     "LoaderTopicTest",
			topics:   []string{"loader-373ebe-db.inventory.t1", "loader-373ebe-db.inventory.t2", "loader-373ebe-db.inventory.t3", "loader-373ebe-db.inventory.t4", "loader-373ebe-db.inventory.t5", "loader-373ebe-db.inventory.t6", "loader-373ebe-db.inventory.t7", "loader-373ebe-db.inventory.t8", "loader-373ebe-db.inventory.t9"},
			realtime: []string{"loader-373ebe-db.inventory.t3", "loader-373ebe-db.inventory.t4"},
			topicsLast: []topicLast{
				topicLast{
					topic: "loader-373ebe-db.inventory.t1",
					last:  1,
				},
				topicLast{
					topic: "loader-373ebe-db.inventory.t2",
					last:  10,
				},
				topicLast{
					topic: "loader-373ebe-db.inventory.t3",
					last:  100,
				},
				topicLast{
					topic: "loader-373ebe-db.inventory.t4",
					last:  1000,
				},
				topicLast{
					topic: "loader-373ebe-db.inventory.t5",
					last:  10000,
				},
				topicLast{
					topic: "loader-373ebe-db.inventory.t6",
					last:  20000,
				},
				topicLast{
					topic: "loader-373ebe-db.inventory.t7",
					last:  100000,
				},
			},
			maxReloadingUnits:      5,
			currentReloadingTopics: []string{"loader-373ebe-db.inventory.t1", "loader-373ebe-db.inventory.t2", "loader-373ebe-db.inventory.t3", "loader-373ebe-db.inventory.t4", "loader-373ebe-db.inventory.t5"},
			units: []deploymentUnit{
				deploymentUnit{
					id:     "t1",
					topics: []string{"loader-373ebe-db.inventory.t1"},
				},
				deploymentUnit{
					id:     "t2",
					topics: []string{"loader-373ebe-db.inventory.t2"},
				},
				deploymentUnit{
					id:     "t5",
					topics: []string{"loader-373ebe-db.inventory.t5"},
				},
				deploymentUnit{
					id:     "t6",
					topics: []string{"loader-373ebe-db.inventory.t6"},
				},
				deploymentUnit{
					id:     "t7",
					topics: []string{"loader-373ebe-db.inventory.t7"},
				},
				deploymentUnit{
					id:     "realtime",
					topics: []string{"loader-373ebe-db.inventory.t3", "loader-373ebe-db.inventory.t4"},
				},
			},
		},
		{
			name: "LoaderTopicTest2",
			topics: []string{
				"loader-ned4ea-ts.inventory.address",
				"loader-ned4ea-ts.inventory.users",
				"loader-ned4ea-ts.inventory.abc_slugs_published",
				"loader-ned4ea-ts.inventory.abc_slugs",
				"loader-ned4ea-ts.inventory.provider_slugs",
				"loader-ned4ea-ts.inventory.provider_slugs2",
			},
			realtime: []string{},
			topicsLast: []topicLast{
				topicLast{
					topic: "ts.inventory.address",
					last:  94,
				},
				topicLast{
					topic: "ts.inventory.users",
					last:  187,
				},
				topicLast{
					topic: "ts.inventory.abc_slugs_published",
					last:  5198,
				},
				topicLast{
					topic: "ts.inventory.abc_slugs",
					last:  3776,
				},
				topicLast{
					topic: "ts.inventory.address",
					last:  110,
				},
				topicLast{
					topic: "ts.inventory.provider_slugs",
					last:  1650,
				},
				topicLast{
					topic: "ts.inventory.provider_slugs2",
					last:  1651,
				},
			},
			maxReloadingUnits: 2,
			currentReloadingTopics: []string{
				"loader-ned4ea-ts.inventory.address",
				"loader-ned4ea-ts.inventory.users",
				"loader-ned4ea-ts.inventory.abc_slugs_published",
				"loader-ned4ea-ts.inventory.abc_slugs",
			},
			units: []deploymentUnit{
				deploymentUnit{
					id:     "address",
					topics: []string{"loader-ned4ea-ts.inventory.address"},
				},
				deploymentUnit{
					id:     "users",
					topics: []string{"loader-ned4ea-ts.inventory.users"},
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
