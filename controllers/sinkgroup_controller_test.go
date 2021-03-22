package controllers

import (
	"reflect"
	"testing"
)

func TestDeploymentUnitAssignment(t *testing.T) {
	// t.Parallel()

	tests := []struct {
		name        string
		allTopics   []string
		maxTopics   int
		resultUnits []deploymentUnit
	}{
		{
			name:      "single group",
			allTopics: []string{"t1", "t2", "t3", "t4", "t5"},
			maxTopics: 10,
			resultUnits: []deploymentUnit{
				deploymentUnit{
					id:     "0",
					topics: []string{"t1", "t2", "t3", "t4", "t5"},
				},
			},
		},
		{
			name:      "equal group",
			allTopics: []string{"t1", "t2", "t3", "t4", "t5"},
			maxTopics: 1,
			resultUnits: []deploymentUnit{
				deploymentUnit{
					id:     "0",
					topics: []string{"t1"},
				},
				deploymentUnit{
					id:     "1",
					topics: []string{"t2"},
				},
				deploymentUnit{
					id:     "2",
					topics: []string{"t3"},
				},
				deploymentUnit{
					id:     "3",
					topics: []string{"t4"},
				},
				deploymentUnit{
					id:     "4",
					topics: []string{"t5"},
				},
			},
		},
		{
			name:      "unequal group",
			allTopics: []string{"t1", "t2", "t3", "t4", "t5"},
			maxTopics: 3,
			resultUnits: []deploymentUnit{
				deploymentUnit{
					id:     "0",
					topics: []string{"t1", "t2", "t3"},
				},
				deploymentUnit{
					id:     "1",
					topics: []string{"t4", "t5"},
				},
			},
		},
		{
			name:      "equal group one more",
			allTopics: []string{"t1", "t2", "t3", "t4", "t5", "t6"},
			maxTopics: 2,
			resultUnits: []deploymentUnit{
				deploymentUnit{
					id:     "0",
					topics: []string{"t1", "t2"},
				},
				deploymentUnit{
					id:     "1",
					topics: []string{"t3", "t4"},
				},
				deploymentUnit{
					id:     "2",
					topics: []string{"t5", "t6"},
				},
			},
		},
		{
			name:      "unequal group",
			allTopics: []string{"t1", "t2", "t3", "t4", "t5", "t6"},
			maxTopics: 5,
			resultUnits: []deploymentUnit{
				deploymentUnit{
					id:     "0",
					topics: []string{"t1", "t2", "t3", "t4", "t5"},
				},
				deploymentUnit{
					id:     "1",
					topics: []string{"t6"},
				},
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			resultUnits := assignDeploymentUnits(tc.allTopics, tc.maxTopics)
			if !reflect.DeepEqual(tc.resultUnits, resultUnits) {
				t.Errorf("expected: %v, got: %v\n", tc.resultUnits, resultUnits)
			}
		})
	}
}
