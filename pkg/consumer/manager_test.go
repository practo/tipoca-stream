package consumer

import (
	"context"
	"reflect"
	"testing"
)

var tcancelFunc context.CancelFunc

func testTopicRegex(t *testing.T, regexes string,
	allTopics []string, expectedTopics []string) {

	c := NewManager(nil, "cg01", regexes, tcancelFunc, true)
	c.updatetopics(allTopics)

	topics := c.deepCopyTopics()

	if !reflect.DeepEqual(expectedTopics, topics) {
		t.Errorf("expectedTopics: %v, got: %v\n", expectedTopics, c.topics)
	}
}

func TestTopicRegex(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		topicRegexes   string
		allTopics      []string
		expectedTopics []string
	}{
		{
			name:           "test with no topics and no regexes",
			topicRegexes:   "",
			allTopics:      []string{},
			expectedTopics: []string{},
		},
		{
			name:           "test with no topics and no regexes",
			topicRegexes:   "^db.*",
			allTopics:      []string{},
			expectedTopics: []string{},
		},
		{
			name:           "test with no topics and no regexes",
			topicRegexes:   "",
			allTopics:      []string{"db.topic1"},
			expectedTopics: []string{"db.topic1"},
		},
		{
			name:           "test with no topics and no regexes",
			topicRegexes:   "^db\\.to",
			allTopics:      []string{"db.topic1", "dbtopic.alok"},
			expectedTopics: []string{"db.topic1"},
		},
		{
			name:           "test with no topics and no regexes",
			topicRegexes:   "^db\\.topic11",
			allTopics:      []string{"db.topic1", "dbtopic.alok"},
			expectedTopics: []string{},
		},
		{
			name:           "test with no topics and no regexes",
			topicRegexes:   "^db\\.topic1,^db\\.topic3\\.",
			allTopics:      []string{"db.topic1", "db.topic2", "db.topic3.t3"},
			expectedTopics: []string{"db.topic1", "db.topic3.t3"},
		},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			testTopicRegex(t, tc.topicRegexes, tc.allTopics, tc.expectedTopics)
		})
	}
}

func testSync(t *testing.T, c *Manager, allTopics []string, expected []string) {
	c.updatetopics(allTopics)
	if !reflect.DeepEqual(expected, c.topics) {
		t.Errorf("expectedTopics: %v, got: %v\n", expected, c.topics)
	}
}

func TestTopicSync(t *testing.T) {
	c := NewManager(nil, "cg01", "db.*", tcancelFunc, true)

	// nothing
	allTopics := []string{}
	expected := []string{}
	testSync(t, c, allTopics, expected)

	// additions and duplicates
	allTopics = []string{"db.topic1", "db.topic2", "db.topic2"}
	expected = []string{"db.topic1", "db.topic2"}
	testSync(t, c, allTopics, expected)
	allTopics = append(allTopics, "db.topic3")
	testSync(t, c, allTopics, append(expected, "db.topic3"))

	// removals
	allTopics = allTopics[:len(allTopics)-1]
	expected = []string{"db.topic1", "db.topic2"}
	testSync(t, c, allTopics, expected)
}
