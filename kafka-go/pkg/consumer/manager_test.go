package consumer

import (
    "testing"
)

func TestTopicSync(t *testing.T) {
    topicRegexes := "^inventory.inventory.customers, ^dbserver.database.table"
    allTopics := []string{"dbserver.database"}

    c := NewManager(
        nil,
        topicRegexes,
    )
    c.updatetopics(allTopics)

    expectedTopics := []string{
        ""
    }
}
