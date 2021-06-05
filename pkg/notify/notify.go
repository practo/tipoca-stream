package notify

import (
	"fmt"
	"github.com/slack-go/slack"
)

type Notifier interface {
	Notify(message string) error
}

type slackNotifier struct {
	client    *slack.Client
	channelID string
}

func New(token string, channelID string) Notifier {
	return &slackNotifier{
		client:    slack.New(token),
		channelID: channelID,
	}
}

func (s *slackNotifier) Notify(message string) error {
	_, _, err := s.client.PostMessage(
		s.channelID,
		slack.MsgOptionText(message, false),
		slack.MsgOptionAsUser(true),
	)
	if err != nil {
		return fmt.Errorf("Error sending slack message, err: %v", err)
	}

	return nil
}
