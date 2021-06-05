package git

import (
	"reflect"
	"testing"
)

func TestParseGithubURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		url    string
		result []string
	}{
		{
			name: "url test1",
			url:  "/practo/tipoca-stream/pkg/README.md",
			result: []string{
				"practo/tipoca-stream",
				"pkg/README.md",
			},
		},
		{
			name: "url test2",
			url:  "/practo/tipoca-stream/redshiftsink/pkg/transformer/masker/database.yaml",
			result: []string{
				"practo/tipoca-stream",
				"redshiftsink/pkg/transformer/masker/database.yaml",
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			repo, filePath := ParseGithubURL(tc.url)
			gotResult := []string{repo, filePath}

			if !reflect.DeepEqual(tc.result, gotResult) {
				t.Errorf("expected: %v, got: %v\n", tc.result, gotResult)
			}
		})
	}
}
