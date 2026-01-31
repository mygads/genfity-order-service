package handlers

import (
	"sort"
	"testing"
)

func TestFeedbackSentiment(t *testing.T) {
	cases := []struct {
		name     string
		comment  string
		rating   int
		expected string
	}{
		{
			name:     "positive with strong rating",
			comment:  "Great service and fast delivery",
			rating:   5,
			expected: "positive",
		},
		{
			name:     "negative due to keywords",
			comment:  "Slow and cold food",
			rating:   5,
			expected: "negative",
		},
		{
			name:     "negative due to low rating",
			comment:  "Not good",
			rating:   2,
			expected: "negative",
		},
		{
			name:     "neutral when mixed sentiment",
			comment:  "Great taste but slow delivery",
			rating:   4,
			expected: "negative",
		},
		{
			name:     "neutral fallback",
			comment:  "Average experience",
			rating:   3,
			expected: "neutral",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := feedbackSentiment(tc.comment, tc.rating); got != tc.expected {
				t.Fatalf("expected %s, got %s", tc.expected, got)
			}
		})
	}
}

func TestFeedbackTags(t *testing.T) {
	comment := "Friendly staff and fast delivery, tasty food with clean packaging"
	got := feedbackTags(comment)
	expected := []string{"service", "speed", "delivery", "food", "packaging", "cleanliness"}

	sort.Strings(got)
	sort.Strings(expected)

	if len(got) != len(expected) {
		t.Fatalf("expected %d tags, got %d", len(expected), len(got))
	}
	for i, value := range expected {
		if got[i] != value {
			t.Fatalf("expected tag %s, got %s", value, got[i])
		}
	}

	empty := feedbackTags("")
	if len(empty) != 0 {
		t.Fatalf("expected no tags for empty comment")
	}
}
