package timeutil_test

import (
	"testing"
	"time"

	"github.com/ivov/n8n-tracer/internal/timeutil"
	"github.com/stretchr/testify/assert"
)

func Test_ParseEventTime_ValidRFC3339(t *testing.T) {
	testCases := []struct {
		name      string
		timestamp string
		expected  time.Time
	}{
		{
			name:      "RFC3339 with Z timezone",
			timestamp: "2025-06-09T20:00:00.000Z",
			expected:  time.Date(2025, 6, 9, 20, 0, 0, 0, time.UTC),
		},
		{
			name:      "RFC3339 with timezone offset",
			timestamp: "2025-06-09T20:00:00.000+05:00",
			expected:  time.Date(2025, 6, 9, 20, 0, 0, 0, time.FixedZone("", 5*60*60)),
		},
		{
			name:      "RFC3339 with negative timezone offset",
			timestamp: "2025-06-09T20:00:00.000-08:00",
			expected:  time.Date(2025, 6, 9, 20, 0, 0, 0, time.FixedZone("", -8*60*60)),
		},
		{
			name:      "RFC3339 without milliseconds",
			timestamp: "2025-06-09T20:00:00Z",
			expected:  time.Date(2025, 6, 9, 20, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := timeutil.ParseEventTime(tc.timestamp)
			assert.Equal(t, tc.expected, result)
			assert.False(t, result.IsZero(), "Parsed time should not be zero")
		})
	}
}

func Test_ParseEventTime_InvalidFormats(t *testing.T) {
	testCases := []struct {
		name      string
		timestamp string
	}{
		{
			name:      "Empty string",
			timestamp: "",
		},
		{
			name:      "Invalid format - missing T",
			timestamp: "2025-06-09 20:00:00.000Z",
		},
		{
			name:      "Invalid format - missing timezone",
			timestamp: "2025-06-09T20:00:00.000",
		},
		{
			name:      "Invalid format - wrong date format",
			timestamp: "25-06-09T20:00:00.000Z",
		},
		{
			name:      "Invalid format - random string",
			timestamp: "not-a-timestamp",
		},
		{
			name:      "Invalid format - Unix timestamp",
			timestamp: "1654876800",
		},
		{
			name:      "Invalid format - ISO 8601 without colons",
			timestamp: "20250609T200000Z",
		},
		{
			name:      "Invalid month",
			timestamp: "2025-13-09T20:00:00.000Z",
		},
		{
			name:      "Invalid day",
			timestamp: "2025-06-32T20:00:00.000Z",
		},
		{
			name:      "Invalid hour",
			timestamp: "2025-06-09T25:00:00.000Z",
		},
		{
			name:      "Invalid minute",
			timestamp: "2025-06-09T20:60:00.000Z",
		},
		{
			name:      "Invalid second",
			timestamp: "2025-06-09T20:00:60.000Z",
		},
		{
			name:      "Invalid timezone format",
			timestamp: "2025-06-09T20:00:00.000+25:00",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := timeutil.ParseEventTime(tc.timestamp)
			assert.True(t, result.IsZero(), "Invalid timestamp should return zero time")
		})
	}
}
