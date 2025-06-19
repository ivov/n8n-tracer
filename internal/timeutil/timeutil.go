package timeutil

import "time"

func ParseEventTime(timestamp string) time.Time {
	eventTime, err := time.Parse(time.RFC3339, timestamp)
	if err != nil {
		return time.Time{} // zero time indicates parsing failure
	}

	return eventTime
}
