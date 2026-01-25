package utils

import "time"

func CurrentDateInTimezone(tz string) string {
	loc, err := time.LoadLocation(tz)
	if err != nil {
		loc = time.UTC
	}
	now := time.Now().In(loc)
	return now.Format("2006-01-02")
}

func CurrentTimeInTimezone(tz string) string {
	loc, err := time.LoadLocation(tz)
	if err != nil {
		loc = time.UTC
	}
	now := time.Now().In(loc)
	return now.Format("15:04")
}
