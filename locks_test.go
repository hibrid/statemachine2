package statemachine

import (
	"testing"
	"time"
)

func TestIsWithinRecurringSchedule(t *testing.T) {
	// Test setup
	layout := "2006-01-02 15:04:05"
	startTime, _ := time.Parse(layout, "2023-01-01 09:00:00")
	endTime, _ := time.Parse(layout, "2023-01-01 17:00:00")

	tests := []struct {
		name       string
		interval   string
		dayOfWeek  int
		dayOfMonth int
		queryTime  time.Time
		want       bool
	}{
		{
			name:      "Daily within interval",
			interval:  "Daily",
			queryTime: time.Date(2023, 1, 1, 10, 0, 0, 0, time.UTC),
			want:      true,
		},
		{
			name:      "Daily outside interval",
			interval:  "Daily",
			queryTime: time.Date(2023, 1, 1, 18, 0, 0, 0, time.UTC),
			want:      false,
		},
		{
			name:      "Weekly on correct day",
			interval:  "Weekly",
			dayOfWeek: int(time.Sunday),
			queryTime: time.Date(2023, 1, 1, 10, 0, 0, 0, time.UTC), // Sunday
			want:      true,
		},
		{
			name:      "Weekly on incorrect day",
			interval:  "Weekly",
			dayOfWeek: int(time.Monday),
			queryTime: time.Date(2023, 1, 1, 10, 0, 0, 0, time.UTC), // Sunday
			want:      false,
		},
		{
			name:       "Monthly on correct day",
			interval:   "Monthly",
			dayOfMonth: 1,
			queryTime:  time.Date(2023, 1, 1, 10, 0, 0, 0, time.UTC), // 1st of the month
			want:       true,
		},
		{
			name:       "Monthly on incorrect day",
			interval:   "Monthly",
			dayOfMonth: 2,
			queryTime:  time.Date(2023, 1, 1, 10, 0, 0, 0, time.UTC), // 1st of the month
			want:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isWithinRecurringSchedule(tt.interval, tt.dayOfWeek, tt.dayOfMonth, startTime, endTime, tt.queryTime); got != tt.want {
				t.Errorf("isWithinRecurringSchedule() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsTimeWithinInterval(t *testing.T) {
	// Test setup
	layout := "15:04:05"
	startTime, _ := time.Parse(layout, "09:00:00")
	endTime, _ := time.Parse(layout, "17:00:00")

	tests := []struct {
		name        string
		currentTime time.Time
		want        bool
	}{
		{
			name:        "Within interval",
			currentTime: time.Date(2023, 1, 1, 10, 0, 0, 0, time.UTC),
			want:        true,
		},
		{
			name:        "At start time",
			currentTime: time.Date(2023, 1, 1, 9, 0, 0, 0, time.UTC),
			want:        false, // exactly at start time is not considered within
		},
		{
			name:        "At end time",
			currentTime: time.Date(2023, 1, 1, 17, 0, 0, 0, time.UTC),
			want:        false, // exactly at end time is not considered within
		},
		{
			name:        "Before interval",
			currentTime: time.Date(2023, 1, 1, 8, 59, 59, 0, time.UTC),
			want:        false,
		},
		{
			name:        "After interval",
			currentTime: time.Date(2023, 1, 1, 17, 0, 1, 0, time.UTC),
			want:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isTimeWithinInterval(startTime, endTime, tt.currentTime); got != tt.want {
				t.Errorf("isTimeWithinInterval() = %v, want %v", got, tt.want)
			}
		})
	}
}
