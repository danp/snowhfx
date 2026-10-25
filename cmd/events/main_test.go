package main

import (
	"testing"
	"time"
)

func TestParseUpdateTime(t *testing.T) {
	loc, err := time.LoadLocation("America/Halifax")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, text, observed, want string
	}{
		{"earlier today", "7 AM", "2026-02-10 08:00", "2026-02-10 07:00"},
		{"previous day", "9 AM", "2026-02-10 08:00", "2026-02-09 09:00"},
		{"month boundary", "11 p.m.", "2026-03-01 08:00", "2026-02-28 23:00"},
		{"year boundary", "9 AM", "2026-01-01 08:00", "2025-12-31 09:00"},
		{"equal time", "8 AM", "2026-01-01 08:00", "2026-01-01 08:00"},
		{"day after DST starts", "11 p.m.", "2026-03-09 08:00", "2026-03-08 23:00"},
		{"day after DST ends", "11 p.m.", "2026-11-02 08:00", "2026-11-01 23:00"},
		{"dated update", "Feb. 9 | 11 p.m.", "2026-02-10 08:00", "2026-02-09 23:00"},
		{"dated previous year", "Dec. 31 | 11 p.m.", "2026-01-01 08:00", "2025-12-31 23:00"},
		{"date only", "Monday Jan 5", "2026-01-06 08:30", "2026-01-05 08:30"},
		{"explicit year", "2/9/2025 11:00 PM", "2026-02-10 08:00", "2025-02-09 23:00"},
		{"noon", "Feb. 9 at noon", "2026-02-10 08:00", "2026-02-09 12:00"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parse := func(s string) time.Time {
				t.Helper()
				v, err := time.ParseInLocation("2006-01-02 15:04", s, loc)
				if err != nil {
					t.Fatal(err)
				}
				return v
			}
			got, ok := parseUpdateTime(tc.text, parse(tc.observed))
			want := parse(tc.want)
			if !ok || !got.Equal(want) {
				t.Fatalf("parseUpdateTime(%q) = %v, %v; want %v, true", tc.text, got, ok, want)
			}
		})
	}
}
