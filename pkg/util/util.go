package util

import (
	"iter"
	"time"
)

func ParseDateTime(s string) (time.Time, error) {
	dateFormats := []string{
		"Jan _2, 2006",
		"January 2, 2006",
		"January 2 2006",
		"Jan 2 2006",
		"2006 January 2",
		time.DateOnly,
		time.DateTime,
		time.Layout,
		time.ANSIC,
		time.UnixDate,
		time.RubyDate,
		time.RFC822,
		time.RFC822Z,
		time.RFC850,
		time.RFC1123,
		time.RFC1123Z,
		time.RFC3339,
	}

	var t time.Time
	var err error
	for _, layout := range dateFormats {
		if t, err = time.Parse(layout, s); err == nil {
			return t, nil
		}
	}

	return time.Time{}, err
}

// Estimate an interval around a time which is still "meaningful"
//
// Ex: 2025-06-14 -> [2025-06-10, 2025-06-18]
// Ex: 2025-06-14T12:00 -> [2025-06-14T8:00, 2025-06-14T16:00]
func FuzzDatetime(t time.Time) (start time.Time, stop time.Time) {
	hour, minute, sec := t.Clock()
	_, month, day := t.Date()

	var d time.Duration
	if sec != 0 {
		d = 5 * time.Minute
	} else if minute != 0 {
		d = 30 * time.Minute
	} else if hour != 0 {
		d = 4 * time.Hour
	} else if day != 1 {
		d = 84 * time.Hour // +- 3.5 days
	} else if month != time.January {
		d = 336 * time.Hour // +- .5 months
	} else {
		d = 4380 * time.Hour // search +- 6months
	}

	return t.Add(-d), t.Add(d)
}

// Create a copy of a slice with all values that satisfy cond
func Fitler[E any](s []E, cond func(e E) bool) []E {
	filtered := make([]E, 0, len(s))
	for _, e := range s {
		if cond(e) {
			filtered = append(filtered, e)
		}
	}

	return filtered
}

// Create an iterator of index and element for all values in a slice which satisfy cond.
func FilterIter[E any](s []E, cond func(e E) bool) iter.Seq2[int, E] {
	return func(yield func(int, E) bool) {
		for i, e := range s {
			if cond(e) {
				if !yield(i, e) {
					return
				}
			}
		}
	}
}

// FilterIter but backwards
func BackwardsFilterIter[E any](s []E, cond func(e E) bool) iter.Seq2[int, E] {
	return func(yield func(int, E) bool) {
		for i := len(s) - 1; i >= 0; i-- {
			if cond(s[i]) {
				if !yield(i, s[i]) {
					return
				}
			}
		}
	}
}
