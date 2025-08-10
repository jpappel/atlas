package util

import (
	"iter"
	"math"
	"strings"
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

// A Levenshtein distance implementation based off of
//
// https://en.wikipedia.org/wiki/Levenshtein_distance#Iterative_with_full_matrix
// PERF: more performant implementations exist
func LevensteinDistance(s, t string) int {
	m, n := len(s), len(t)
	d := make([][]int, m+1)
	for i := range m + 1 {
		d[i] = make([]int, n+1)
	}

	for i := range m {
		d[i+1][0] = i
	}
	for j := range n {
		d[0][j+1] = j
	}

	var subCost int
	for j := range n {
		for i := range m {
			if s[i] == t[j] {
				subCost = 0
			} else {
				subCost = 1
			}

			del := d[i][j+1] + 1
			insert := d[i+1][j] + 1
			sub := d[i][j] + subCost
			d[i+1][j+1] = min(del, insert, sub)
		}
	}

	return d[m][n]
}

// Find nearest element of a slice using cmp, returns the found element and
// if the distance is below ceil
func Nearest[E any](candidate E, valid []E, cmp func(E, E) int, ceil int) (E, bool) {
	minDistance := math.MaxInt
	minIdx := -1
	var d int
	for i, e := range valid {
		if sd := cmp(candidate, e); sd < 0 {
			d = -sd
		} else {
			d = sd
		}
		if d < minDistance {
			minDistance = d
			minIdx = i
		}
	}

	if minIdx < 0 {
		return candidate, false
	}
	return valid[minIdx], minDistance < ceil
}

// Check if substr[left:right] is a substring of S.
// If left > len(substr) use 0
// If right < 0 use 0
func ContainsSliced(s, substr string, left, right int) bool {
	return strings.Contains(s, substr[min(left, len(substr)):max(right, 0)])
}
