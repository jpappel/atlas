package util

import "time"

func ParseDateTime(s string) (time.Time, error) {
	dateFormats := []string{
		"Jan _2, 2006",
		"January 2, 2006",
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
