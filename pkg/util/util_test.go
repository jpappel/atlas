package util_test

import (
	"github.com/jpappel/atlas/pkg/util"
	"testing"
)

func TestLevensteinDistance(t *testing.T) {
	tests := []struct {
		s    string
		t    string
		want int
	}{
		{"sitting", "kitten", 3},
		{"Saturday", "Sunday", 3},
		{"hello", "kelm", 3},
	}
	for _, tt := range tests {
		t.Run(tt.s+" "+tt.t, func(t *testing.T) {
			got := util.LevensteinDistance(tt.s, tt.t)
			if got != tt.want {
				t.Errorf("LevensteinDistance() = %v, want %v", got, tt.want)
			}
		})
	}
}
