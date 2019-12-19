package common

import (
	"testing"
)

func TestCheckTopic(t *testing.T) {
	tests := []struct {
		name     string
		topic    string
		wildcard bool
		want     bool
	}{
		{name: "1", topic: "topic", wildcard: false, want: true},
		{name: "2", topic: "topic/a", wildcard: false, want: true},
		{name: "3", topic: "topic/a/b", wildcard: false, want: true},
		{name: "4", topic: "$baidu", wildcard: false, want: true},
		{name: "5", topic: "$link", wildcard: false, want: true},
		{name: "6", topic: "$baidu/services/a", wildcard: false, want: true},
		{name: "7", topic: "$link/services/a", wildcard: false, want: true},

		{name: "8", topic: "+", wildcard: false, want: false},
		{name: "9", topic: "#", wildcard: false, want: false},
		{name: "10", topic: "topic/+", wildcard: false, want: false},
		{name: "11", topic: "topic/#", wildcard: false, want: false},
		{name: "12", topic: "$SYS", wildcard: false, want: false},
		{name: "13", topic: "$SYS/services/a", wildcard: false, want: false},
		{name: "14", topic: "$SYS/+", wildcard: false, want: false},
		{name: "15", topic: "$SYS/#", wildcard: false, want: false},

		{name: "16", topic: "topic", wildcard: true, want: true},
		{name: "17", topic: "topic/a", wildcard: true, want: true},
		{name: "18", topic: "topic/a/b", wildcard: true, want: true},
		{name: "19", topic: "+", wildcard: true, want: true},
		{name: "20", topic: "#", wildcard: true, want: true},
		{name: "21", topic: "topic/+", wildcard: true, want: true},
		{name: "22", topic: "topic/#", wildcard: true, want: true},
		{name: "23", topic: "topic/+/b", wildcard: true, want: true},
		{name: "24", topic: "topic/a/+", wildcard: true, want: true},
		{name: "25", topic: "topic/a/#", wildcard: true, want: true},
		{name: "26", topic: "+/a/#", wildcard: true, want: true},
		{name: "27", topic: "+/+/#", wildcard: true, want: true},
		{name: "28", topic: "$baidu", wildcard: true, want: true},
		{name: "29", topic: "$link", wildcard: true, want: true},
		{name: "30", topic: "$baidu/+/a", wildcard: true, want: true},
		{name: "31", topic: "$baidu/+/#", wildcard: true, want: true},
		{name: "32", topic: "$baidu/services/a", wildcard: true, want: true},
		{name: "33", topic: "$link/+/a", wildcard: true, want: true},
		{name: "34", topic: "$link/+/#", wildcard: true, want: true},
		{name: "35", topic: "$link/services/a", wildcard: true, want: true},

		{name: "36", topic: "", wildcard: true, want: false},
		{name: "37", topic: "++", wildcard: true, want: false},
		{name: "38", topic: "##", wildcard: true, want: false},
		{name: "39", topic: "#/+", wildcard: true, want: false},
		{name: "40", topic: "#/#", wildcard: true, want: false},
		{name: "41", topic: "$SYS", wildcard: true, want: false},
		{name: "42", topic: "$SYS/+", wildcard: true, want: false},
		{name: "43", topic: "$SYS/#", wildcard: true, want: false},
		{name: "44", topic: "$SYS/services/a", wildcard: true, want: false},
		{name: "45", topic: "$+", wildcard: true, want: false},
		{name: "46", topic: "$#", wildcard: true, want: false},
		{name: "47", topic: "$+/a", wildcard: true, want: false},
		{name: "48", topic: "$#/a", wildcard: true, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CheckTopic(tt.topic, tt.wildcard); got != tt.want {
				t.Errorf("topic = %s CheckTopic() = %v, want %v", tt.topic, got, tt.want)
			}
		})
	}
}
