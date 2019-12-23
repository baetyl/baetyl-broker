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
		{name: "8", topic: "$SYS", wildcard: false, want: true},
		{name: "9", topic: "$SYS/services/a", wildcard: true, want: true},

		{name: "10", topic: "+", wildcard: false, want: false},
		{name: "11", topic: "#", wildcard: false, want: false},
		{name: "12", topic: "topic/+", wildcard: false, want: false},
		{name: "13", topic: "topic/#", wildcard: false, want: false},
		{name: "14", topic: "$SYS/+", wildcard: false, want: false},
		{name: "15", topic: "$SYS/#", wildcard: false, want: false},
		{name: "16", topic: "$baidu/+", wildcard: false, want: false},
		{name: "17", topic: "$baidu/#", wildcard: false, want: false},
		{name: "18", topic: "$link/+", wildcard: false, want: false},
		{name: "19", topic: "$link/#", wildcard: false, want: false},

		{name: "20", topic: "topic", wildcard: true, want: true},
		{name: "21", topic: "topic/a", wildcard: true, want: true},
		{name: "22", topic: "topic/a/b", wildcard: true, want: true},
		{name: "23", topic: "+", wildcard: true, want: true},
		{name: "24", topic: "#", wildcard: true, want: true},
		{name: "25", topic: "topic/+", wildcard: true, want: true},
		{name: "26", topic: "topic/#", wildcard: true, want: true},
		{name: "27", topic: "topic/+/b", wildcard: true, want: true},
		{name: "28", topic: "topic/a/+", wildcard: true, want: true},
		{name: "29", topic: "topic/a/#", wildcard: true, want: true},
		{name: "30", topic: "+/a/#", wildcard: true, want: true},
		{name: "31", topic: "+/+/#", wildcard: true, want: true},
		{name: "32", topic: "$baidu", wildcard: true, want: true},
		{name: "33", topic: "$link", wildcard: true, want: true},
		{name: "34", topic: "$baidu/+/a", wildcard: true, want: true},
		{name: "35", topic: "$baidu/+/#", wildcard: true, want: true},
		{name: "36", topic: "$baidu/services/a", wildcard: true, want: true},
		{name: "37", topic: "$link/+/a", wildcard: true, want: true},
		{name: "38", topic: "$link/+/#", wildcard: true, want: true},
		{name: "39", topic: "$link/services/a", wildcard: true, want: true},
		{name: "40", topic: "$SYS", wildcard: true, want: true},
		{name: "41", topic: "$SYS/+", wildcard: true, want: true},
		{name: "42", topic: "$SYS/#", wildcard: true, want: true},
		{name: "43", topic: "$SYS/services/a", wildcard: true, want: true},

		{name: "44", topic: "", wildcard: true, want: false},
		{name: "45", topic: "++", wildcard: true, want: false},
		{name: "46", topic: "##", wildcard: true, want: false},
		{name: "47", topic: "#/+", wildcard: true, want: false},
		{name: "48", topic: "#/#", wildcard: true, want: false},
		{name: "49", topic: "$+", wildcard: true, want: false},
		{name: "50", topic: "$#", wildcard: true, want: false},
		{name: "51", topic: "$+/a", wildcard: true, want: false},
		{name: "52", topic: "$#/a", wildcard: true, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CheckTopic(tt.topic, tt.wildcard); got != tt.want {
				t.Errorf("topic = %s CheckTopic() = %v, want %v", tt.topic, got, tt.want)
			}
		})
	}
}
