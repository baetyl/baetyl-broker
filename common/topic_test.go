package common

import "testing"

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
		{name: "4", topic: "$SYS", wildcard: false, want: true},
		{name: "5", topic: "$SYS/services/a", wildcard: false, want: true},
		{name: "6", topic: "+", wildcard: false, want: false},
		{name: "7", topic: "#", wildcard: false, want: false},
		{name: "8", topic: "topic/+", wildcard: false, want: false},
		{name: "9", topic: "topic/#", wildcard: false, want: false},
		{name: "10", topic: "$SYS/+", wildcard: false, want: false},
		{name: "11", topic: "$SYS/#", wildcard: false, want: false},

		{name: "12", topic: "topic", wildcard: true, want: true},
		{name: "13", topic: "topic/a", wildcard: true, want: true},
		{name: "14", topic: "topic/a/b", wildcard: true, want: true},
		{name: "15", topic: "+", wildcard: true, want: true},
		{name: "16", topic: "#", wildcard: true, want: true},
		{name: "17", topic: "topic/+", wildcard: true, want: true},
		{name: "18", topic: "topic/#", wildcard: true, want: true},
		{name: "19", topic: "topic/+/b", wildcard: true, want: true},
		{name: "20", topic: "topic/a/+", wildcard: true, want: true},
		{name: "21", topic: "topic/a/#", wildcard: true, want: true},
		{name: "22", topic: "+/a/#", wildcard: true, want: true},
		{name: "23", topic: "+/+/#", wildcard: true, want: true},
		{name: "24", topic: "$SYS", wildcard: true, want: true},
		{name: "25", topic: "$SYS/+", wildcard: true, want: true},
		{name: "26", topic: "$SYS/#", wildcard: true, want: true},
		{name: "27", topic: "$SYS/services/a", wildcard: true, want: true},

		{name: "1", topic: "", wildcard: true, want: false},
		{name: "1", topic: "++", wildcard: true, want: false},
		{name: "1", topic: "##", wildcard: true, want: false},
		{name: "1", topic: "#/+", wildcard: true, want: false},
		{name: "1", topic: "#/#", wildcard: true, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CheckTopic(tt.topic, tt.wildcard); got != tt.want {
				t.Errorf("CheckTopic() = %v, want %v", got, tt.want)
			}
		})
	}
}
