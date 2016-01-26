package filter

import (
	"encoding/json"
)

type eventMessage struct {
	Metric  string `json:"metric"`
	Pattern string `json:"pattern"`
}

func makeEvent(pattern string, metric string) ([]byte, error) {

	event := &eventMessage{
		Metric:  metric,
		Pattern: pattern,
	}

	return json.Marshal(event)
}
