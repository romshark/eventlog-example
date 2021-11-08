package event

import (
	"encoding/json"
	"fmt"

	"github.com/romshark/eventlog/client"
)

type Event struct {
	Operation string `json:"-"`
	Object    string `json:"object"`
	Quantity  int64  `json:"quantity"`
}

func Decode(i client.Event) (e Event, err error) {
	switch string(i.Label) {
	case "put", "take":
		e.Operation = string(i.Label)
	default:
		return Event{}, fmt.Errorf("unknown event type: %q", i.Label)
	}
	err = json.Unmarshal(i.PayloadJSON, &e)
	return
}

func Encode(i Event) (e client.EventData, err error) {
	switch i.Operation {
	case "put", "take":
	default:
		return client.EventData{}, fmt.Errorf("unknown event type: %#v", i)
	}
	if e.PayloadJSON, err = json.Marshal(i); err != nil {
		return
	}
	e.Label = []byte(i.Operation)
	return
}
