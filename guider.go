package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
)

type Guider interface {
	Check(iter string, trace *Trace, eventTrace *EventTrace, record bool) (bool, int)
	Coverage() int
	Reset()
}

type TLCStateGuider struct {
	TLCAddr   string
	statesMap map[int64]bool
	tlcClient *TLCClient

	recordPath string
}

var _ Guider = &TLCStateGuider{}

func NewTLCStateGuider(tlcAddr, recordPath string) *TLCStateGuider {
	return &TLCStateGuider{
		TLCAddr:    tlcAddr,
		statesMap:  make(map[int64]bool),
		tlcClient:  NewTLCClient(tlcAddr),
		recordPath: recordPath,
	}
}

func (t *TLCStateGuider) Reset() {
	t.statesMap = make(map[int64]bool)

}

func (t *TLCStateGuider) Coverage() int {
	return len(t.statesMap)
}

func (t *TLCStateGuider) Check(iter string, trace *Trace, eventTrace *EventTrace, record bool) (bool, int) {

	numNewStates := 0
	if tlcStates, err := t.tlcClient.SendTrace(eventTrace); err == nil {
		if record {
			t.recordTrace(iter, trace, eventTrace, tlcStates)
		}
		for _, s := range tlcStates {
			_, ok := t.statesMap[s.Key]
			if !ok {
				numNewStates += 1
				t.statesMap[s.Key] = true
			}
		}
	} else {
		panic(fmt.Sprintf("error connecting to tlc: %s", err))
	}
	return numNewStates != 0, numNewStates
}

func (t *TLCStateGuider) recordTrace(as string, trace *Trace, eventTrace *EventTrace, states []TLCState) {

	filePath := path.Join(t.recordPath, as+".json")
	data := map[string]interface{}{
		"trace":       trace,
		"event_trace": eventTrace,
		"state_trace": parseTLCStateTrace(states),
	}
	dataB, err := json.MarshalIndent(data, "", "\t")
	if err != nil {
		return
	}
	file, err := os.Create(filePath)
	if err != nil {
		return
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	writer.Write(dataB)
	writer.Flush()
}

func parseTLCStateTrace(states []TLCState) []TLCState {
	newStates := make([]TLCState, len(states))
	for i, s := range states {
		repr := strings.ReplaceAll(s.Repr, "\n", ",")
		repr = strings.ReplaceAll(repr, "/\\", "")
		repr = strings.ReplaceAll(repr, "\u003e\u003e", "]")
		repr = strings.ReplaceAll(repr, "\u003c\u003c", "[")
		repr = strings.ReplaceAll(repr, "\u003e", ">")
		newStates[i] = TLCState{
			Repr: repr,
			Key:  s.Key,
		}
	}
	return newStates
}
