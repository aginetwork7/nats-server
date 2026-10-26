// Copyright 2019-2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !skip_js_tests

package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestMaxDeliveryCount(t *testing.T) {
	for _, test := range []struct {
		name      string
		rdc       map[uint64]uint64
		wantCount uint64
		wantSeq   uint64
	}{
		{"nil", nil, 0, 0},
		{"empty", map[uint64]uint64{}, 0, 0},
		{"single", map[uint64]uint64{7: 1}, 2, 7},
		{"highest wins", map[uint64]uint64{3: 1, 9: 5, 12: 2}, 6, 9},
		{"tie picks lowest seq", map[uint64]uint64{20: 4, 11: 4, 15: 4}, 5, 11},
	} {
		t.Run(test.name, func(t *testing.T) {
			count, seq := maxDeliveryCount(test.rdc)
			require_Equal(t, count, test.wantCount)
			require_Equal(t, seq, test.wantSeq)
		})
	}
}

func TestJetStreamConsumerInfoMaxDelivery(t *testing.T) {
	s := RunBasicJetStreamServer(t)
	defer s.Shutdown()

	mset, err := s.GlobalAccount().addStream(&StreamConfig{Name: "MD", Storage: MemoryStorage})
	require_NoError(t, err)
	defer mset.delete()

	nc, js := jsClientConnect(t, s)
	defer nc.Close()

	for range 2 {
		_, err = js.Publish("MD", []byte("OK!"))
		require_NoError(t, err)
	}
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if state := mset.state(); state.Msgs != 2 {
			return errors.New("expected 2 messages")
		}
		return nil
	})

	o, err := mset.addConsumer(workerModeConfig("WQ"))
	require_NoError(t, err)
	defer o.delete()

	// Nothing redelivered yet.
	ci := o.info()
	require_Equal(t, ci.NumRedelivered, 0)
	require_Equal(t, ci.MaxDeliveryCount, 0)
	require_Equal(t, ci.MaxDeliveryStreamSeq, 0)

	// Get the first message, nak it, and keep getting it back. It must be
	// message 1 each time since naked messages are redelivered first.
	for want := uint64(1); want <= 4; want++ {
		m, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
		require_NoError(t, err)
		sseq, _, dcount, _, _ := ackReplyInfo(m.Reply)
		require_Equal(t, sseq, 1)
		require_Equal(t, dcount, want)
		require_NoError(t, m.Respond(AckNak))
		require_NoError(t, nc.Flush())
	}

	// Four deliveries of message 1 so far, and it is the only one redelivered.
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		ci := o.info()
		if ci.NumRedelivered != 1 || ci.MaxDeliveryCount != 4 || ci.MaxDeliveryStreamSeq != 1 {
			return fmt.Errorf("unexpected info: redelivered=%d count=%d seq=%d",
				ci.NumRedelivered, ci.MaxDeliveryCount, ci.MaxDeliveryStreamSeq)
		}
		return nil
	})

	// The fields must be visible in the JSON that /jsz and the consumer info API return.
	var raw map[string]any
	data, err := json.Marshal(o.info())
	require_NoError(t, err)
	require_NoError(t, json.Unmarshal(data, &raw))
	require_Equal(t, raw["max_delivery_count"].(float64), 4)
	require_Equal(t, raw["max_delivery_stream_seq"].(float64), 1)

	// Acking the message clears the tracking, so the fields go back to zero
	// and are omitted from the JSON.
	m, err := nc.Request(o.requestNextMsgSubject(), nil, time.Second)
	require_NoError(t, err)
	require_NoError(t, m.AckSync())
	checkFor(t, time.Second, 10*time.Millisecond, func() error {
		if ci := o.info(); ci.MaxDeliveryCount != 0 || ci.MaxDeliveryStreamSeq != 0 {
			return fmt.Errorf("expected zero after ack, got count=%d seq=%d", ci.MaxDeliveryCount, ci.MaxDeliveryStreamSeq)
		}
		return nil
	})
	data, err = json.Marshal(o.info())
	require_NoError(t, err)
	raw = nil
	require_NoError(t, json.Unmarshal(data, &raw))
	_, has := raw["max_delivery_count"]
	require_False(t, has)
}
