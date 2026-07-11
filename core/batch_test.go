package core

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestBatchCommandRoundTrip(t *testing.T) {
	cmd := NewBatchCommand([]KV{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}})
	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Command
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Op != "batch" || len(decoded.Puts) != 2 {
		t.Fatalf("decoded %+v", decoded)
	}
	if decoded.Puts[0].Key != "a" || decoded.Puts[1].Value != "2" {
		t.Fatalf("puts %+v", decoded.Puts)
	}
}

func TestApplyBatchEntry(t *testing.T) {
	n := NewNode("n1", map[string]string{"n1": "127.0.0.1:19001"})
	n.SetState(Leader)
	n.Term.Store(1)
	n.AppendLog(NewBatchCommand([]KV{{Key: "k1", Value: "v1"}, {Key: "k2", Value: "v2"}}))
	n.CommitIndex.Store(0)
	n.ApplyCommitted()
	if got := n.Storage.Get("k1"); got != "v1" {
		t.Fatalf("k1=%q", got)
	}
	if got := n.Storage.Get("k2"); got != "v2" {
		t.Fatalf("k2=%q", got)
	}
	if n.GetLogSize() != 1 {
		t.Fatalf("expected 1 log entry, got %d", n.GetLogSize())
	}
}

func advanceCommits(n *Node, stop <-chan struct{}) {
	for {
		select {
		case <-stop:
			return
		default:
			last := int64(n.GetLogSize() - 1)
			for id := range n.MatchIndex {
				n.MatchIndex[id].Store(last)
			}
			n.UpdateCommitIndex()
			time.Sleep(time.Millisecond)
		}
	}
}

func TestBatchFlushOnSize(t *testing.T) {
	prevOps, prevWait := BatchMaxOps, BatchMaxWait
	BatchMaxOps = 4
	BatchMaxWait = time.Hour
	defer func() {
		BatchMaxOps = prevOps
		BatchMaxWait = prevWait
	}()

	n := NewNode("n1", map[string]string{"n1": "127.0.0.1:19002"})
	n.SetState(Leader)
	n.Term.Store(1)
	go n.batcher.run()
	stop := make(chan struct{})
	go advanceCommits(n, stop)
	defer close(stop)

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got := n.HandleCommand(NewCommand("put", fmt.Sprintf("k%d", i), "v"))
			if got != "success" {
				t.Errorf("put %d: %s", i, got)
			}
		}(i)
	}
	wg.Wait()

	entries := n.GetLogSize()
	if entries >= 4 {
		t.Fatalf("expected batching to reduce log entries below 4, got %d", entries)
	}
	if avg := n.batcher.AvgBatchSize(); avg < 1.5 {
		t.Fatalf("avg batch size %.2f, want > 1.5", avg)
	}
	for i := 0; i < 4; i++ {
		if got := n.Storage.Get(fmt.Sprintf("k%d", i)); got != "v" {
			t.Fatalf("k%d=%q", i, got)
		}
	}
}

func TestBatchFlushOnTimer(t *testing.T) {
	// With drain-and-flush policy there is no timer wait. A single put under
	// BatchMaxOps>1 still commits as its own entry immediately.
	prevOps, prevWait := BatchMaxOps, BatchMaxWait
	BatchMaxOps = 64
	BatchMaxWait = time.Hour
	defer func() {
		BatchMaxOps = prevOps
		BatchMaxWait = prevWait
	}()

	n := NewNode("n1", map[string]string{"n1": "127.0.0.1:19003"})
	n.SetState(Leader)
	n.Term.Store(1)
	go n.batcher.run()
	stop := make(chan struct{})
	go advanceCommits(n, stop)
	defer close(stop)

	got := n.HandleCommand(NewCommand("put", "solo", "v"))
	if got != "success" {
		t.Fatalf("got %s", got)
	}
	if n.GetLogSize() != 1 {
		t.Fatalf("expected 1 entry for a lone put, got %d", n.GetLogSize())
	}
	if got := n.Storage.Get("solo"); got != "v" {
		t.Fatalf("solo=%q", got)
	}
}

func TestBatchRejectOnStepDown(t *testing.T) {
	prevOps, prevWait := BatchMaxOps, BatchMaxWait
	BatchMaxOps = 64
	BatchMaxWait = time.Hour
	defer func() {
		BatchMaxOps = prevOps
		BatchMaxWait = prevWait
	}()

	n := NewNode("n1", map[string]string{"n1": "127.0.0.1:19004"})
	n.SetState(Leader)
	n.Term.Store(1)
	go n.batcher.run()
	// Deliberately do NOT advance commits: the first put blocks inside Commit,
	// and later puts sit in the submit channel until RejectPending drains them.

	firstStarted := make(chan struct{})
	go func() {
		close(firstStarted)
		_ = n.HandleCommand(NewCommand("put", "blocking", "v"))
	}()
	<-firstStarted
	time.Sleep(30 * time.Millisecond)

	queued := make(chan string, 2)
	for i := 0; i < 2; i++ {
		go func(i int) {
			queued <- n.HandleCommand(NewCommand("put", fmt.Sprintf("q%d", i), "v"))
		}(i)
	}
	time.Sleep(30 * time.Millisecond)
	n.SetState(Follower)

	for i := 0; i < 2; i++ {
		select {
		case got := <-queued:
			if got != "Error: election" {
				t.Fatalf("queued put: got %q, want Error: election", got)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for queued put reject")
		}
	}
	// Unblock the in-flight Commit so the deferred BatchMaxOps restore does not
	// race with HandleCommand still reading the package vars.
	last := int64(n.GetLogSize() - 1)
	for id := range n.MatchIndex {
		n.MatchIndex[id].Store(last)
	}
	n.UpdateCommitIndex()
	time.Sleep(20 * time.Millisecond)
}

func TestBatchDisabledBypass(t *testing.T) {
	prevOps := BatchMaxOps
	BatchMaxOps = 1
	defer func() { BatchMaxOps = prevOps }()

	n := NewNode("n1", map[string]string{"n1": "127.0.0.1:19005"})
	n.SetState(Leader)
	n.Term.Store(1)
	stop := make(chan struct{})
	go advanceCommits(n, stop)
	defer close(stop)

	for i := 0; i < 3; i++ {
		if got := n.HandleCommand(NewCommand("put", fmt.Sprintf("k%d", i), "v")); got != "success" {
			t.Fatalf("put %d: %s", i, got)
		}
	}
	if n.GetLogSize() != 3 {
		t.Fatalf("expected 3 entries with batching disabled, got %d", n.GetLogSize())
	}
}
