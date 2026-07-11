package core

import (
	"sync/atomic"
	"time"
)

// BatchMaxOps is the maximum number of client puts packed into one Raft log
// entry. When set to 1 or less, batching is disabled and each put becomes its
// own log entry. Tunable via --batch-max-ops.
var BatchMaxOps = 1

// BatchMaxWait is how long the leader waits for a batch to fill before flushing
// a partial batch. Tunable via --batch-max-wait.
var BatchMaxWait = time.Millisecond

// KV is one key/value pair inside a batched put command.
type KV struct {
	Key   string
	Value string
}

type pendingPut struct {
	key   string
	value string
	done  chan string
}

// WriteBatcher collects concurrent leader-side puts into compound log entries.
// Followers still receive ordinary AppendEntries; only the leader batches.
type WriteBatcher struct {
	n          *Node
	submit     chan *pendingPut
	rejectAll  chan struct{}
	flushSize  atomic.Int64
	flushCount atomic.Int64
	bySize     atomic.Int64
	byTimer    atomic.Int64
	byForced   atomic.Int64
}

func newWriteBatcher(n *Node) *WriteBatcher {
	return &WriteBatcher{
		n:         n,
		submit:    make(chan *pendingPut, 4096),
		rejectAll: make(chan struct{}, 1),
	}
}

// Submit queues a put and blocks until the batch containing it is committed
// (or rejected because this node is no longer leader).
func (b *WriteBatcher) Submit(key, value string) string {
	p := &pendingPut{key: key, value: value, done: make(chan string, 1)}
	select {
	case b.submit <- p:
	default:
		// Channel full under extreme load: fall back to a single-entry commit
		// so clients are not blocked forever behind a stalled collector.
		if b.n.GetState() != Leader {
			return "Error: election"
		}
		b.n.Commit(NewCommand("put", key, value))
		return "success"
	}
	return <-p.done
}

// RejectPending wakes the collector to fail any buffered puts. Called when
// this node leaves the Leader role so clients get a fast error instead of
// waiting for a commit that will never happen.
func (b *WriteBatcher) RejectPending() {
	select {
	case b.rejectAll <- struct{}{}:
	default:
	}
}

func (b *WriteBatcher) AvgBatchSize() float64 {
	c := b.flushCount.Load()
	if c == 0 {
		return 0
	}
	return float64(b.flushSize.Load()) / float64(c)
}

func (b *WriteBatcher) FlushCounts() (size, timer, forced int64) {
	return b.bySize.Load(), b.byTimer.Load(), b.byForced.Load()
}

func (b *WriteBatcher) run() {
	var buf []*pendingPut
	timer := time.NewTimer(BatchMaxWait)
	if !timer.Stop() {
		<-timer.C
	}
	timerActive := false

	flush := func(reason string) {
		if len(buf) == 0 {
			return
		}
		puts := make([]KV, len(buf))
		for i, p := range buf {
			puts[i] = KV{Key: p.key, Value: p.value}
		}
		pending := buf
		buf = nil
		if timerActive {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timerActive = false
		}

		b.flushCount.Add(1)
		b.flushSize.Add(int64(len(pending)))
		batchFlushSize.WithLabelValues(b.n.Id).Add(float64(len(pending)))
		switch reason {
		case "size":
			b.bySize.Add(1)
			batchFlushTotal.WithLabelValues(b.n.Id, "size").Inc()
		case "timer":
			b.byTimer.Add(1)
			batchFlushTotal.WithLabelValues(b.n.Id, "timer").Inc()
		case "forced":
			b.byForced.Add(1)
			batchFlushTotal.WithLabelValues(b.n.Id, "forced").Inc()
		}

		if b.n.GetState() != Leader {
			for _, p := range pending {
				p.done <- "Error: election"
			}
			return
		}

		var cmd *Command
		if len(puts) == 1 {
			cmd = NewCommand("put", puts[0].Key, puts[0].Value)
		} else {
			cmd = NewBatchCommand(puts)
		}
		b.n.Commit(cmd)
		for _, p := range pending {
			p.done <- "success"
		}
	}

	reject := func() {
		for _, p := range buf {
			p.done <- "Error: election"
		}
		buf = nil
		if timerActive {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timerActive = false
		}
		// Drain any puts that arrived while we were rejecting.
		for {
			select {
			case p := <-b.submit:
				p.done <- "Error: election"
			default:
				return
			}
		}
	}

	for {
		select {
		case p := <-b.submit:
			if BatchMaxOps <= 1 {
				if b.n.GetState() != Leader {
					p.done <- "Error: election"
					continue
				}
				b.n.Commit(NewCommand("put", p.key, p.value))
				p.done <- "success"
				continue
			}
			if len(buf) == 0 {
				timer.Reset(BatchMaxWait)
				timerActive = true
			}
			buf = append(buf, p)
			if len(buf) >= BatchMaxOps {
				flush("size")
			}
		case <-timer.C:
			timerActive = false
			flush("timer")
		case <-b.rejectAll:
			reject()
		}
	}
}
