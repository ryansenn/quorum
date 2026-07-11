package core

import (
	"sync/atomic"
	"time"
)

// BatchMaxOps is the maximum number of client puts packed into one Raft log
// entry. When set to 1 or less, batching is disabled and each put becomes its
// own log entry. Tunable via --batch-max-ops.
var BatchMaxOps = 1

// BatchMaxWait is retained as a CLI knob for experiment harnesses. The
// collector flushes as soon as it has drained the submit queue (closed-loop
// clients cannot fill a timer wait without adding latency), so this value is
// currently unused by the flush policy.
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
// Followers still receive ordinary AppendEntries. Only the leader batches.
//
// Flush policy: on each arrival, non-blocking-drain the submit channel up to
// BatchMaxOps, then flush immediately. Concurrent closed-loop clients pile up
// in the channel while the previous Commit runs, so the next drain naturally
// forms a large batch without a timer. A timer wait would only hurt conc=1.
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
		if b.n.GetState() != Leader {
			return "Error: election"
		}
		b.n.Commit(NewCommand("put", key, value))
		return "success"
	}
	return <-p.done
}

// RejectPending fails any puts waiting in the submit channel and wakes the
// collector to clear an in-progress buffer. Safe to call from SetState while
// the collector is blocked inside Commit.
func (b *WriteBatcher) RejectPending() {
	for {
		select {
		case p := <-b.submit:
			p.done <- "Error: election"
		default:
			select {
			case b.rejectAll <- struct{}{}:
			default:
			}
			return
		}
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

		b.flushCount.Add(1)
		b.flushSize.Add(int64(len(pending)))
		batchFlushSize.WithLabelValues(b.n.Id).Add(float64(len(pending)))
		switch reason {
		case "forced":
			b.byForced.Add(1)
			batchFlushTotal.WithLabelValues(b.n.Id, "forced").Inc()
		default:
			b.bySize.Add(1)
			batchFlushTotal.WithLabelValues(b.n.Id, "size").Inc()
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

	drain := func() {
		for len(buf) < BatchMaxOps {
			select {
			case p := <-b.submit:
				buf = append(buf, p)
			default:
				return
			}
		}
	}

	reject := func() {
		for _, p := range buf {
			p.done <- "Error: election"
		}
		buf = nil
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
			buf = append(buf, p)
			drain()
			flush("size")
		case <-b.rejectAll:
			reject()
		}
	}
}
