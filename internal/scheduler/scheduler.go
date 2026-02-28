package scheduler

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

// Scheduler delivers messages at or after their scheduled deliverAt time.
//
// Usage:
//
//	s := New()
//	s.Start(ctx, func(msgID, queueKey string) {
//	    // move message from SCHEDULED → READY in the queue
//	})
//	defer s.Stop()
//
//	s.Schedule("01...", "payments/orders", time.Now().Add(time.Hour).UnixMilli())
//
// All methods are safe for concurrent use.
type Scheduler struct {
	mu     sync.Mutex
	h      minHeap
	byID   map[string]*item // msgID → item for O(1) Cancel lookup

	// notify is a buffered channel of capacity 1.
	// Schedule() sends a signal whenever a new item is added that might be
	// earlier than the current timer deadline, prompting the goroutine to
	// re-evaluate its sleep duration.
	notify chan struct{}

	done chan struct{}
	wg   sync.WaitGroup
}

// New creates a new Scheduler. Call Start() to begin delivering messages.
func New() *Scheduler {
	h := make(minHeap, 0, 64)
	heap.Init(&h)
	return &Scheduler{
		h:      h,
		byID:   make(map[string]*item),
		notify: make(chan struct{}, 1),
		done:   make(chan struct{}),
	}
}

// Schedule adds a message to the scheduler.
// If deliverAt <= now, readyFn is guaranteed to be called promptly on the
// next tick of the delivery goroutine (already past due).
//
// Calling Schedule with an ID that was previously cancelled replaces the old
// entry cleanly — the old item is removed before the new one is inserted.
//
// Schedule must not be called after Stop().
func (s *Scheduler) Schedule(msgID, queueKey string, deliverAt int64) {
	s.mu.Lock()

	// Replace a previously cancelled (or still-pending) entry for the same ID.
	if prev, ok := s.byID[msgID]; ok {
		prev.cancelled = true
		s.h.remove(prev.heapIdx)
		delete(s.byID, msgID)
	}

	it := &item{
		msgID:     msgID,
		queueKey:  queueKey,
		deliverAt: deliverAt,
	}
	heap.Push(&s.h, it)
	s.byID[msgID] = it

	s.mu.Unlock()

	// Signal the delivery goroutine to re-evaluate. Non-blocking: if a signal
	// is already pending (channel full), no-op — the goroutine will wake soon.
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// Cancel marks a scheduled message as cancelled so it will not be delivered.
// It is a no-op if the message is not currently scheduled.
// Cancel is O(log N) due to heap re-ordering.
func (s *Scheduler) Cancel(msgID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	it, ok := s.byID[msgID]
	if !ok {
		return
	}
	it.cancelled = true
	s.h.remove(it.heapIdx)
	delete(s.byID, msgID)
}

// Len returns the number of currently scheduled (non-cancelled) messages.
func (s *Scheduler) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.byID)
}

// CountByQueue returns the number of pending scheduled messages that belong to
// the given queueKey ("namespace/name"). Safe for concurrent use.
func (s *Scheduler) CountByQueue(queueKey string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int64
	for _, it := range s.byID {
		if it.queueKey == queueKey {
			n++
		}
	}
	return n
}

// Start launches the background delivery goroutine.
// readyFn is called for each message whose deliverAt has arrived.
// readyFn is called from the scheduler goroutine — it must not block for long.
// Start must be called exactly once.
func (s *Scheduler) Start(ctx context.Context, readyFn func(msgID, queueKey string)) {
	s.wg.Add(1)
	go s.run(ctx, readyFn)
}

// Stop shuts down the background goroutine and waits for it to exit.
// Any messages still in the heap are silently abandoned.
func (s *Scheduler) Stop() {
	select {
	case <-s.done:
		// already stopped
	default:
		close(s.done)
	}
	s.wg.Wait()
}

// ─── delivery goroutine ───────────────────────────────────────────────────────

func (s *Scheduler) run(ctx context.Context, readyFn func(msgID, queueKey string)) {
	defer s.wg.Done()

	// timer is lazily allocated when there's something to wait for.
	var t *time.Timer
	defer func() {
		if t != nil {
			t.Stop()
		}
	}()

	for {
		s.mu.Lock()
		next := s.peekReady() // returns nil if heap is empty
		s.mu.Unlock()

		if next == nil {
			// Heap is empty — wait for a new message or shutdown.
			select {
			case <-ctx.Done():
				return
			case <-s.done:
				return
			case <-s.notify:
				// A message was scheduled; loop around to re-evaluate.
			}
			continue
		}

		delay := time.Until(time.UnixMilli(next.deliverAt))
		if delay <= 0 {
			// Already due — pop and deliver without sleeping.
			s.mu.Lock()
			it := s.popAndRemove()
			s.mu.Unlock()
			if it != nil && !it.cancelled {
				readyFn(it.msgID, it.queueKey)
			}
			continue
		}

		// Sleep until the next message is due, but stay responsive to new
		// messages (notify) and shutdown signals.
		if t == nil {
			t = time.NewTimer(delay)
		} else {
			t.Reset(delay)
		}

		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-s.done:
			t.Stop()
			return
		case <-s.notify:
			// A new item may be due sooner — re-evaluate from the top.
			t.Stop()
			// Drain the timer channel if it fired between Reset and Stop.
			select {
			case <-t.C:
			default:
			}
			t = nil
		case <-t.C:
			t = nil
			// Timer fired — pop the root and deliver.
			s.mu.Lock()
			it := s.popAndRemove()
			s.mu.Unlock()
			if it != nil && !it.cancelled {
				readyFn(it.msgID, it.queueKey)
			}
		}
	}
}

// peekReady returns the root item without removing it, or nil if heap is empty.
// MUST be called with s.mu held.
func (s *Scheduler) peekReady() *item {
	for s.h.Len() > 0 {
		root := s.h[0]
		if root.cancelled {
			// Drain lazily-cancelled items from the root.
			heap.Pop(&s.h)
			delete(s.byID, root.msgID)
			continue
		}
		return root
	}
	return nil
}

// popAndRemove removes the root item and returns it (or nil if empty).
// MUST be called with s.mu held.
func (s *Scheduler) popAndRemove() *item {
	for s.h.Len() > 0 {
		it := heap.Pop(&s.h).(*item)
		delete(s.byID, it.msgID)
		if it.cancelled {
			continue
		}
		return it
	}
	return nil
}
