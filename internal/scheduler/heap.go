// Package scheduler implements a Min-Heap based message scheduler.
//
// Core design principle from the plan:
//   - DB scan WHERE deliver_at <= NOW() → O(N) — gets slower as messages grow.
//   - Min-Heap peek                     → O(1) — constant regardless of size.
//   - Min-Heap insert                    → O(log N) — fast.
//
// The Scheduler goroutine peeks at the heap root (the soonest-due message),
// sleeps until that point, then pops and fires the readyFn callback.
// A buffered notify channel lets Schedule() interrupt the sleep early whenever
// a newly added message is due sooner than the current root.
package scheduler

import "container/heap"

// item is one entry in the scheduler Min-Heap.
type item struct {
	msgID     string // ULID — uniquely identifies the message
	queueKey  string // "namespace/queueName" — routing key for readyFn
	deliverAt int64  // UTC milliseconds — sort key

	// heapIdx is the item's current position in the heap slice.
	// Maintained by minHeap.Swap so we can do O(log N) Cancel via heap.Remove.
	heapIdx int

	// cancelled marks an item for lazy deletion.
	// Cancelled items are discarded by the goroutine instead of delivered.
	// Lazy deletion avoids an extra O(log N) heap.Remove in the common path.
	cancelled bool
}

// minHeap is a slice of *item that satisfies heap.Interface.
// The smallest deliverAt sits at index 0 (Min-Heap).
type minHeap []*item

func (h minHeap) Len() int { return len(h) }

func (h minHeap) Less(i, j int) bool {
	return h[i].deliverAt < h[j].deliverAt
}

func (h minHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIdx = i
	h[j].heapIdx = j
}

func (h *minHeap) Push(x any) {
	n := len(*h)
	it := x.(*item)
	it.heapIdx = n
	*h = append(*h, it)
}

func (h *minHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	old[n-1] = nil  // allow GC
	it.heapIdx = -1 // mark as not in heap
	*h = old[:n-1]
	return it
}

// remove removes the item at position idx and re-heapifies in O(log N).
func (h *minHeap) remove(idx int) *item {
	return heap.Remove(h, idx).(*item)
}
