package queue

// statemachine.go — message lifecycle state transition rules.
//
// State diagram (from the plan):
//
//	SCHEDULED ──────────────────────────────► READY
//	                                             │
//	                          ┌──────────────────┘
//	                          ▼
//	                       IN_FLIGHT
//	                          │
//	             ┌────────────┼────────────────────┐
//	             ▼            ▼                    ▼
//	          DELETED       READY             DEAD_LETTER
//	          (ACK)     (NACK or timeout)    (retries exhausted)

// ValidTransition reports whether the transition from → to is a legal
// state change for a message.
//
// Used defensively in tests; production code drives transitions through the
// Queue methods (Ack, Nack, requeue, deadLetter) which already enforce the rules.
func ValidTransition(from, to Status) bool {
	switch from {
	case StatusReady:
		// READY can only move to IN_FLIGHT (via Dequeue).
		return to == StatusInFlight
	case StatusInFlight:
		// IN_FLIGHT can:
		//   → DELETED     — consumer ACKed
		//   → READY       — consumer NACKed or visibility timeout expired
		//   → DEAD_LETTER — retries exhausted
		return to == StatusDeleted || to == StatusReady || to == StatusDeadLetter
	case StatusScheduled:
		// SCHEDULED can only move to READY (via the Scheduler delivery callback).
		return to == StatusReady
	case StatusDeadLetter:
		// DEAD_LETTER is the terminal failure state — not directly re-transitionable
		// by the queue engine. DLQ Replay creates a NEW message in the target queue.
		return false
	case StatusDeleted:
		// DELETED is the terminal success state.
		return false
	}
	return false
}
