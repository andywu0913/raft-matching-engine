package fsm

// dedupKey composes the idempotency key. Namespaced by client_id so two
// clients can't collide on the same request_id.
type dedupKey struct {
	ClientID  string
	RequestID string
}

// ApplyResult is the cached idempotency outcome. One of Place / Cancel is
// populated based on the originating event type. Err carries a structured
// failure reason (e.g. "not_found"); when Err != "" the order_id and trade
// fields are zero-valued.
type ApplyResult struct {
	Type        EventType
	OrderID     uint64
	FilledQty   int64
	RestingQty  int64
	Trades      []Trade
	Err         string
	WrittenAtNs int64 // log entry's TsNanos; drives TTL eviction deterministically
}

type EventType uint8

const (
	EventPlaceOrder EventType = iota + 1
	EventCancelOrder
)

// dedupCache is a simple map with deterministic, log-driven TTL eviction.
// Eviction runs from inside Apply (driven off log timestamps), never off
// wall-clock time, so all replicas evict identically.
type dedupCache struct {
	entries map[dedupKey]ApplyResult
	ttlNs   int64
}

func newDedupCache(ttlNs int64) *dedupCache {
	return &dedupCache{
		entries: make(map[dedupKey]ApplyResult),
		ttlNs:   ttlNs,
	}
}

func (d *dedupCache) get(clientID, requestID string) (ApplyResult, bool) {
	v, ok := d.entries[dedupKey{clientID, requestID}]
	return v, ok
}

func (d *dedupCache) put(clientID, requestID string, res ApplyResult) {
	d.entries[dedupKey{clientID, requestID}] = res
}

// evictOlderThan removes entries whose WrittenAtNs is older than (nowNs - ttl).
// Caller passes the current log entry's TsNanos as nowNs so eviction is
// deterministic across replicas.
func (d *dedupCache) evictOlderThan(nowNs int64) {
	cutoff := nowNs - d.ttlNs
	for k, v := range d.entries {
		if v.WrittenAtNs < cutoff {
			delete(d.entries, k)
		}
	}
}

func (d *dedupCache) len() int { return len(d.entries) }
