// Package fsm implements the raft.FSM for the match engine. The FSM owns:
//
//   - The order book(s), keyed by symbol.
//   - The dedup cache, keyed by (client_id, request_id), used for idempotency
//     and as the backing store for LookupByRequestID.
//   - A monotonic next-order-id, advanced inside Apply so it's deterministic
//     on replay.
//
// Apply is called from a single raft goroutine, but read methods (Lookup,
// TopOfBook) are called from gRPC handler goroutines, so we guard FSM state
// with an RWMutex. Apply takes Lock; reads take RLock.
package fsm

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"

	pb "raft-matching-engine/gen/matchengine/v1"
)

// DefaultDedupTTL — how long an idempotent result is remembered. 24h is a
// reasonable POC default; production systems would tighten with per-client
// monotonic seqs.
const DefaultDedupTTL = 24 * time.Hour

// dedupGCInterval — how often (in number of Apply calls) the dedup cache
// gets a sweep. Driven off Apply count so it's deterministic across replicas.
const dedupGCInterval = 4096


// Compile-time assert FSM satisfies raft.FSM (Snapshot/Restore in snapshot.go).
var _ raft.FSM = (*FSM)(nil)


type FSM struct {
	mu          sync.RWMutex
	books       map[string]*Book
	dedup       *dedupCache
	nextOrderID uint64
	appliedIdx  uint64
	applyCount  uint64
}

func New() *FSM {
	return &FSM{
		books: make(map[string]*Book),
		dedup: newDedupCache(int64(DefaultDedupTTL)),
	}
}

// ----------------------------------------------------------------------------
// raft.FSM: Apply
// ----------------------------------------------------------------------------

func (f *FSM) Apply(log *raft.Log) any {
	f.mu.Lock()
	defer f.mu.Unlock()

	var entry pb.LogEntry
	if err := proto.Unmarshal(log.Data, &entry); err != nil {
		return ApplyResult{Err: "decode: " + err.Error()}
	}

	// Idempotent replay: if we've seen this (client_id, request_id) before,
	// return the cached result without touching the book. This is what makes
	// "client retries the same request_id" safe.
	if cached, ok := f.dedup.get(entry.ClientId, entry.RequestId); ok {
		f.appliedIdx = log.Index
		return cached
	}

	var res ApplyResult
	switch entry.Type {
	case pb.EventType_EVENT_TYPE_PLACE_ORDER:
		res = f.applyPlace(&entry)
	case pb.EventType_EVENT_TYPE_CANCEL_ORDER:
		res = f.applyCancel(&entry)
	default:
		res = ApplyResult{Err: fmt.Sprintf("unknown event type: %v", entry.Type)}
	}
	res.WrittenAtNs = entry.TsNanos
	f.dedup.put(entry.ClientId, entry.RequestId, res)

	f.appliedIdx = log.Index
	f.applyCount++
	if f.applyCount%dedupGCInterval == 0 {
		f.dedup.evictOlderThan(entry.TsNanos)
	}
	return res
}

func (f *FSM) applyPlace(e *pb.LogEntry) ApplyResult {
	p := e.GetPlace()
	if p == nil {
		return ApplyResult{Type: EventPlaceOrder, Err: "missing place payload"}
	}
	side, err := sideFromProto(p.Side)
	if err != nil {
		return ApplyResult{Type: EventPlaceOrder, Err: err.Error()}
	}

	book, ok := f.books[p.Symbol]
	if !ok {
		book = newBook(p.Symbol)
		f.books[p.Symbol] = book
	}

	f.nextOrderID++
	orig := p.Quantity
	taker := &Order{
		ID:       f.nextOrderID,
		ClientID: e.ClientId,
		Side:     side,
		Price:    p.Price,
		Qty:      p.Quantity,
		TsNanos:  e.TsNanos,
	}

	cross := crossesBuy
	if side == SideSell {
		cross = crossesSell
	}
	trades := match(book, taker, cross)
	res := ApplyResult{
		Type:       EventPlaceOrder,
		OrderID:    taker.ID,
		FilledQty:  orig - taker.Qty,
		RestingQty: taker.Qty,
		Trades:     trades,
	}

	if taker.Qty > 0 {
		level := book.getOrCreateLevel(side, taker.Price)
		level.pushBack(taker)
		book.Orders[taker.ID] = taker
	}
	return res
}

func (f *FSM) applyCancel(e *pb.LogEntry) ApplyResult {
	c := e.GetCancel()
	if c == nil {
		return ApplyResult{Type: EventCancelOrder, Err: "missing cancel payload"}
	}
	book, ok := f.books[c.Symbol]
	if !ok {
		return ApplyResult{Type: EventCancelOrder, OrderID: c.OrderId, Err: "not_found"}
	}
	o, ok := book.Orders[c.OrderId]
	if !ok {
		return ApplyResult{Type: EventCancelOrder, OrderID: c.OrderId, Err: "not_found"}
	}
	// Authorization: only the owner may cancel.
	if o.ClientID != e.ClientId {
		return ApplyResult{Type: EventCancelOrder, OrderID: c.OrderId, Err: "forbidden"}
	}

	lvl := o.level
	remaining := o.Qty
	lvl.unlink(o)
	lvl.TotalQty -= remaining
	if lvl.head == nil {
		book.sideTree(o.Side).Delete(lvl)
	}
	delete(book.Orders, o.ID)
	return ApplyResult{Type: EventCancelOrder, OrderID: c.OrderId}
}

// ----------------------------------------------------------------------------
// Read APIs (called from gRPC handler goroutines)
// ----------------------------------------------------------------------------

// Lookup returns the cached idempotency result for (clientID, requestID).
// This is what backs LookupByRequestID — it does NOT go through raft.Apply,
// because lookups must not contribute to the log (DoS via paranoid retries).
func (f *FSM) Lookup(clientID, requestID string) (ApplyResult, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.dedup.get(clientID, requestID)
}

// TopOfBook returns up to depth levels per side, sorted: bids high→low, asks low→high.
func (f *FSM) TopOfBook(symbol string, depth int) (bids, asks []LevelView, ok bool) {
	if depth <= 0 {
		depth = 5
	}
	f.mu.RLock()
	defer f.mu.RUnlock()
	book, exists := f.books[symbol]
	if !exists {
		return nil, nil, false
	}
	bids = make([]LevelView, 0, depth)
	asks = make([]LevelView, 0, depth)

	book.Bids.Ascend(func(l *PriceLevel) bool {
		bids = append(bids, LevelView{Price: l.Price, TotalQty: l.TotalQty, OrderCount: l.OrderCount})
		return len(bids) < depth
	})
	book.Asks.Ascend(func(l *PriceLevel) bool {
		asks = append(asks, LevelView{Price: l.Price, TotalQty: l.TotalQty, OrderCount: l.OrderCount})
		return len(asks) < depth
	})
	return bids, asks, true
}

type LevelView struct {
	Price      int64
	TotalQty   int64
	OrderCount int32
}

// AppliedIndex returns the raft log index of the most recent Apply.
// Useful for diagnostics and read-after-write checks.
func (f *FSM) AppliedIndex() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.appliedIdx
}

// ----------------------------------------------------------------------------
// Helpers
// ----------------------------------------------------------------------------

func sideFromProto(s pb.Side) (Side, error) {
	switch s {
	case pb.Side_SIDE_BUY:
		return SideBuy, nil
	case pb.Side_SIDE_SELL:
		return SideSell, nil
	default:
		return 0, errors.New("invalid side")
	}
}

func sideToProto(s Side) pb.Side {
	if s == SideSell {
		return pb.Side_SIDE_SELL
	}
	return pb.Side_SIDE_BUY
}
