package fsm

import (
	"fmt"
	"io"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"

	pb "raft-matching-engine/gen/matchengine/v1"
)

// Snapshot captures a point-in-time copy of FSM state. Per raft contract,
// Snapshot() runs on the FSM goroutine (so it's serialized with Apply), but
// the returned FSMSnapshot.Persist runs on its own goroutine and CAN race
// with subsequent Apply calls. So we deep-copy here and let Persist write
// the frozen view at its own pace, lock-free.
//
// We also serialize the entire payload upfront. For a POC the order-book
// size is bounded; this trades memory for simpler concurrency semantics.
// Future optimization: switch to a persistent (path-copying) btree so the
// "snapshot" is just a root-pointer grab, and stream-write in Persist.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	snap := &pb.Snapshot{
		AppliedIndex: f.appliedIdx,
		NextOrderId:  f.nextOrderID,
		// TsNanos is informational; deterministic replay does not need it.
	}
	for symbol, book := range f.books {
		sb := &pb.SnapshotBook{Symbol: symbol}
		book.Bids.Ascend(func(lvl *PriceLevel) bool {
			sb.Bids = append(sb.Bids, levelToProto(lvl))
			return true
		})
		book.Asks.Ascend(func(lvl *PriceLevel) bool {
			sb.Asks = append(sb.Asks, levelToProto(lvl))
			return true
		})
		snap.Books = append(snap.Books, sb)
	}
	for k, v := range f.dedup.entries {
		snap.Dedup = append(snap.Dedup, dedupEntryToProto(k, v))
	}

	data, err := proto.Marshal(snap)
	if err != nil {
		return nil, fmt.Errorf("marshal snapshot: %w", err)
	}
	return &fsmSnapshot{data: data}, nil
}

func levelToProto(lvl *PriceLevel) *pb.SnapshotLevel {
	out := &pb.SnapshotLevel{Price: lvl.Price}
	for o := lvl.head; o != nil; o = o.next {
		out.Orders = append(out.Orders, &pb.SnapshotOrder{
			Id:       o.ID,
			ClientId: o.ClientID,
			Quantity: o.Qty,
			TsNanos:  o.TsNanos,
		})
	}
	return out
}

func dedupEntryToProto(k dedupKey, v ApplyResult) *pb.SnapshotDedupEntry {
	out := &pb.SnapshotDedupEntry{
		ClientId:       k.ClientID,
		RequestId:      k.RequestID,
		ExpiresAtNanos: v.WrittenAtNs, // restored as-written; eviction will re-check
	}
	switch {
	case v.Err != "":
		out.Result = &pb.SnapshotDedupEntry_Error{Error: v.Err}
	case v.Type == EventPlaceOrder:
		out.Result = &pb.SnapshotDedupEntry_Place{Place: &pb.PlaceOrderResponse{
			OrderId:    v.OrderID,
			FilledQty:  v.FilledQty,
			RestingQty: v.RestingQty,
			Trades:     tradesToProto(v.Trades),
		}}
	case v.Type == EventCancelOrder:
		out.Result = &pb.SnapshotDedupEntry_Cancel{Cancel: &pb.CancelOrderResponse{
			OrderId: v.OrderID,
		}}
	}
	return out
}

func tradesToProto(ts []Trade) []*pb.Trade {
	if len(ts) == 0 {
		return nil
	}
	out := make([]*pb.Trade, len(ts))
	for i, t := range ts {
		out[i] = &pb.Trade{
			MakerOrderId: t.MakerOrderID,
			TakerOrderId: t.TakerOrderID,
			Price:        t.Price,
			Quantity:     t.Qty,
			TsNanos:      t.TsNanos,
		}
	}
	return out
}

func tradesFromProto(ts []*pb.Trade) []Trade {
	if len(ts) == 0 {
		return nil
	}
	out := make([]Trade, len(ts))
	for i, t := range ts {
		out[i] = Trade{
			MakerOrderID: t.MakerOrderId,
			TakerOrderID: t.TakerOrderId,
			Price:        t.Price,
			Qty:          t.Quantity,
			TsNanos:      t.TsNanos,
		}
	}
	return out
}

// ----------------------------------------------------------------------------

type fsmSnapshot struct {
	data []byte
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}

// ----------------------------------------------------------------------------
// raft.FSM: Restore — discards all current state and rebuilds from snapshot.
// ----------------------------------------------------------------------------

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("read snapshot: %w", err)
	}
	var snap pb.Snapshot
	if err := proto.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("unmarshal snapshot: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// Discard all previous state, per raft contract.
	f.books = make(map[string]*Book)
	f.dedup = newDedupCache(int64(DefaultDedupTTL))
	f.nextOrderID = snap.NextOrderId
	f.appliedIdx = snap.AppliedIndex
	f.applyCount = 0

	for _, sb := range snap.Books {
		book := newBook(sb.Symbol)
		f.books[sb.Symbol] = book
		for _, l := range sb.Bids {
			restoreLevel(book, SideBuy, l)
		}
		for _, l := range sb.Asks {
			restoreLevel(book, SideSell, l)
		}
	}
	for _, e := range snap.Dedup {
		res := dedupResultFromProto(e)
		f.dedup.put(e.ClientId, e.RequestId, res)
	}
	return nil
}

func restoreLevel(book *Book, side Side, l *pb.SnapshotLevel) {
	level := book.getOrCreateLevel(side, l.Price)
	for _, so := range l.Orders {
		o := &Order{
			ID:       so.Id,
			ClientID: so.ClientId,
			Side:     side,
			Price:    l.Price,
			Qty:      so.Quantity,
			TsNanos:  so.TsNanos,
		}
		level.pushBack(o)
		book.Orders[o.ID] = o
	}
}

func dedupResultFromProto(e *pb.SnapshotDedupEntry) ApplyResult {
	res := ApplyResult{WrittenAtNs: e.ExpiresAtNanos}
	switch r := e.Result.(type) {
	case *pb.SnapshotDedupEntry_Place:
		res.Type = EventPlaceOrder
		res.OrderID = r.Place.OrderId
		res.FilledQty = r.Place.FilledQty
		res.RestingQty = r.Place.RestingQty
		res.Trades = tradesFromProto(r.Place.Trades)
	case *pb.SnapshotDedupEntry_Cancel:
		res.Type = EventCancelOrder
		res.OrderID = r.Cancel.OrderId
	case *pb.SnapshotDedupEntry_Error:
		res.Err = r.Error
	}
	return res
}
