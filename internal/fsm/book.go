package fsm

import (
	"github.com/google/btree"
)

type Side uint8

const (
	SideBuy Side = iota + 1
	SideSell
)

type Order struct {
	ID       uint64
	ClientID string
	Side     Side
	Price    int64
	Qty      int64
	TsNanos  int64

	// Intrusive FIFO pointers — saves a *Element per order vs container/list.
	prev, next *Order
	level      *PriceLevel
}

type PriceLevel struct {
	Price      int64
	TotalQty   int64
	OrderCount int32
	head, tail *Order
}

type Book struct {
	Symbol string
	Bids   *btree.BTreeG[*PriceLevel] // max-first: highest bid at Min()
	Asks   *btree.BTreeG[*PriceLevel] // min-first: lowest ask at Min()
	Orders map[uint64]*Order
}

const btreeDegree = 32

func newBook(symbol string) *Book {
	return &Book{
		Symbol: symbol,
		// Bids: highest first → "less" returns true when a is *greater*.
		Bids: btree.NewG(btreeDegree, func(a, b *PriceLevel) bool {
			return a.Price > b.Price
		}),
		Asks: btree.NewG(btreeDegree, func(a, b *PriceLevel) bool {
			return a.Price < b.Price
		}),
		Orders: make(map[uint64]*Order),
	}
}

func (b *Book) sideTree(side Side) *btree.BTreeG[*PriceLevel] {
	if side == SideBuy {
		return b.Bids
	}
	return b.Asks
}

// getOrCreateLevel finds an existing level at price on the given side, or
// inserts a new empty one. The returned level is always non-nil.
func (b *Book) getOrCreateLevel(side Side, price int64) *PriceLevel {
	tree := b.sideTree(side)
	probe := &PriceLevel{Price: price}
	if existing, ok := tree.Get(probe); ok {
		return existing
	}
	tree.ReplaceOrInsert(probe)
	return probe
}

// pushBack inserts o at the tail of level (FIFO order).
func (l *PriceLevel) pushBack(o *Order) {
	o.level = l
	o.prev = l.tail
	o.next = nil
	if l.tail != nil {
		l.tail.next = o
	} else {
		l.head = o
	}
	l.tail = o
	l.TotalQty += o.Qty
	l.OrderCount++
}

// unlink removes o from its level's FIFO. Caller updates level.TotalQty
// based on the *remaining* qty being removed (since partial fills already
// decremented TotalQty).
func (l *PriceLevel) unlink(o *Order) {
	if o.prev != nil {
		o.prev.next = o.next
	} else {
		l.head = o.next
	}
	if o.next != nil {
		o.next.prev = o.prev
	} else {
		l.tail = o.prev
	}
	o.prev, o.next, o.level = nil, nil, nil
	l.OrderCount--
}
