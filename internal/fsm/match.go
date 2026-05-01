package fsm

// Trade is the in-memory matched-trade record. The protobuf Trade is
// constructed from this at the gRPC boundary.
type Trade struct {
	MakerOrderID uint64
	TakerOrderID uint64
	Price        int64
	Qty          int64
	TsNanos      int64
}

// match runs the FIFO crossing loop for the taker against the opposite side.
// Mutates the book and the taker.Qty in place. Returns the trade list.
//
// crosses(takerPx, makerPx) reports whether the taker can cross the maker's
// price. For a Buy taker: makerPx <= takerPx. For a Sell taker: makerPx >= takerPx.
func match(book *Book, taker *Order, crosses func(takerPx, makerPx int64) bool) []Trade {
	var trades []Trade
	opp := book.Asks
	if taker.Side == SideSell {
		opp = book.Bids
	}

	for taker.Qty > 0 {
		best, ok := opp.Min()
		if !ok || !crosses(taker.Price, best.Price) {
			break
		}

		// Walk this level's FIFO.
		for taker.Qty > 0 && best.head != nil {
			maker := best.head
			qty := minI64(taker.Qty, maker.Qty)

			trades = append(trades, Trade{
				MakerOrderID: maker.ID,
				TakerOrderID: taker.ID,
				Price:        best.Price, // maker price is the trade price
				Qty:          qty,
				TsNanos:      taker.TsNanos,
			})

			taker.Qty -= qty
			maker.Qty -= qty
			best.TotalQty -= qty

			if maker.Qty == 0 {
				best.unlink(maker)
				delete(book.Orders, maker.ID)
				// pool release deferred to caller / future optimization
			}
		}

		if best.head == nil {
			opp.Delete(best)
		}
	}
	return trades
}

func minI64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func crossesBuy(takerPx, makerPx int64) bool  { return makerPx <= takerPx }
func crossesSell(takerPx, makerPx int64) bool { return makerPx >= takerPx }
