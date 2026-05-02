// load_test_client drives sustained PlaceOrder traffic against a leader and reports
// throughput + latency percentiles. Each worker uses unique request_ids so
// the FSM does real work (no dedup-replay short-circuit). Designed for a
// 3-node local cluster — the QPS ceiling here is dominated by raft commit
// latency (boltdb fsync + replication RTT), not the matching engine itself.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	pb "raft-matching-engine/proto_gen/matchengine/v1"
)

func main() {
	var (
		target      = flag.String("target", "127.0.0.1:9001", "leader gRPC address")
		duration    = flag.Duration("duration", 20*time.Second, "measured duration (post-warmup)")
		warmup      = flag.Duration("warmup", 2*time.Second, "warmup period excluded from stats")
		concurrency = flag.Int("concurrency", 64, "concurrent workers")
		symbol      = flag.String("symbol", "BTCUSD", "symbol")
		priceRange  = flag.Int("price-range", 1000, "spread of prices around 50000 (avoids hot-level contention)")
	)
	flag.Parse()

	conn, err := grpc.NewClient(*target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewMatchEngineServiceClient(conn)

	// Pre-flight: confirm the target is actually the leader.
	pingCtx := metadata.NewOutgoingContext(
		context.Background(),
		metadata.New(map[string]string{"x-client-id": "load_test_client"}),
	)
	pingCtx, pcancel := context.WithTimeout(pingCtx, 5*time.Second)
	if _, err := client.GetTopOfBook(pingCtx, &pb.GetTopOfBookRequest{Symbol: *symbol, Depth: 1}); err != nil {
		pcancel()
		log.Fatalf("pre-flight: %v (target probably isn't the leader)", err)
	}
	pcancel()

	log.Printf("load_test_client → %s  symbol=%s  concurrency=%d  warmup=%s  duration=%s",
		*target, *symbol, *concurrency, *warmup, *duration)

	type workerStats struct {
		latencies []time.Duration
		ok, fail  uint64
	}
	stats := make([]workerStats, *concurrency)

	var (
		totalSent atomic.Uint64
		totalOK   atomic.Uint64
		totalFail atomic.Uint64
	)

	overallStart := time.Now()
	warmupDeadline := overallStart.Add(*warmup)
	measuredDeadline := overallStart.Add(*warmup + *duration)

	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(workerID), uint64(workerID*31+1)))
			ws := &stats[workerID]
			ws.latencies = make([]time.Duration, 0, 16384)
			ctx := metadata.NewOutgoingContext(
				context.Background(),
				metadata.New(map[string]string{"x-client-id": fmt.Sprintf("loadw%d", workerID)}),
			)
			counter := 0
			for {
				now := time.Now()
				if now.After(measuredDeadline) {
					return
				}
				counter++
				req := &pb.PlaceOrderRequest{
					RequestId: fmt.Sprintf("w%d-%d", workerID, counter),
					Symbol:    *symbol,
					Side:      pb.Side_SIDE_SELL,
					Price:     int64(50000 + r.IntN(*priceRange)),
					Quantity:  1,
				}
				start := time.Now()
				_, err := client.PlaceOrder(ctx, req)
				lat := time.Since(start)
				totalSent.Add(1)
				if err != nil {
					ws.fail++
					totalFail.Add(1)
					continue
				}
				ws.ok++
				totalOK.Add(1)
				if start.After(warmupDeadline) {
					ws.latencies = append(ws.latencies, lat)
				}
			}
		}(i)
	}
	wg.Wait()

	// Merge per-worker latencies.
	var all []time.Duration
	for i := range stats {
		all = append(all, stats[i].latencies...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })

	measuredDur := time.Since(warmupDeadline)
	if measuredDur < 0 {
		measuredDur = 0
	}
	qps := float64(len(all)) / measuredDur.Seconds()

	pct := func(p float64) time.Duration {
		if len(all) == 0 {
			return 0
		}
		i := int(float64(len(all)) * p)
		if i >= len(all) {
			i = len(all) - 1
		}
		return all[i]
	}

	fmt.Println("---- results ----")
	fmt.Printf("total sent     : %d\n", totalSent.Load())
	fmt.Printf("total ok       : %d\n", totalOK.Load())
	fmt.Printf("total failed   : %d\n", totalFail.Load())
	fmt.Printf("measured window: %s (post-warmup)\n", measuredDur.Round(time.Millisecond))
	fmt.Printf("measured reqs  : %d\n", len(all))
	fmt.Printf("QPS            : %.0f\n", qps)
	fmt.Printf("latency p50    : %s\n", pct(0.50).Round(time.Microsecond))
	fmt.Printf("latency p95    : %s\n", pct(0.95).Round(time.Microsecond))
	fmt.Printf("latency p99    : %s\n", pct(0.99).Round(time.Microsecond))
	fmt.Printf("latency max    : %s\n", pct(1.0).Round(time.Microsecond))
}
