// Command producer-go replays PaySim rows into Kafka as JSON events.
//
// It emits exactly the same event schema, keys, and event-time scheme as producer/replay.py,
// so the Spark streaming job consumes either one unchanged:
//
//	producer-go -csv /data/raw/paysim.csv -brokers kafka:29092 -rate 0
package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// simStart anchors PaySim's hour offset (`step`) to a calendar date, same as the Python producer.
var simStart = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

type event struct {
	TxnID          int64   `json:"txn_id"`
	EventTS        string  `json:"event_ts"`
	TxnType        string  `json:"txn_type"`
	Amount         float64 `json:"amount"`
	AccountID      string  `json:"account_id"`
	OldBalanceOrig float64 `json:"old_balance_orig"`
	NewBalanceOrig float64 `json:"new_balance_orig"`
	DestID         string  `json:"dest_id"`
	OldBalanceDest float64 `json:"old_balance_dest"`
	NewBalanceDest float64 `json:"new_balance_dest"`
	IsFraud        int     `json:"is_fraud"` // ground-truth label, carried only to score the rules
}

func main() {
	csvPath := flag.String("csv", "/data/raw/paysim.csv", "PaySim CSV path")
	brokers := flag.String("brokers", envOr("KAFKA_BOOTSTRAP", "localhost:9092"), "comma-separated Kafka brokers")
	topic := flag.String("topic", "transactions", "Kafka topic")
	rate := flag.Int("rate", 0, "events per second (0 = as fast as possible)")
	limit := flag.Int64("limit", 0, "stop after N events (0 = all)")
	flag.Parse()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.DefaultProduceTopic(*topic),
		kgo.RequiredAcks(kgo.AllISRAcks()), // acks=all, like the Python producer
		kgo.ProducerLinger(20*time.Millisecond),
		kgo.ProducerBatchMaxBytes(1<<20),
		kgo.MaxBufferedRecords(200_000),
		// franz-go enables the idempotent producer by default: retries can't duplicate or reorder.
	)
	if err != nil {
		log.Fatalf("kafka client: %v", err)
	}
	defer client.Close()

	f, err := os.Open(*csvPath)
	if err != nil {
		log.Fatalf("open csv: %v", err)
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.ReuseRecord = true
	header, err := r.Read()
	if err != nil {
		log.Fatalf("read header: %v", err)
	}
	col := index(header)

	var failed atomic.Int64
	onDone := func(_ *kgo.Record, err error) {
		if err != nil {
			failed.Add(1)
		}
	}

	ctx := context.Background()
	start := time.Now()
	var sent int64
	step, rowsInStep := "", 0

	for {
		row, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			log.Fatalf("read row %d: %v", sent+1, err)
		}

		if row[col["step"]] != step {
			step, rowsInStep = row[col["step"]], 0
		}
		sent++
		ev, err := buildEvent(row, col, sent, rowsInStep)
		if err != nil {
			log.Fatalf("row %d: %v", sent, err)
		}
		rowsInStep++
		value, err := json.Marshal(ev)
		if err != nil {
			log.Fatalf("marshal row %d: %v", sent, err)
		}
		// Key by origin account so one account's events stay ordered within a partition.
		client.Produce(ctx, &kgo.Record{Key: []byte(ev.AccountID), Value: value}, onDone)

		if *limit > 0 && sent >= *limit {
			break
		}
		if *rate > 0 {
			if ahead := time.Duration(float64(sent)/float64(*rate)*float64(time.Second)) - time.Since(start); ahead > 0 {
				time.Sleep(ahead)
			}
		}
	}

	if err := client.Flush(ctx); err != nil {
		log.Fatalf("flush: %v", err)
	}
	elapsed := time.Since(start).Seconds()
	fmt.Printf("sent %d events in %.1fs (%.0f/s), delivery failures: %d\n", sent, elapsed, float64(sent)/elapsed, failed.Load())
}

// buildEvent converts one PaySim CSV row into the Kafka event.
//
// PaySim only has hour-level `step`, so rows inside the same hour are spread 50 ms apart in order.
// Event time therefore never goes backwards (the busiest hour has ~51K rows < 72K slots), which
// keeps the streaming job's watermark from discarding events as late.
func buildEvent(row []string, col map[string]int, txnID int64, rowInStep int) (event, error) {
	hours, err := strconv.Atoi(row[col["step"]])
	if err != nil {
		return event{}, fmt.Errorf("bad step %q: %w", row[col["step"]], err)
	}
	ts := simStart.Add(time.Duration(hours)*time.Hour + time.Duration(50*rowInStep)*time.Millisecond)
	f := func(name string) (float64, error) { return strconv.ParseFloat(row[col[name]], 64) }

	ev := event{
		TxnID:     txnID, // PaySim has no id; row order is stable
		EventTS:   ts.Format("2006-01-02T15:04:05.000000"),
		TxnType:   row[col["type"]],
		AccountID: row[col["nameOrig"]],
		DestID:    row[col["nameDest"]],
	}
	for name, dst := range map[string]*float64{
		"amount": &ev.Amount, "oldbalanceOrg": &ev.OldBalanceOrig, "newbalanceOrig": &ev.NewBalanceOrig,
		"oldbalanceDest": &ev.OldBalanceDest, "newbalanceDest": &ev.NewBalanceDest,
	} {
		v, err := f(name)
		if err != nil {
			return event{}, fmt.Errorf("bad %s %q: %w", name, row[col[name]], err)
		}
		*dst = v
	}
	fraud, err := strconv.Atoi(row[col["isFraud"]])
	if err != nil {
		return event{}, fmt.Errorf("bad isFraud %q: %w", row[col["isFraud"]], err)
	}
	ev.IsFraud = fraud
	return ev, nil
}

func index(header []string) map[string]int {
	m := make(map[string]int, len(header))
	for i, h := range header {
		m[h] = i
	}
	for _, want := range []string{"step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig", "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud"} {
		if _, ok := m[want]; !ok {
			log.Fatalf("csv missing column %q", want)
		}
	}
	return m
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
