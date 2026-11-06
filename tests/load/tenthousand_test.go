//go:build loadproof

package load

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"os/exec"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"

	"github.com/rowjay007/collab-sphere/pkg/collabclient"
	"github.com/rowjay007/collab-sphere/pkg/crdt"
	"github.com/rowjay007/collab-sphere/tests/internal/stack"
)

const (
	loadDocs        = 500
	loadConnsPerDoc = 20
	loadActiveDocs  = 25
	loadEditors     = 2
	loadEdits       = 40
	loadSendGap     = 200 * time.Millisecond
	loadWarmup      = 3 * time.Second
	loadDialLimit   = 64
	loadClientBase  = 100_000
)

func TestTenThousand(t *testing.T) {
	preflight(t)

	s := stack.New(t,
		stack.WithHubs(2),
		stack.WithHubMetrics(),
		stack.WithHubEnv("HUB_IDLE_SYNC=10s"),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 18*time.Minute)
	defer cancel()

	docIDs := make([]string, loadDocs)
	for i := range docIDs {
		docIDs[i] = s.CreateDocument(ctx)
	}

	type slot struct {
		doc  int
		ix   int
		hub  int
		id   crdt.ClientID
		conn *collabclient.Client
	}
	slots := make([]slot, 0, loadDocs*loadConnsPerDoc)
	for doc := range loadDocs {
		hub := doc % 2
		for ix := range loadConnsPerDoc {
			slots = append(slots, slot{
				doc: doc,
				ix:  ix,
				hub: hub,
				id:  crdt.ClientID(loadClientBase + doc*loadConnsPerDoc + ix),
			})
		}
	}

	dialStart := time.Now()
	sem := make(chan struct{}, loadDialLimit)
	var dialWG sync.WaitGroup
	var dialErr atomic.Value
	for i := range slots {
		dialWG.Add(1)
		go func(i int) {
			defer dialWG.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if dialErr.Load() != nil {
				return
			}
			sl := &slots[i]
			c, err := dialHeld(ctx, s, docIDs[sl.doc], sl.id, sl.hub)
			if err != nil {
				dialErr.Store(err)
				return
			}
			sl.conn = c
		}(i)
	}
	dialWG.Wait()
	if err, _ := dialErr.Load().(error); err != nil {
		t.Fatal(err)
	}
	dialElapsed := time.Since(dialStart)
	fmt.Printf("dial time=%s\n", dialElapsed.Round(time.Millisecond))
	t.Cleanup(func() {
		for i := range slots {
			if slots[i].conn != nil {
				_ = slots[i].conn.Close()
			}
		}
	})

	byDoc := make([][]*collabclient.Client, loadDocs)
	for i := range slots {
		sl := slots[i]
		byDoc[sl.doc] = append(byDoc[sl.doc], sl.conn)
	}
	for doc := range loadDocs {
		if len(byDoc[doc]) != loadConnsPerDoc {
			t.Fatalf("doc %d: got %d connections, want %d", doc, len(byDoc[doc]), loadConnsPerDoc)
		}
	}

	start := make(chan struct{})
	var editWG sync.WaitGroup
	for doc := range loadActiveDocs {
		for editor := range loadEditors {
			c := byDoc[doc][editor]
			editWG.Add(1)
			go func(c *collabclient.Client) {
				defer editWG.Done()
				<-start
				for range loadEdits {
					time.Sleep(loadSendGap)
					if err := c.Edit(func(r *crdt.Replica) ([]crdt.Op, error) {
						op, err := r.InsertAfter(crdt.Root, 'x')
						return []crdt.Op{op}, err
					}); err != nil {
						t.Error(err)
						return
					}
				}
			}(c)
		}
	}
	cohortStart := time.Now()
	close(start)
	editWG.Wait()
	for doc := range loadActiveDocs {
		for editor := range loadEditors {
			if err := byDoc[doc][editor].WaitAcked(ctx); err != nil {
				t.Fatal(err)
			}
		}
	}

	recordAfter := cohortStart.Add(loadWarmup)
	echoHist := hdrhistogram.New(1, 60_000_000, 3)
	fanoutHist := hdrhistogram.New(1, 60_000_000, 3)
	var echoSamples, fanoutSamples int

	for doc := range loadActiveDocs {
		conns := byDoc[doc]
		received := make([]map[crdt.ID]time.Time, len(conns))
		for i, c := range conns {
			received[i] = c.ReceiveTimes()
		}
		for editor := range loadEditors {
			sender := conns[editor]
			senderIdx := editor
			type opSend struct {
				id crdt.ID
				at time.Time
			}
			sends := sender.SendTimes()
			ordered := make([]opSend, 0, len(sends))
			for id, at := range sends {
				ordered = append(ordered, opSend{id, at})
			}
			slices.SortFunc(ordered, func(a, b opSend) int {
				return a.at.Compare(b.at)
			})
			echoes := sender.Latencies()
			if len(echoes) != len(ordered) {
				t.Fatalf("doc %d editor %d: got %d echoes, want %d sends", doc, editor, len(echoes), len(ordered))
			}
			for i, op := range ordered {
				if op.at.Before(recordAfter) {
					continue
				}
				echoUs := echoes[i].Microseconds()
				if err := echoHist.RecordValue(echoUs); err != nil {
					t.Fatal(err)
				}
				echoSamples++

				var last time.Time
				for connIdx, recvMap := range received {
					if connIdx == senderIdx {
						continue
					}
					recvAt, ok := recvMap[op.id]
					if !ok {
						t.Fatalf("doc %d conn %d missing receive for %v", doc, connIdx, op.id)
					}
					if recvAt.After(last) {
						last = recvAt
					}
				}
				fanoutUs := last.Sub(op.at).Microseconds()
				if err := fanoutHist.RecordValue(fanoutUs); err != nil {
					t.Fatal(err)
				}
				fanoutSamples++
			}
		}
	}

	fmt.Printf("connections=%d\n", loadDocs*loadConnsPerDoc)
	fmt.Printf("echo samples=%d fan-out samples=%d\n", echoSamples, fanoutSamples)
	fmt.Printf("echo p50=%dus p99=%dus max=%dus\n",
		echoHist.ValueAtPercentile(50), echoHist.ValueAtPercentile(99.0), echoHist.Max())
	fmt.Printf("fan-out p50=%dus p99=%dus max=%dus\n",
		fanoutHist.ValueAtPercentile(50), fanoutHist.ValueAtPercentile(99.0), fanoutHist.Max())
	printHubAlloc(t, s)
	printHubStalls(s)

	echoP99 := echoHist.ValueAtPercentile(99.0)
	fanoutP99 := fanoutHist.ValueAtPercentile(99.0)
	if echoP99 >= 50_000 {
		t.Fatalf("echo p99 %dus >= 50000us", echoP99)
	}
	if fanoutP99 >= 50_000 {
		t.Fatalf("fan-out p99 %dus >= 50000us", fanoutP99)
	}
}

func dialHeld(ctx context.Context, s *stack.Stack, docID string, id crdt.ClientID, hub int) (*collabclient.Client, error) {
	opts := collabclient.Options{URLs: []string{s.HubURL(hub)}, Client: id, Token: s.Token(s.Owner())}
	c, err := collabclient.DialWith(ctx, docID, opts)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func preflight(t *testing.T) {
	t.Helper()
	var lim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim); err != nil {
		t.Fatalf("read RLIMIT_NOFILE: %v", err)
	}
	if lim.Cur < 20_000 {
		t.Fatalf("RLIMIT_NOFILE soft limit is %d, need at least 20000 (run ulimit -n 65536)", lim.Cur)
	}
	if runtime.GOOS != "darwin" {
		return
	}
	first, err := sysctlInt("net.inet.ip.portrange.first")
	if err != nil {
		t.Fatalf("read net.inet.ip.portrange.first: %v", err)
	}
	last, err := sysctlInt("net.inet.ip.portrange.last")
	if err != nil {
		t.Fatalf("read net.inet.ip.portrange.last: %v", err)
	}
	ports := last - first + 1
	if ports < 12_000 {
		t.Fatalf("ephemeral port range is %d (%d-%d), need at least 12000", ports, first, last)
	}
}

func sysctlInt(name string) (int, error) {
	out, err := exec.Command("/usr/sbin/sysctl", "-n", name).Output()
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(string(out)))
}

func printHubAlloc(t *testing.T, s *stack.Stack) {
	t.Helper()
	for i := range s.Hubs() {
		url := s.HubMetricsURL(i)
		if url == "" {
			fmt.Printf("hub-%d go_memstats_alloc_bytes: metrics disabled\n", i)
			continue
		}
		alloc, err := scrapeGoMemstatsAlloc(url)
		if err != nil {
			fmt.Printf("hub-%d go_memstats_alloc_bytes: unavailable (%v)\n", i, err)
			continue
		}
		fmt.Printf("hub-%d go_memstats_alloc_bytes=%d\n", i, alloc)
	}
}

func printHubStalls(s *stack.Stack) {
	names := []string{
		"collab_hub_apply_seconds",
		"collab_hub_append_seconds",
		"collab_hub_deliver_seconds",
		"collab_hub_fanout_lag_seconds",
	}
	for i := range s.Hubs() {
		url := s.HubMetricsURL(i)
		if url == "" {
			continue
		}
		text, err := fetchMetrics(url)
		if err != nil {
			fmt.Printf("hub-%d metrics: %v\n", i, err)
			continue
		}
		for _, name := range names {
			sum, count, ok := histogramSumCount(text, name)
			if !ok {
				continue
			}
			fmt.Printf("hub-%d %s count=%.0f sum=%.6fs\n", i, name, count, sum)
			for _, le := range []string{"0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1"} {
				if c, ok := histogramBucket(text, name, le); ok {
					fmt.Printf("hub-%d %s le=%s count=%.0f\n", i, name, le, c)
				}
			}
		}
	}
}

func fetchMetrics(metricsURL string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metricsURL, nil)
	if err != nil {
		return "", err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status %d", resp.StatusCode)
	}
	var b strings.Builder
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		b.WriteString(sc.Text())
		b.WriteByte('\n')
	}
	if err := sc.Err(); err != nil {
		return "", err
	}
	return b.String(), nil
}

func histogramSumCount(text, name string) (sum, count float64, ok bool) {
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, name+"_sum ") {
			sum, _ = strconv.ParseFloat(strings.Fields(line)[1], 64)
		}
		if strings.HasPrefix(line, name+"_count ") {
			count, _ = strconv.ParseFloat(strings.Fields(line)[1], 64)
		}
	}
	return sum, count, count > 0
}

func histogramBucket(text, name, le string) (float64, bool) {
	needle := name + "_bucket{le=\"" + le + "\"}"
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, needle) {
			f, err := strconv.ParseFloat(strings.Fields(line)[1], 64)
			return f, err == nil
		}
	}
	return 0, false
}

func scrapeGoMemstatsAlloc(metricsURL string) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metricsURL, nil)
	if err != nil {
		return 0, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status %d", resp.StatusCode)
	}
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "go_memstats_alloc_bytes ") {
			fields := strings.Fields(line)
			if len(fields) != 2 {
				return 0, fmt.Errorf("bad line %q", line)
			}
			f, err := strconv.ParseFloat(fields[1], 64)
			if err != nil {
				return 0, err
			}
			return uint64(f), nil
		}
	}
	if err := sc.Err(); err != nil {
		return 0, err
	}
	return 0, fmt.Errorf("go_memstats_alloc_bytes not found")
}
