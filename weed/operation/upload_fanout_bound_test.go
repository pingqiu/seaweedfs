package operation

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// silentHolder accepts connections and never answers: a silently
// partitioned volume server after the connect.
func silentHolder(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var held []net.Conn
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			held = append(held, conn)
		}
	}()
	t.Cleanup(func() {
		listener.Close()
		for _, conn := range held {
			conn.Close()
		}
	})
	return listener.Addr().String()
}

func shortFanoutBounds(t *testing.T, fanout, cleanup time.Duration) {
	t.Helper()
	oldFanout, oldCleanup := replicaFanoutTimeout, replicaCleanupTimeout
	replicaFanoutTimeout, replicaCleanupTimeout = fanout, cleanup
	t.Cleanup(func() { replicaFanoutTimeout, replicaCleanupTimeout = oldFanout, oldCleanup })
}

// within runs f and fails the test if it outlives limit.
func within(t *testing.T, limit time.Duration, what string, f func()) time.Duration {
	t.Helper()
	done := make(chan struct{})
	started := time.Now()
	go func() {
		defer close(done)
		f()
	}()
	select {
	case <-done:
		return time.Since(started)
	case <-time.After(limit):
		t.Fatalf("%s still waiting after %v", what, limit)
		return 0
	}
}

// A silent holder held one S3 chunk ~50 s across the fan-out's
// attempts (30 s header timeout, then dial timeouts). The fan-out now fails at
// its deadline, reassignable, and rolls back the copy that landed.
func TestUploadChunkToHoldersBoundsASilentHolder(t *testing.T) {
	shortFanoutBounds(t, 300*time.Millisecond, 300*time.Millisecond)
	var goodDeletes int32
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			atomic.AddInt32(&goodDeletes, 1)
			w.WriteHeader(http.StatusOK)
			return
		}
		fmt.Fprintf(w, `{"name":"f","size":11}`)
	}))
	defer good.Close()

	hosts := []string{strings.TrimPrefix(good.URL, "http://"), silentHolder(t)}
	var err error
	took := within(t, 10*time.Second, "fan-out to a silent holder", func() {
		_, err = uploadChunkToHolders(context.Background(), hosts, "3,01abcdef", []byte("hello world"), "", "", false, &ChunkedUploadOption{})
	})
	if err == nil || !ShouldReassignUpload(err) {
		t.Fatalf("err = %v, want a reassignable failure", err)
	}
	if took > 3*time.Second {
		t.Errorf("fan-out took %v, want about its 300ms deadline", took)
	}
	if got := atomic.LoadInt32(&goodDeletes); got != 1 {
		t.Errorf("rollback deletes on the good holder = %d, want 1", got)
	}
}

// The rollback after a failed chunk deletes from every holder, the silent one
// included; each delete is bounded so the cleanup cannot hold the upload.
func TestDeleteChunkFromHoldersBoundsASilentHolder(t *testing.T) {
	shortFanoutBounds(t, 300*time.Millisecond, 200*time.Millisecond)
	var goodDeletes int32
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&goodDeletes, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer good.Close()

	took := within(t, 10*time.Second, "cleanup with a silent holder", func() {
		deleteChunkFromHolders([]string{silentHolder(t), strings.TrimPrefix(good.URL, "http://")}, "3,01abcdef", "", &ChunkedUploadOption{})
	})
	if took > 3*time.Second {
		t.Errorf("cleanup took %v, want about its 200ms bound", took)
	}
	if got := atomic.LoadInt32(&goodDeletes); got != 1 {
		t.Errorf("deletes on the good holder = %d, want 1: a silent holder first must not skip the rest", got)
	}
}

// With a silent holder on every offer (the master keeps assigning such
// volumes until it drops the server), the chunk stops at its second fan-out
// deadline: two offers, 2 x (10 s + 3 s), about 26 s, not three (~39 s).
func TestUploadReaderInChunksStopsAfterTwoFanoutDeadlines(t *testing.T) {
	const fanout, cleanup = 300 * time.Millisecond, 200 * time.Millisecond
	shortFanoutBounds(t, fanout, cleanup)
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusOK)
			return
		}
		fmt.Fprintf(w, `{"name":"f","size":11}`)
	}))
	defer good.Close()
	silent := silentHolder(t)
	var assigns int32
	opt := &ChunkedUploadOption{
		ChunkSize: 1 << 20,
		AssignFunc: func(ctx context.Context, count int, size uint64) (*VolumeAssignRequest, *AssignResult, error) {
			n := atomic.AddInt32(&assigns, 1)
			return nil, &AssignResult{Fid: fmt.Sprintf("%d,01abcdef", n), Url: strings.TrimPrefix(good.URL, "http://"),
				Replicas: []Location{{Url: silent}}}, nil
		},
	}
	var err error
	took := within(t, 30*time.Second, "upload with a silent holder on every offer", func() {
		_, err = UploadReaderInChunks(context.Background(), bytes.NewReader([]byte("hello world")), opt)
	})
	if err == nil || !errors.Is(err, errReplicaFanoutDeadline) {
		t.Fatalf("err = %v, want the fan-out deadline", err)
	}
	if offers := atomic.LoadInt32(&assigns); offers != 2 {
		t.Errorf("offers = %d, want 2", offers)
	}
	if limit := 2*(fanout+cleanup) + (fanout+cleanup)/2; took > limit {
		t.Errorf("took %v, want within two offers (%v)", took, limit)
	}
}

// When the caller's own deadline is the shorter one, the
// failure is the caller's, not a silent holder's, and must not reassign.
func TestUploadChunkToHoldersLeavesTheCallersDeadlineAlone(t *testing.T) {
	shortFanoutBounds(t, 5*time.Second, 200*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	var err error
	within(t, 10*time.Second, "fan-out under the caller's deadline", func() {
		_, err = uploadChunkToHolders(ctx, []string{silentHolder(t), silentHolder(t)}, "3,01abcdef", []byte("hello world"), "", "", false, &ChunkedUploadOption{})
	})
	if err == nil || ShouldReassignUpload(err) || errors.Is(err, errReplicaFanoutDeadline) {
		t.Fatalf("err = %v, want the caller's deadline, not reassignable", err)
	}
}
