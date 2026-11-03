package storage

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/volume_server_pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	flag "github.com/seaweedfs/seaweedfs/weed/util/fla9"
)

var ecReadReasons = []EcReadReason{EcReadForClient, EcReadForScrub, EcReadForDelete, EcReadForAdmin}

// degradedReads snapshots ec_degraded_read_total for every reason and result.
// The counter is process-wide; this package has no parallel tests, so a
// test's before/after difference is its own.
func degradedReads() map[string]float64 {
	counts := make(map[string]float64)
	for _, reason := range ecReadReasons {
		for _, result := range []string{"ok", "fail"} {
			counts[string(reason)+"/"+result] = testutil.ToFloat64(stats.VolumeServerECDegradedReadCounter.WithLabelValues(string(reason), result))
		}
	}
	return counts
}

// assertDegradedReads checks that exactly want moved since before, every other
// reason and result staying put.
func assertDegradedReads(t *testing.T, before map[string]float64, want map[string]float64) {
	t.Helper()
	for key, now := range degradedReads() {
		if got := now - before[key]; got != want[key] {
			t.Errorf("ec_degraded_read_total{%s} moved by %v, want %v", key, got, want[key])
		}
	}
}

// mountEcShardsExcept writes an EC volume holding one needle and mounts every
// shard but those in missing. The location cache points at a port nothing
// listens on and has no entry for the missing shards, so a read never asks the
// master and reconstructs them from what is mounted.
func mountEcShardsExcept(t *testing.T, store *Store, vid needle.VolumeId, missing ...erasure_coding.ShardId) (*erasure_coding.EcVolume, *needle.Needle) {
	t.Helper()
	_, n := writeEcVolumeFiles(t, store.Locations[0].Directory, vid)
	skip := make(map[erasure_coding.ShardId]bool)
	for _, shardId := range missing {
		skip[shardId] = true
	}
	for shardId := erasure_coding.ShardId(0); shardId < erasure_coding.TotalShardsCount; shardId++ {
		if skip[shardId] {
			continue
		}
		if err := store.MountEcShards("", vid, shardId, ""); err != nil {
			t.Fatalf("mount shard %d: %v", shardId, err)
		}
	}
	ecVolume, found := store.Locations[0].FindEcVolume(vid)
	if !found {
		t.Fatal("ec volume not mounted")
	}
	seedShardLocations(ecVolume, pb.NewServerAddressWithGrpcPort("127.0.0.1:1", 1))
	ecVolume.ShardLocationsLock.Lock()
	for shardId := range skip {
		delete(ecVolume.ShardLocations, shardId)
	}
	ecVolume.ShardLocationsLock.Unlock()
	return ecVolume, n
}

// intervalsOnShard counts the needle's intervals on shardId: one mark each
// when that shard is missing.
func intervalsOnShard(t *testing.T, ecVolume *erasure_coding.EcVolume, n *needle.Needle, shardId erasure_coding.ShardId) (count int) {
	t.Helper()
	_, _, intervals, err := ecVolume.LocateEcShardNeedle(n.Id, ecVolume.Version)
	if err != nil {
		t.Fatalf("locate needle: %v", err)
	}
	for _, interval := range intervals {
		if sid, _ := ecVolume.IntervalToShardIdAndOffset(interval); sid == shardId {
			count++
		}
	}
	if count == 0 {
		t.Fatalf("the needle has no interval on shard %d", shardId)
	}
	return count
}

// A read whose shards are all present reads them directly and isn't counted.
func TestEcDegradedReadNotCountedForADirectRead(t *testing.T) {
	store := newTestStore(t, 1)
	const vid = needle.VolumeId(17)
	_, n := mountEcShardsExcept(t, store, vid)
	before := degradedReads()

	got := &needle.Needle{Id: n.Id}
	if _, err := store.ReadEcShardNeedle(vid, got, nil); err != nil {
		t.Fatalf("read needle: %v", err)
	}
	if !bytes.Equal(got.Data, n.Data) {
		t.Fatal("direct read returned different bytes")
	}
	assertDegradedReads(t, before, nil)
}

// A client read that has to rebuild the shard its needle starts on is counted
// as read/ok once per interval on that shard, and returns the exact bytes.
func TestEcDegradedReadCountsAReconstructedRead(t *testing.T) {
	store := newTestStore(t, 1)
	const vid = needle.VolumeId(18)
	ecVolume, n := mountEcShardsExcept(t, store, vid, 0)
	want := float64(intervalsOnShard(t, ecVolume, n, 0))
	before := degradedReads()

	got := &needle.Needle{Id: n.Id}
	if _, err := store.ReadEcShardNeedle(vid, got, nil); err != nil {
		t.Fatalf("read needle: %v", err)
	}
	if !bytes.Equal(got.Data, n.Data) {
		t.Fatal("reconstructed read returned different bytes")
	}
	assertDegradedReads(t, before, map[string]float64{"read/ok": want})
}

// A reconstruction short of DataShards shards is counted as fail.
func TestEcDegradedReadCountsAFailedReconstruction(t *testing.T) {
	store := newTestStore(t, 1)
	const vid = needle.VolumeId(19)
	missing := []erasure_coding.ShardId{0, 6, 7, 8, 9, 10, 11, 12, 13}
	ecVolume, n := mountEcShardsExcept(t, store, vid, missing...)
	_, _, intervals, err := ecVolume.LocateEcShardNeedle(n.Id, ecVolume.Version)
	if err != nil {
		t.Fatalf("locate needle: %v", err)
	}
	shardId, offset := ecVolume.IntervalToShardIdAndOffset(intervals[0])
	if shardId != 0 {
		t.Fatalf("first interval on shard %d, want 0", shardId)
	}
	before := degradedReads()

	buf := make([]byte, intervals[0].Size)
	if _, _, err := store.recoverOneRemoteEcShardInterval(EcReadForClient, n.Id, ecVolume, shardId, buf, offset); err == nil {
		t.Fatal("recovered with 5 shards, want a failure")
	}
	assertDegradedReads(t, before, map[string]float64{"read/fail": 1})
}

// A scrub that rebuilds what it can't read is counted as scrub, not read.
func TestEcDegradedReadCountsAScrubAsScrub(t *testing.T) {
	const vid = needle.VolumeId(20)
	store, ecVolume, n := newLocalEcVolume(t, vid)
	want := float64(intervalsOnShard(t, ecVolume, n, 0))
	if err := store.UnmountEcShards(vid, 0, 0); err != nil {
		t.Fatalf("unmount shard 0: %v", err)
	}
	before := degradedReads()

	if _, _, errs := store.ScrubEcVolume(vid, volume_server_pb.VolumeScrubMode_READS, false); len(errs) != 0 {
		t.Fatalf("scrub: %v", errs)
	}
	assertDegradedReads(t, before, map[string]float64{"scrub/ok": want})
}

// Deleting a needle reads it first; a reconstruction there is counted as delete.
func TestEcDegradedReadCountsADeleteAsDelete(t *testing.T) {
	store := newTestStore(t, 1)
	const vid = needle.VolumeId(21)
	ecVolume, n := mountEcShardsExcept(t, store, vid, 0)
	want := float64(intervalsOnShard(t, ecVolume, n, 0))
	before := degradedReads()

	got := &needle.Needle{Id: n.Id}
	if _, err := store.ReadEcShardNeedleFor(EcReadForDelete, vid, got, nil); err != nil {
		t.Fatalf("read needle for delete: %v", err)
	}
	assertDegradedReads(t, before, map[string]float64{"delete/ok": want})
}

// The rebuild that drops one gathered input at a time reconstructs too, so a
// read repaired that way is counted, once per interval.
func TestEcDegradedReadCountsADroppedInputRebuild(t *testing.T) {
	store := newTestStore(t, 1)
	const vid = needle.VolumeId(22)
	n := randomNeedleOfSize(77, 1024)
	ec := encodeAndMountEcVolume(t, store, vid, []*needle.Needle{n})
	offset, size, intervals, err := ec.LocateEcShardNeedle(n.Id, ec.Version)
	if err != nil || len(intervals) != 1 {
		t.Fatalf("fixture: %v, %d intervals", err, len(intervals))
	}
	sid, shardOffset := ec.IntervalToShardIdAndOffset(intervals[0])
	corruptEcShardRange(t, ec, sid, shardOffset, int64(intervals[0].Size))
	corruptEcShardRange(t, ec, erasure_coding.ShardId(ec.ECContext.DataShards), shardOffset, int64(intervals[0].Size))
	before := degradedReads()

	got := &needle.Needle{Id: n.Id}
	candidate, deleted := store.rebuildEcNeedleFromDroppedInputs(EcReadForClient, got, n.Id, ec, ec.ECContext, intervals,
		[]erasure_coding.ShardId{sid}, []int64{shardOffset}, offset.ToActualOffset(), size,
		bytes.Repeat([]byte{0xA5}, int(intervals[0].Size)), map[erasure_coding.ShardId]bool{sid: true})
	if deleted || candidate == nil || !bytes.Equal(got.Data, n.Data) {
		t.Fatal("the dropped-input rebuild did not recover the needle")
	}
	assertDegradedReads(t, before, map[string]float64{"read/ok": 1})
}

// captureStderr runs f with os.Stderr piped, and returns what was written.
func captureStderr(t *testing.T, f func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr := os.Stderr
	os.Stderr = w
	out := make(chan string)
	go func() {
		b, _ := io.ReadAll(r)
		out <- string(b)
	}()
	defer func() { os.Stderr = stderr }()
	f()
	w.Close()
	return <-out
}

// The marker line is what an operator or a test harness reads: it names the
// volume, encode generation, needle, shard, available shards, reason and
// result at V(1) for this file, and is absent at the default level.
func TestEcDegradedReadLogLine(t *testing.T) {
	for _, flagValue := range []struct{ name, value string }{{"logtostderr", "true"}} {
		if err := flag.Set(flagValue.name, flagValue.value); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		flag.Set("vmodule", "")
		flag.Set("logtostderr", "false")
	})
	read := func(vid needle.VolumeId, missing ...erasure_coding.ShardId) (*erasure_coding.EcVolume, *needle.Needle, string) {
		store := newTestStore(t, 1)
		ecVolume, n := mountEcShardsExcept(t, store, vid, missing...)
		ecVolume.EncodeTsNs = 1700000000123456789
		var markers []string
		logged := captureStderr(t, func() {
			got := &needle.Needle{Id: n.Id}
			if _, err := store.ReadEcShardNeedle(vid, got, nil); (err == nil) != (len(missing) == 1) {
				t.Errorf("read needle with %v missing: %v", missing, err)
			}
		})
		for _, line := range strings.Split(logged, "\n") {
			if strings.Contains(line, "ec degraded read:") {
				markers = append(markers, line)
			}
		}
		return ecVolume, n, strings.Join(markers, "\n")
	}

	if err := flag.Set("vmodule", "store_ec=1"); err != nil {
		t.Fatal(err)
	}
	_, n, markers := read(23, 0)
	want := "ec degraded read: volume=23 generation=1700000000123456789 needle=" + n.Id.String() +
		" shard=0 available=10/10 reason=read result=ok error=-"
	if !strings.HasSuffix(markers, want) || strings.Count(markers, "ec degraded read:") != 1 {
		t.Errorf("at V(1) the marker is %q, want one line ending %q", markers, want)
	}

	_, n, markers = read(25, 0, 6, 7, 8, 9, 10, 11, 12, 13)
	want = "ec degraded read: volume=25 generation=1700000000123456789 needle=" + n.Id.String() +
		" shard=0 available=5/10 reason=read result=fail error="
	lines := strings.Split(markers, "\n")
	for _, line := range lines {
		_, failure, _ := strings.Cut(line, " error=")
		if failure = strings.TrimSpace(failure); !strings.Contains(line, want) || failure == "" || failure == "-" {
			t.Errorf("failure marker %q, want it to contain %q and a non-empty error", line, want)
		}
	}
	if len(lines) != 1 {
		t.Errorf("logged %d failure markers, want 1: %q", len(lines), markers)
	}

	if err := flag.Set("vmodule", ""); err != nil {
		t.Fatal(err)
	}
	if _, _, markers := read(24, 0); markers != "" {
		t.Errorf("at the default level the marker was logged: %q", markers)
	}
}
