package storage

import (
	"bytes"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/stats"
	"github.com/seaweedfs/seaweedfs/weed/storage/erasure_coding"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
)

// degradedReads is the ec_degraded_read_total count for result. The counter is
// process-wide, so tests compare it before and after their own read.
func degradedReads(result string) float64 {
	return testutil.ToFloat64(stats.VolumeServerECDegradedReadCounter.WithLabelValues(result))
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

// A read whose shards are all present reads them directly and isn't counted.
func TestEcDegradedReadNotCountedForADirectRead(t *testing.T) {
	store := newTestStore(t, 1)
	const vid = needle.VolumeId(17)
	_, n := mountEcShardsExcept(t, store, vid)
	ok, fail := degradedReads("ok"), degradedReads("fail")

	got := &needle.Needle{Id: n.Id}
	if _, err := store.ReadEcShardNeedle(vid, got, nil); err != nil {
		t.Fatalf("read needle: %v", err)
	}
	if !bytes.Equal(got.Data, n.Data) {
		t.Fatal("direct read returned different bytes")
	}
	if d := degradedReads("ok") - ok; d != 0 {
		t.Errorf("ok count moved by %v on a direct read", d)
	}
	if d := degradedReads("fail") - fail; d != 0 {
		t.Errorf("fail count moved by %v on a direct read", d)
	}
}

// A read that has to rebuild the shard its needle starts on is counted once as
// ok, and still returns the needle's bytes.
func TestEcDegradedReadCountsAReconstructedRead(t *testing.T) {
	store := newTestStore(t, 1)
	const vid = needle.VolumeId(18)
	ecVolume, n := mountEcShardsExcept(t, store, vid, 0)
	_, _, intervals, err := ecVolume.LocateEcShardNeedle(n.Id, ecVolume.Version)
	if err != nil {
		t.Fatalf("locate needle: %v", err)
	}
	missingIntervals := 0
	for _, interval := range intervals {
		if shardId, _ := ecVolume.IntervalToShardIdAndOffset(interval); shardId == 0 {
			missingIntervals++
		}
	}
	if missingIntervals == 0 {
		t.Fatal("the needle has no interval on shard 0, so nothing would be reconstructed")
	}
	ok, fail := degradedReads("ok"), degradedReads("fail")

	got := &needle.Needle{Id: n.Id}
	if _, err := store.ReadEcShardNeedle(vid, got, nil); err != nil {
		t.Fatalf("read needle: %v", err)
	}
	if !bytes.Equal(got.Data, n.Data) {
		t.Fatal("reconstructed read returned different bytes")
	}
	if d := degradedReads("ok") - ok; d != float64(missingIntervals) {
		t.Errorf("ok count moved by %v, want %d, one per interval on the missing shard", d, missingIntervals)
	}
	if d := degradedReads("fail") - fail; d != 0 {
		t.Errorf("fail count moved by %v on a successful reconstruction", d)
	}
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
	ok, fail := degradedReads("ok"), degradedReads("fail")

	buf := make([]byte, intervals[0].Size)
	if _, _, err := store.recoverOneRemoteEcShardInterval(n.Id, ecVolume, shardId, buf, offset); err == nil {
		t.Fatal("recovered with 5 shards, want a failure")
	}
	if d := degradedReads("fail") - fail; d != 1 {
		t.Errorf("fail count moved by %v, want 1", d)
	}
	if d := degradedReads("ok") - ok; d != 0 {
		t.Errorf("ok count moved by %v on a failed reconstruction", d)
	}
}
