package topology

import (
	"strings"
	"testing"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/sequence"
	"github.com/seaweedfs/seaweedfs/weed/storage"
	"github.com/seaweedfs/seaweedfs/weed/storage/needle"
	"github.com/seaweedfs/seaweedfs/weed/storage/super_block"
	"github.com/seaweedfs/seaweedfs/weed/storage/types"
)

// heartbeatLayout is three servers in one rack and replication 001: volume 1
// on {a, b}, 2 on {b, c}, 3 on {a, c}. Every server has just heartbeated.
func heartbeatLayout(t *testing.T) (*Topology, *VolumeLayout, map[string]*DataNode) {
	t.Helper()
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)
	dc := NewDataCenter("dc1")
	topo.LinkChildNode(dc)
	rack := NewRack("rack1")
	dc.LinkChildNode(rack)
	rp, _ := super_block.NewReplicaPlacementFromString("001")
	vl := topo.GetVolumeLayout("", rp, needle.EMPTY_TTL, types.HardDriveType)
	nodes := map[string]*DataNode{}
	for _, id := range []string{"a", "b", "c"} {
		dn := NewDataNode(id)
		dn.Ip = id
		rack.LinkChildNode(dn)
		dn.MarkHeartbeat(topo.HeartbeatDeadline(time.Now()))
		nodes[id] = dn
	}
	for vid, holders := range map[needle.VolumeId][]string{1: {"a", "b"}, 2: {"b", "c"}, 3: {"a", "c"}} {
		vi := storage.VolumeInfo{Id: vid, Version: needle.GetCurrentVersion(), ReplicaPlacement: rp}
		for _, id := range holders {
			nodes[id].AddOrUpdateVolume(vi)
			vl.RegisterVolume(&vi, nodes[id], nil)
		}
		vl.setVolumeWritable(vid)
	}
	return topo, vl, nodes
}

func picked(t *testing.T, vl *VolumeLayout, option *VolumeGrowOption) map[needle.VolumeId]bool {
	t.Helper()
	got := map[needle.VolumeId]bool{}
	for i := 0; i < 200; i++ {
		vid, _, _, _, err := vl.PickForWrite(1, option)
		if err != nil {
			t.Fatal(err)
		}
		got[vid] = true
	}
	return got
}

// A silently partitioned volume server keeps its stream, and its
// volumes writable, until gRPC keepalive gives up (~80 s), and every write
// assigned there blocks on the silent replica. Past three missed pulses its
// volumes are no longer picked; one heartbeat brings them back.
func TestPickForWriteSkipsVolumesOnASilentServer(t *testing.T) {
	topo, vl, nodes := heartbeatLayout(t)
	nodes["b"].MarkHeartbeat(-1) // any non-zero deadline in the past; 0 means never heard
	for name, option := range map[string]*VolumeGrowOption{
		"any location": {},
		"on server a":  {DataNode: "a"},
		"in rack1":     {Rack: "rack1"},
	} {
		if got := picked(t, vl, option); got[1] || got[2] || !got[3] {
			t.Errorf("%s: picked %v, want only volume 3 while b is silent", name, got)
		}
	}
	vl.maxWritableVolumes = 3 // the capped path collects every match first
	if got := picked(t, vl, &VolumeGrowOption{DataNode: "a"}); got[1] || !got[3] {
		t.Errorf("capped: picked %v, want only volume 3", got)
	}
	vl.maxWritableVolumes = 0

	nodes["b"].MarkHeartbeat(topo.HeartbeatDeadline(time.Now()))
	if got := picked(t, vl, &VolumeGrowOption{}); !got[1] || !got[2] {
		t.Errorf("picked %v after b heartbeated again, want its volumes back", got)
	}
}

func TestPickForWriteWithEveryHolderSilentAsksForGrowth(t *testing.T) {
	_, vl, nodes := heartbeatLayout(t)
	for _, dn := range nodes {
		dn.MarkHeartbeat(-1) // any non-zero deadline in the past; 0 means never heard
	}
	for _, option := range []*VolumeGrowOption{{}, {DataNode: "a"}} {
		_, _, _, shouldGrow, err := vl.PickForWrite(1, option)
		if err == nil || !strings.Contains(err.Error(), NoWritableVolumes) || !shouldGrow {
			t.Errorf("option %+v: err=%v shouldGrow=%v, want NoWritableVolumes and growth", option, err, shouldGrow)
		}
	}
}

// The deadline is three pulses (15 s) after the heartbeat: one pulse would
// flap healthy servers that heartbeat every 5 s.
func TestHeartbeatDeadlineIsThreePulses(t *testing.T) {
	topo := NewTopology("weedfs", sequence.NewMemorySequencer(), 32*1024, 5, false)
	now := time.Now()
	if got := topo.HeartbeatDeadline(now) - monoSeconds(now); got != 15 {
		t.Fatalf("deadline margin = %d s, want 15", got)
	}
}

// A server not yet heard from on this master (deadline 0), as in topologies
// built without heartbeats, is never treated as silent.
func TestHeartbeatOverdueNeedsAHeartbeatFirst(t *testing.T) {
	dn := NewDataNode("fresh")
	if dn.heartbeatOverdue(monoSeconds(time.Now()) + 3600) {
		t.Fatal("a server with no heartbeat yet counted as silent")
	}
	dn.MarkHeartbeat(100)
	if !dn.heartbeatOverdue(101) || dn.heartbeatOverdue(100) {
		t.Fatal("deadline is inclusive of its own second and overdue after it")
	}
}

// With every writable volume holding a copy on the silent server,
// PickForWrite asks for growth, and growth used to place the new volume on
// that server, where AllocateVolume hung.
// Growth now treats a silent server like one in maintenance mode.
func TestVolumeGrowthSkipsASilentServer(t *testing.T) {
	topo, dc := newMaintenanceTestTopology()
	rack := NewRack("rack1")
	dc.LinkChildNode(rack)
	silent := addMaintenanceTestNode(rack, "silent", 1000)
	silent.MarkHeartbeat(-1) // any non-zero deadline in the past
	for _, id := range []string{"small1", "small2"} {
		addMaintenanceTestNode(rack, id, 2).MarkHeartbeat(topo.HeartbeatDeadline(time.Now()))
	}

	vg := NewDefaultVolumeGrowth()
	rp, _ := super_block.NewReplicaPlacementFromString("001")
	option := &VolumeGrowOption{ReplicaPlacement: rp, DiskType: types.HardDriveType}
	for i := 0; i < 50; i++ {
		servers, _, err := vg.findEmptySlotsForOneVolume(topo, option, false)
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		for _, s := range servers {
			if s.Id() == silent.Id() {
				t.Fatalf("iteration %d: growth picked the silent server", i)
			}
		}
	}
	rp0, _ := super_block.NewReplicaPlacementFromString("000")
	pinned := &VolumeGrowOption{ReplicaPlacement: rp0, DiskType: types.HardDriveType, DataNode: "silent"}
	if _, _, err := vg.findEmptySlotsForOneVolume(topo, pinned, false); err == nil {
		t.Fatal("a volume pinned to a silent server must not be created")
	}

	// One heartbeat and it is a candidate again.
	silent.MarkHeartbeat(topo.HeartbeatDeadline(time.Now()))
	if _, _, err := vg.findEmptySlotsForOneVolume(topo, pinned, false); err != nil {
		t.Fatalf("a server heartbeating again must take volumes: %v", err)
	}
}
