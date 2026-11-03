package weed_server

import (
	"testing"
	"time"

	"github.com/seaweedfs/raft"
	"github.com/seaweedfs/seaweedfs/weed/pb"
	"github.com/seaweedfs/seaweedfs/weed/pb/master_pb"
	"github.com/stretchr/testify/require"
)

// drainHookStream runs onDrain once every queued heartbeat has been handled,
// while the stream is still open and the node still registered.
type drainHookStream struct {
	fakeHeartbeatStream
	onDrain func()
}

func (s *drainHookStream) Recv() (*master_pb.Heartbeat, error) {
	if len(s.heartbeats) == 0 && s.onDrain != nil {
		s.onDrain()
		s.onDrain = nil
	}
	return s.fakeHeartbeatStream.Recv()
}

type followerRaftServer struct{ raft.Server }

func (followerRaftServer) State() string  { return raft.Follower }
func (followerRaftServer) Leader() string { return "" }

// The leader's heartbeat handling is what arms the deadline: every
// heartbeat it processes sets the node's deadline to now + three pulses.
// Without that call no deadline is ever set and nothing is ever skipped.
func TestSendHeartbeatSetsTheDeadlineOnTheLeader(t *testing.T) {
	ms := newLeaderMaster()
	ms.Topo.RaftServer = admitTestRaftServer{}
	ms.Topo.ReleaseAllocationOnFirstReport("10.0.0.1:8080")
	address := pb.NewServerAddress("10.0.0.1", 8080, 0)

	got := int64(-1)
	stream := &drainHookStream{fakeHeartbeatStream: fakeHeartbeatStream{heartbeats: []*master_pb.Heartbeat{
		{Ip: "10.0.0.1", Port: 8080, HasNoVolumes: true},
		{Ip: "10.0.0.1", Port: 8080, HasNoVolumes: true},
	}}}
	stream.onDrain = func() {
		if dn := ms.Topo.LookupDataNodeByAddress(address); dn != nil {
			got = dn.HeartbeatDeadlineSeconds()
		}
	}
	want := ms.Topo.HeartbeatDeadline(time.Now())
	_ = ms.SendHeartbeat(stream)
	require.InDelta(t, want, got, 1, "deadline after a leader-processed heartbeat")
}

// A follower hands the stream back to the leader and must not arm anything.
func TestSendHeartbeatOnAFollowerLeavesTheDeadlineUnset(t *testing.T) {
	ms := newLeaderMaster()
	ms.Topo.RaftServer = followerRaftServer{}
	dc := ms.Topo.GetOrCreateDataCenter("DefaultDataCenter")
	dn := dc.GetOrCreateRack("DefaultRack").GetOrCreateDataNode("10.0.0.1", 8080, 18080, "", "", nil)

	stream := &fakeHeartbeatStream{heartbeats: []*master_pb.Heartbeat{{Ip: "10.0.0.1", Port: 8080, HasNoVolumes: true}}}
	require.Error(t, ms.SendHeartbeat(stream))
	require.Zero(t, dn.HeartbeatDeadlineSeconds())
}
