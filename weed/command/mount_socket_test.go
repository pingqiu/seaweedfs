//go:build linux || darwin || freebsd || windows

package command

import (
	"net"
	"os"
	"path/filepath"
	"testing"
)

// The mount's local gRPC socket must not outlive the mount (issue 1176).
func TestCloseMountSocketRemovesTheSocketFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seaweedfs-mount-1.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Skipf("unix sockets unavailable: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("listening socket missing: %v", err)
	}
	closeMountSocket(listener, path)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("socket file still present after close: %v", err)
	}
	closeMountSocket(listener, path) // the interrupt hook and the deferred call may both run
}

// A file left at the socket path (a listener that does not unlink on close) is removed too.
func TestCloseMountSocketRemovesALeftoverPath(t *testing.T) {
	dir := t.TempDir()
	listener, err := net.Listen("unix", filepath.Join(dir, "other.sock"))
	if err != nil {
		t.Skipf("unix sockets unavailable: %v", err)
	}
	path := filepath.Join(dir, "seaweedfs-mount-2.sock")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	closeMountSocket(listener, path)
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("leftover socket path still present: %v", err)
	}
}
