package surfstore

import (
	"fmt"
)

var ErrServerCrashedUnreachable = fmt.Errorf("server is crashed or unreachable")
var ErrServerCrashed = fmt.Errorf("server is crashed")
var ErrNotLeader = fmt.Errorf("server is not the leader")

// Enums

type PeerInfo int

const (
	PeerInfoUnreachable PeerInfo = iota
	PeerInfoSuccess
	PeerInfoFail
)

// Structs

type UpdateFileResponse struct {
	version *Version
	Err     error
}

type PeerStatus struct {
	isReachable bool
	isUpdated   bool
}

type PeerMessage struct {
	peerId   int64
	peerInfo PeerInfo
}
