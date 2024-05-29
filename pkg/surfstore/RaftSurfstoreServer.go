package surfstore

import (
	context "context"
	"log"
	"sync"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	serverStatus      ServerStatus
	serverStatusMutex *sync.RWMutex

	id   int64
	term int64

	log            []*UpdateOperation
	metaStore      *MetaStore
	commitIndex    int64
	raftStateMutex *sync.RWMutex

	rpcConns   []*grpc.ClientConn
	grpcServer *grpc.Server

	/*--------------- Added --------------*/
	n int
	m int

	lastApplied int64

	peers           []string
	pendingRequests []*chan PendingRequest

	/*--------------- Chaos Monkey --------------*/
	unreachableFrom map[int64]bool
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	// Ensure that the majority of servers are up
	// if err := s.checkStatus(); err != nil {
	// 	return nil, err
	// }
	return s.metaStore.GetFileInfoMap(ctx, empty)
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	// Ensure that the majority of servers are up
	return s.metaStore.GetBlockStoreMap(ctx, hashes)

}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	// Ensure that the majority of servers are up
	return s.metaStore.GetBlockStoreAddrs(ctx, empty)

}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	// Ensure that the request gets replicated on majority of the servers.
	// Commit the entries and then apply to the state machine

	// if err := s.checkStatus(); err != nil {
	// 	return nil, err
	// }

	pendingReq := make(chan PendingRequest)
	s.raftStateMutex.Lock()
	entry := UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}
	s.log = append(s.log, &entry)

	s.pendingRequests = append(s.pendingRequests, &pendingReq)

	//TODO: Think whether it should be last or first request
	reqId := len(s.pendingRequests) - 1
	s.raftStateMutex.Unlock()

	go s.sendPersistentHeartbeats(ctx, int64(reqId))

	response := <-pendingReq
	if response.err != nil {
		return nil, response.err
	}

	//TODO:
	// Ensure that leader commits first and then applies to the state machine
	s.raftStateMutex.Lock()
	s.commitIndex += 1
	s.raftStateMutex.Unlock()

	return s.metaStore.UpdateFile(ctx, entry.FileMetaData)
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex or whose term
// doesn't match prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	// Check status
	s.serverStatusMutex.RLock()
	myStatus := s.serverStatus
	s.serverStatusMutex.RUnlock()
	if err := s.checkStatus(myStatus, true, input.LeaderId); err != nil {
		return nil, err
	}

	s.raftStateMutex.RLock()
	myTerm := s.term
	myId := s.id
	s.raftStateMutex.RUnlock()

	// Reject if peer is stale
	if input.Term < myTerm {
		return s.makeAppendEntryOutput(myTerm, myId, false, -1), nil
	}

	// Revert to follower if I am stale
	if myTerm < input.Term {
		if myStatus != ServerStatus_FOLLOWER {
			s.serverStatusMutex.Lock()
			s.serverStatus = ServerStatus_FOLLOWER
			s.serverStatusMutex.Unlock()
		}

		s.raftStateMutex.Lock()
		s.term = input.Term
		s.raftStateMutex.Unlock()
		myTerm = input.Term
	}

	s.raftStateMutex.Lock()

	// Reply false if no matched index
	if !s.isPrevLogMatched(input.PrevLogIndex, input.PrevLogTerm) {
		s.raftStateMutex.Unlock()
		return s.makeAppendEntryOutput(myTerm, myId, false, -1), nil
	}

	// Replicate log
	s.log = append(s.log[:input.PrevLogIndex+1], input.Entries...)

	// Update commit index
	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = min(input.LeaderCommit, int64(len(s.log)-1))
	}

	// Apply to state machine
	for s.lastApplied < s.commitIndex {
		nextToApply := s.lastApplied + 1
		nextEntry := s.log[nextToApply]
		_, err := s.metaStore.UpdateFile(ctx, nextEntry.FileMetaData)
		if err != nil {
			s.raftStateMutex.Unlock()
			return nil, err
		}
		s.lastApplied = nextToApply
	}

	s.raftStateMutex.Unlock()

	return s.makeAppendEntryOutput(myTerm, myId, true, int64(len(s.log)-1)), nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.serverStatusMutex.RLock()
	serverStatus := s.serverStatus
	s.serverStatusMutex.RUnlock()

	if serverStatus == ServerStatus_CRASHED {
		return &Success{Flag: false}, ErrServerCrashed
	}

	s.serverStatusMutex.Lock()
	s.serverStatus = ServerStatus_LEADER
	log.Printf("Server %d has been set as a leader", s.id)
	s.serverStatusMutex.Unlock()

	s.raftStateMutex.Lock()
	s.term += 1
	s.raftStateMutex.Unlock()

	//TODO: update the state

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// if err := s.checkStatus(); err != nil {
	// 	return nil, err
	// }

	s.raftStateMutex.RLock()
	reqId := len(s.pendingRequests) - 1
	s.raftStateMutex.RUnlock()

	s.sendPersistentHeartbeats(ctx, int64(reqId))

	return &Success{Flag: true}, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) MakeServerUnreachableFrom(ctx context.Context, servers *UnreachableFromServers) (*Success, error) {
	s.raftStateMutex.Lock()
	for _, serverId := range servers.ServerIds {
		s.unreachableFrom[serverId] = true
	}
	log.Printf("Server %v is unreachable from", s.unreachableFrom)
	s.raftStateMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.serverStatusMutex.Lock()
	s.serverStatus = ServerStatus_CRASHED
	log.Printf("Server %d is crashed", s.id)
	s.serverStatusMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.serverStatusMutex.Lock()
	s.serverStatus = ServerStatus_FOLLOWER
	s.serverStatusMutex.Unlock()

	s.raftStateMutex.Lock()
	s.unreachableFrom = make(map[int64]bool)
	s.raftStateMutex.Unlock()

	log.Printf("Server %d is restored to follower and reachable from all servers", s.id)

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.serverStatusMutex.RLock()
	s.raftStateMutex.RLock()
	state := &RaftInternalState{
		Status:      s.serverStatus,
		Term:        s.term,
		CommitIndex: s.commitIndex,
		Log:         s.log,
		MetaMap:     fileInfoMap,
	}
	s.raftStateMutex.RUnlock()
	s.serverStatusMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
