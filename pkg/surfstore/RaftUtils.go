package surfstore

import (
	"bufio"
	context "context"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RaftConfig struct {
	RaftAddrs  []string
	BlockAddrs []string
}

func LoadRaftConfigFile(filename string) (cfg RaftConfig) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	decoder := json.NewDecoder(configReader)

	if err := decoder.Decode(&cfg); err == io.EOF {
		return
	} else if err != nil {
		log.Fatal(err)
	}
	return
}

func NewRaftServer(id int64, config RaftConfig) (*RaftSurfstore, error) {
	conns := make([]*grpc.ClientConn, 0)
	for _, addr := range config.RaftAddrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		conns = append(conns, conn)
	}

	serverStatusMutex := sync.RWMutex{}
	raftStateMutex := sync.RWMutex{}

	server := RaftSurfstore{
		serverStatus:      ServerStatus_FOLLOWER,
		serverStatusMutex: &serverStatusMutex,

		id:   id,
		term: 0,

		log:            make([]*UpdateOperation, 0),
		metaStore:      NewMetaStore(config.BlockAddrs),
		commitIndex:    -1,
		raftStateMutex: &raftStateMutex,

		rpcConns:   conns,
		grpcServer: grpc.NewServer(),

		/*--------------- Added --------------*/
		n: len(config.RaftAddrs),
		m: len(config.RaftAddrs)/2 + 1,

		lastApplied:      -1,
		nextIndex:        make([]int64, len(config.RaftAddrs)),
		matchIndex:       make([]int64, len(config.RaftAddrs)),
		pendingResponses: make(map[int64]*Response),

		peers: config.RaftAddrs,

		unreachableFrom: make(map[int64]bool),
	}

	return &server, nil
}

func ServeRaftServer(server *RaftSurfstore) error {
	RegisterRaftSurfstoreServer(server.grpcServer, server)
	l, e := net.Listen("tcp", server.peers[server.id])
	if e != nil {
		return e
	}
	return server.grpcServer.Serve(l)
}

func (s *RaftSurfstore) checkStatus(isFromLeader bool, leaderId int64) (ServerStatus, error) {
	s.serverStatusMutex.RLock()
	myStatus := s.serverStatus
	s.serverStatusMutex.RUnlock()

	// If crashed
	if myStatus == ServerStatus_CRASHED {
		return myStatus, ErrServerCrashed
	}

	if isFromLeader {
		// If from leader, check unreachablity
		s.raftStateMutex.RLock()
		isUnreachable := s.unreachableFrom[leaderId]
		s.raftStateMutex.RUnlock()
		if isUnreachable {
			return myStatus, ErrServerCrashedUnreachable
		}
	} else {
		// If from client, check leadership
		if myStatus != ServerStatus_LEADER {
			return myStatus, ErrNotLeader
		}
	}

	return myStatus, nil
}

func (s *RaftSurfstore) makeAppendEntryOutput(term int64, serverId int64, success bool, matchedIndex int64) *AppendEntryOutput {
	return &AppendEntryOutput{
		Term:         term,
		ServerId:     serverId,
		Success:      success,
		MatchedIndex: matchedIndex,
	}
}

// Locked
func (s *RaftSurfstore) isPrevLogMatched(prevLogIndex int64, prevLogTerm int64) bool {
	myPrevLogIndex := int64(len(s.log) - 1)
	if myPrevLogIndex < prevLogIndex {
		return false
	}
	if prevLogIndex < 0 {
		return true
	}
	myPrevLogTerm := s.log[prevLogIndex].Term
	return myPrevLogTerm == prevLogTerm
}

// Locked
func (s *RaftSurfstore) mergeLog(nextLogIndex int64, newEntries []*UpdateOperation) {
	myLogLength := int64(len(s.log))
	newEntiresLength := int64(len(newEntries))

	// If new entries are longer
	if nextLogIndex+newEntiresLength >= myLogLength {
		s.log = append(s.log[:nextLogIndex], newEntries...)
		return
	}

	// Check if existing entries are equal
	myEntires := s.log[nextLogIndex : nextLogIndex+newEntiresLength]
	entriesEqual := true
	for i := range myEntires {
		if myEntires[i].Term != newEntries[i].Term {
			entriesEqual = false
			break
		}
	}

	// If already contains the same entries
	if entriesEqual {
		return
	}

	// If not equal
	s.log = append(s.log[:nextLogIndex], newEntries...)
}

// Locked
func (s *RaftSurfstore) initLeaderStates() {
	s.nextIndex = make([]int64, s.n)
	for i := range s.nextIndex {
		s.nextIndex[i] = int64(len(s.log))
	}
	s.matchIndex = make([]int64, s.n)
	for i := range s.matchIndex {
		s.matchIndex[i] = -1
	}
	s.pendingResponses = make(map[int64]*Response)
}

func (s *RaftSurfstore) sendPersistentHeartbeats(ctx context.Context) bool {
	peerResults := make(chan bool, s.n-1)

	for peerId := range s.peers {
		peerId := int64(peerId)
		if peerId == s.id {
			continue
		}
		go s.mustSendToFollower(ctx, peerId, peerResults)
	}

	// Wait for majority
	numResults := 1
	numAliveServers := 1
	for numResults < s.n {
		peerOk := <-peerResults
		numResults++
		if peerOk {
			numAliveServers++
		}
		if numAliveServers >= s.m {
			break
		}
	}

	if numAliveServers >= s.m {
		// If majority, can commit no matter whether reverted to follower
		return true
	} else {
		// If not majority, reverted to follower
		return false
	}
}

func (s *RaftSurfstore) mustSendToFollower(ctx context.Context, peerId int64, peerResult chan<- bool) {
	client := NewRaftSurfstoreClient(s.rpcConns[peerId])

	// Get the latest append entry input
	s.raftStateMutex.RLock()
	myTerm := s.term
	appendEntryInput := s.makeAppendEntryInput(peerId)
	s.raftStateMutex.RUnlock()

	// Make PRC
	output, err := client.AppendEntries(ctx, appendEntryInput)

	for {
		if err != nil {
			// If error, retry
			time.Sleep(100 * time.Millisecond)
			output, err = client.AppendEntries(ctx, appendEntryInput)
		} else if output.Success {
			// If successful, update next index and match index
			s.raftStateMutex.Lock()
			s.nextIndex[peerId] = output.MatchedIndex + 1
			s.matchIndex[peerId] = output.MatchedIndex
			s.raftStateMutex.Unlock()
			peerResult <- true
			break
		} else if output.Term > myTerm {
			// If I am a stale leader, revert to follower
			s.serverStatusMutex.Lock()
			s.serverStatus = ServerStatus_FOLLOWER
			s.serverStatusMutex.Unlock()
			s.raftStateMutex.Lock()
			s.term = output.Term
			s.raftStateMutex.Unlock()
			peerResult <- false
			break
		} else {
			// If log inconsistency, decrement next index and retry
			s.raftStateMutex.Lock()
			s.nextIndex[peerId] -= 1
			appendEntryInput = s.makeAppendEntryInput(peerId)
			s.raftStateMutex.Unlock()
			output, err = client.AppendEntries(ctx, appendEntryInput)
		}
	}
}

// Locked
func (s *RaftSurfstore) makeAppendEntryInput(peerId int64) *AppendEntryInput {
	peerNextIndex := s.nextIndex[peerId]
	prevLogTerm := int64(0)
	if peerNextIndex > 0 {
		prevLogTerm = s.log[peerNextIndex-1].Term
	}
	appendEntryInput := &AppendEntryInput{
		Term:         s.term,
		LeaderId:     s.id,
		PrevLogTerm:  prevLogTerm,
		PrevLogIndex: peerNextIndex - 1,
		Entries:      s.log[peerNextIndex:],
		LeaderCommit: s.commitIndex,
	}
	return appendEntryInput
}
