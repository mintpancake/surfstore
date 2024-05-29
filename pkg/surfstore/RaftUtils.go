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
	// TODO Any initialization you need here
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

		lastApplied: -1,

		peers:           config.RaftAddrs,
		pendingRequests: make([]*chan PendingRequest, 0),

		unreachableFrom: make(map[int64]bool),
	}

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	RegisterRaftSurfstoreServer(server.grpcServer, server)
	l, e := net.Listen("tcp", server.peers[server.id])
	if e != nil {
		return e
	}
	return server.grpcServer.Serve(l)
}

func (s *RaftSurfstore) checkStatus(myStatus ServerStatus, isFromLeader bool, leaderId int64) error {
	// If crashed
	if myStatus == ServerStatus_CRASHED {
		return ErrServerCrashed
	}

	if isFromLeader {
		// If from leader, check unreachablity
		s.raftStateMutex.RLock()
		isUnreachableFromLeader := s.unreachableFrom[leaderId]
		s.raftStateMutex.RUnlock()
		if isUnreachableFromLeader {
			return ErrServerCrashedUnreachable
		}
	} else {
		// If from client, check leadership
		if myStatus != ServerStatus_LEADER {
			return ErrNotLeader
		}
	}

	return nil
}

func (s *RaftSurfstore) makeAppendEntryOutput(term int64, serverId int64, success bool, matchedIndex int64) *AppendEntryOutput {
	return &AppendEntryOutput{
		Term:         term,
		ServerId:     serverId,
		Success:      success,
		MatchedIndex: matchedIndex,
	}
}

// locked
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

func (s *RaftSurfstore) sendPersistentHeartbeats(ctx context.Context, reqId int64) {
	peerResponses := make(chan bool, s.n-1)

	for peerId := range s.peers {
		peerId := int64(peerId)
		if peerId == s.id {
			continue
		}

		entriesToSend := make([]*UpdateOperation, 0)

		//TODO: Utilize next index

		go s.sendToFollower(ctx, peerId, entriesToSend, peerResponses)
	}

	totalResponses := 1
	numAliveServers := 1
	for totalResponses < s.n {
		response := <-peerResponses
		totalResponses += 1
		if response {
			numAliveServers += 1
		}
	}

	if numAliveServers >= s.m {
		s.raftStateMutex.RLock()
		requestLen := int64(len(s.pendingRequests))
		s.raftStateMutex.RUnlock()

		if reqId >= 0 && reqId < requestLen {
			s.raftStateMutex.Lock()
			*s.pendingRequests[reqId] <- PendingRequest{success: true, err: nil}
			s.pendingRequests = append(s.pendingRequests[:reqId], s.pendingRequests[reqId+1:]...)
			s.raftStateMutex.Unlock()
		}
	}
}

func (s *RaftSurfstore) sendToFollower(ctx context.Context, peerId int64, entries []*UpdateOperation, peerResponses chan<- bool) {
	client := NewRaftSurfstoreClient(s.rpcConns[peerId])

	s.raftStateMutex.RLock()
	appendEntriesInput := AppendEntryInput{
		Term:         s.term,
		LeaderId:     s.id,
		PrevLogTerm:  0,
		PrevLogIndex: -1,
		Entries:      entries,
		LeaderCommit: s.commitIndex,
	}
	s.raftStateMutex.RUnlock()

	reply, err := client.AppendEntries(ctx, &appendEntriesInput)
	log.Println("Server", s.id, ": Receiving output:", "Term", reply.Term, "Id", reply.ServerId, "Success", reply.Success, "Matched Index", reply.MatchedIndex)

	// TODO: Handle reply

	if err != nil {
		peerResponses <- false
	} else {
		peerResponses <- true
	}
}
