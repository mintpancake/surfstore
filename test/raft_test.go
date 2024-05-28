package SurfTest

import (
	"cse224/proj5/pkg/surfstore"
	"testing"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

func TestRaftSetLeader(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state == nil {
			t.Fatalf("Could not get state")
		}
		if state.Term != int64(1) {
			t.Fatalf("Server %d should be in term %d", idx, 1)
		}
		if idx == leaderIdx {
			// server should be the leader
			if state.Status != surfstore.ServerStatus_LEADER {
				t.Fatalf("Server %d should be the leader", idx)
			}
		} else {
			// server should not be the leader
			if state.Status == surfstore.ServerStatus_LEADER {
				t.Fatalf("Server %d should not be the leader", idx)
			}
		}
	}

	leaderIdx = 2
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state == nil {
			t.Fatalf("Could not get state")
		}
		if state.Term != int64(2) {
			t.Fatalf("Server should be in term %d", 2)
		}
		if idx == leaderIdx {
			// server should be the leader
			if state.Status != surfstore.ServerStatus_LEADER {
				t.Fatalf("Server %d should be the leader", idx)
			}
		} else {
			// server should not be the leader
			if state.Status == surfstore.ServerStatus_LEADER {
				t.Fatalf("Server %d should not be the leader", idx)
			}
		}
	}
}

func TestRaftFollowersGetUpdates(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testFile1",
		Version:       1,
		BlockHashList: nil,
	}

	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta1)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	goldenMeta := make(map[string]*surfstore.FileMetaData)
	goldenMeta[filemeta1.Filename] = filemeta1

	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta1,
	})

	var leader bool
	term := int64(1)

	for idx, server := range test.Clients {
		if idx == leaderIdx {
			leader = bool(true)
		} else {
			leader = bool(false)
		}
		_, err := CheckInternalState(&leader, &term, goldenLog, goldenMeta, server, test.Context)
		if err != nil {
			t.Fatalf("Error checking state for server %d: %s", idx, err.Error())
		}
	}
}
