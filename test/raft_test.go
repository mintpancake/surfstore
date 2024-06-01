package SurfTest

import (
	"cse224/proj5/pkg/surfstore"
	"fmt"
	"testing"
	"time"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

func TestRaftSetLeader(t *testing.T) {
	cfgPath := "./config_files/6nodes.json"
	test := InitTest(cfgPath)
	defer EndTest(test)

	leaderIdx := 0
	res, err := test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	t.Logf("%d ||| %v ||| %v\n", leaderIdx, res, err)
	res, err = test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})
	t.Logf("%d ||| %v ||| %v\n", leaderIdx, res, err)
	time.Sleep(100 * time.Millisecond)

	for id, server := range test.Clients {
		res, err := server.GetInternalState(test.Context, &emptypb.Empty{})
		t.Logf("%d ||| %v ||| %v\n", id, res, err)
	}

	leaderIdx = 1
	res, err = test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	t.Logf("%d ||| %v ||| %v\n", leaderIdx, res, err)
	res, err = test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})
	t.Logf("%d ||| %v ||| %v\n", leaderIdx, res, err)
	time.Sleep(100 * time.Millisecond)

	for id, server := range test.Clients {
		res, err := server.GetInternalState(test.Context, &emptypb.Empty{})
		t.Logf("%d ||| %v ||| %v\n", id, res, err)
	}

	leaderIdx = 2
	res, err = test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	t.Logf("%d ||| %v ||| %v\n", leaderIdx, res, err)
	res, err = test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})
	t.Logf("%d ||| %v ||| %v\n", leaderIdx, res, err)
	time.Sleep(100 * time.Millisecond)

	for id, server := range test.Clients {
		res, err := server.GetInternalState(test.Context, &emptypb.Empty{})
		t.Logf("%d ||| %v ||| %v\n", id, res, err)
	}
}

func TestRaftFollowersGetUpdates(t *testing.T) {
	cfgPath := "./config_files/6nodes.json"
	test := InitTest(cfgPath)
	defer EndTest(test)

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
	for id, server := range test.Clients {
		res, err := server.GetInternalState(test.Context, &emptypb.Empty{})
		t.Logf("%d ||| %v ||| %v\n", id, res, err)
	}
	fmt.Println()

	leaderIdx = 1
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	filemeta2 := &surfstore.FileMetaData{
		Filename:      "testFile2",
		Version:       1,
		BlockHashList: nil,
	}
	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta2)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})
	for id, server := range test.Clients {
		res, err := server.GetInternalState(test.Context, &emptypb.Empty{})
		t.Logf("%d ||| %v ||| %v\n", id, res, err)
	}
	fmt.Println()
}

func TestRaftLogsConsistentLeaderCrashesBeforeHeartbeat(t *testing.T) {
	cfgPath := "./config_files/6nodes.json"
	test := InitTest(cfgPath)
	defer EndTest(test)

	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	time.Sleep(200 * time.Millisecond)
	// test.Clients[leaderIdx].Crash(test.Context, &emptypb.Empty{})
	// time.Sleep(200 * time.Millisecond)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})
	time.Sleep(200 * time.Millisecond)
	for id, server := range test.Clients {
		res, err := server.GetInternalState(test.Context, &emptypb.Empty{})
		t.Logf("%d ||| %v ||| %v\n", id, res, err)
	}
	fmt.Println()
	time.Sleep(200 * time.Millisecond)

	// leaderIdx = 1
	// test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	// time.Sleep(200 * time.Millisecond)
	// test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})
	// time.Sleep(200 * time.Millisecond)
	// for id, server := range test.Clients {
	// 	res, err := server.GetInternalState(test.Context, &emptypb.Empty{})
	// 	t.Logf("%d ||| %v ||| %v\n", id, res, err)
	// }
	// fmt.Println()

	// test.Clients[0].Restore(test.Context, &emptypb.Empty{})
	// time.Sleep(200 * time.Millisecond)

	// for id, server := range test.Clients {
	// 	res, err := server.GetInternalState(test.Context, &emptypb.Empty{})
	// 	t.Logf("%d ||| %v ||| %v\n", id, res, err)
	// }
	// fmt.Println()
}
