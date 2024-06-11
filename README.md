# Instructions to start project 5
1. Install the protocol compiler plugins for Go
```
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28

go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2
```
2. Update your PATH so that the protoc compiler can find the plugins:
```
export PATH="$PATH:$(go env GOPATH)/bin"
```
3. Re-generate the protobuf
```console
protoc --proto_path=. --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative pkg/surfstore/SurfStore.proto
```
4. Copy over code to Blockstore.go, Metastore.go, ConsistentHashRing.go, SurfstoreHelper.go, SurfstoreUtils.go, and main.go inside SurfstoreServerExec from your Project 4. 

## SurfstoreRPCClient.go
MetaStore functionality is now provided by the RaftSurfstoreServer, so change the MetaStore clients to RaftSurfstoreServer clients:

```go
c := NewRaftSurfstoreClient(conn)
```

And since we no longer have the `MetaStoreAddr` field, for now you can change `surfclient.MetaStoreAddr` to `surfclient.MetaStoreAddrs[0]`. You will eventually need to change this so you can find a leader, deal with server crashes, etc. 
```go
conn, err := grpc.Dial(surfClient.MetaStoreAddrs[0], grpc.WithTransportCredentials(insecure.NewCredentials()))
```


You should now be able to run `make test` and it will fail with the panic messages.


## Makefile

Run BlockStore server:
```console
$ make run-blockstore
```

Run RaftSurfstore server:
```console
$ make IDX=0 run-raft
```

Test:
```console
$ make test
```

Specific Test:
```console
$ make TEST_REGEX=Test specific-test
```

Clean:
```console
$ make clean
```
