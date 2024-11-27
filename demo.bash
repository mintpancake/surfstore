# See config.txt

# Start blockstore servers
go run cmd/SurfstoreServerExec/main.go -s block -p 8080 -l
go run cmd/SurfstoreServerExec/main.go -s block -p 8081 -l

# Start metastore servers
go run cmd/SurfstoreRaftServerExec/main.go -f config.json -i 0
go run cmd/SurfstoreRaftServerExec/main.go -f config.json -i 1
go run cmd/SurfstoreRaftServerExec/main.go -f config.json -i 2

# Start client
go run cmd/SurfstoreClientExec/main.go -f config.json "demo/Machine A" 4096
go run cmd/SurfstoreClientExec/main.go -f config.json "demo/Machine B" 4096