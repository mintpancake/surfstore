GOBIN = D:\Local\Workspace\CSE224\project-5-mintpancake\test\_bin

.PHONY: install
install:
	rm -rf bin
	GOBIN=$(PWD)/bin go install ./...

.PHONY: run-blockstore
run-blockstore:
	go run cmd/SurfstoreServerExec/main.go -s block -p 8081 -l

.PHONY: run-raft
run-raft:
	go run cmd/SurfstoreRaftServerExec/main.go -f example_config.txt -i $(IDX)

.PHONY: test
test:
	$(shell powershell -Command "if (Test-Path '$(GOBIN)') { Remove-Item -Recurse -Force '$(GOBIN)' }")
	export GOBIN="$(GOBIN)" && go install ./...
	go test -v ./test/...

.PHONY: specific-test
specific-test:
	$(shell powershell -Command "if (Test-Path '$(GOBIN)') { Remove-Item -Recurse -Force '$(GOBIN)' }")
	export GOBIN="$(GOBIN)" && go install ./...
	go test -v -run $(TEST_REGEX) -count=1 ./test/...

.PHONY: clean
clean:
	rm -rf bin/ test/_bin
