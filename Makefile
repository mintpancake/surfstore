GOBIN = $(CURDIR)/bin
TEST_GOBIN = $(CURDIR)/test/_bin

.PHONY: install
install:
	$(shell powershell -Command "if (Test-Path '$(GOBIN)') { Remove-Item -Recurse -Force '$(GOBIN)' }")
	export GOBIN="$(GOBIN)" && go install ./...

.PHONY: run-blockstore
run-blockstore:
	go run cmd/SurfstoreServerExec/main.go -s block -p $(PORT)

.PHONY: run-raft
run-raft:
	go run cmd/SurfstoreRaftServerExec/main.go -f example_config.txt -i $(IDX)

.PHONY: test
test:
	$(shell powershell -Command "if (Test-Path '$(TEST_GOBIN)') { Remove-Item -Recurse -Force '$(TEST_GOBIN)' }")
	export GOBIN="$(TEST_GOBIN)" && go get github.com/mattn/go-sqlite3
	export GOBIN="$(TEST_GOBIN)" && go install ./...
	go test -v ./test/...

.PHONY: fast-test
fast-test:
	go test -v ./test/...

.PHONY: specific-test
specific-test:
	$(shell powershell -Command "if (Test-Path '$(TEST_GOBIN)') { Remove-Item -Recurse -Force '$(TEST_GOBIN)' }")
	export GOBIN="$(TEST_GOBIN)" && go get github.com/mattn/go-sqlite3
	export GOBIN="$(TEST_GOBIN)" && go install ./...
	go test -v -run $(TEST_REGEX) -count=1 ./test/...

.PHONY: fast-specific-test
fast-specific-test:
	go test -v -run $(TEST_REGEX) -count=1 ./test/...

.PHONY: clean
clean:
	$(shell powershell -Command "if (Test-Path '$(GOBIN)') { Remove-Item -Recurse -Force '$(GOBIN)' }")
	$(shell powershell -Command "if (Test-Path '$(TEST_GOBIN)') { Remove-Item -Recurse -Force '$(TEST_GOBIN)' }")
