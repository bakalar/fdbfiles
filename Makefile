fdbfiles: test
	@go build fdbfiles.go

test:
	@go test -bench=.
