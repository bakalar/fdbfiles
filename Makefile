all: fdbfiles test

fdbfiles: fdbfiles.go
	@go build fdbfiles.go

test:
	@go test -bench=.

clean:
	@rm -f fdbfiles
