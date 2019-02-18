package main

import "testing"

func TestLengthToChunkCount(t *testing.T) {
	if lengthToChunkCount(0) != 0 {
		t.Fail()
	}
	if lengthToChunkCount(1) != 1 {
		t.Fail()
	}
	if lengthToChunkCount(chunkSize-1) != 1 {
		t.Fail()
	}
	if lengthToChunkCount(chunkSize) != 1 {
		t.Fail()
	}
	if lengthToChunkCount(chunkSize+1) != 2 {
		t.Fail()
	}
	if lengthToChunkCount(5*chunkSize-1) != 5 {
		t.Fail()
	}
	if lengthToChunkCount(5*chunkSize) != 5 {
		t.Fail()
	}
	if lengthToChunkCount(5*chunkSize+1) != 6 {
		t.Fail()
	}
}
