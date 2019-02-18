package main

import (
	"testing"

	"gopkg.in/mgo.v2/bson"
)

var bin []byte

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

func TestIdWithLoadBalancingPrefix(t *testing.T) {
	if len(idWithLoadBalancingPrefix(bson.NewObjectId())) != 13 {
		t.Fail()
	}
}

func BenchmarkIdWithLoadBalancingPrefix(b *testing.B) {
	_id := bson.NewObjectId()
	var b1 []byte
	for i := 0; i < b.N; i++ {
		b1 = idWithLoadBalancingPrefix(_id)
	}
	bin = b1
}
