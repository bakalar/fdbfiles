// fdbfiles CLI tool
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/google/uuid"
	"github.com/hungys/go-lz4"
	"gopkg.in/mgo.v2/bson"
)

const (
	chunkSize            = 1e4
	chunksPerTransaction = 999

	compressionAlgorithmAuto = -1
	compressionAlgorithmNone = 0
	compressionAlgorithmLZ4  = 1
)

var (
	one                   = []byte{1, 0, 0, 0, 0, 0, 0, 0}
	objectPath            = []string{"object"}
	nameIndexDirPrefix    = []string{"object", "index", "name"}
	compressionAlgorithms = []string{"none", "lz4"}
)

func lengthToChunkCount(length int64) int64 {
	count := length / chunkSize
	if length%chunkSize != 0 {
		count++
	}
	return count
}

// To be stored in a key before id itself.
// This avoids partitioning hot spot.
// Ids generated at the same second on the same machine will use the same prefix.
func loadBalancingPrefix(id bson.ObjectId) byte {
	machine := id.Machine()

	// Machine is stored in upper 4 bits:
	b := (machine[0] ^ machine[1] ^ machine[2]) & 0xf0

	// Seconds are stored in lower 4 bits:
	b = b | (byte(id.Time().Second()) & 0x0f)

	return b
}

// Takes ObjectId as input and adds one byte prefix befure returning
func idWithLoadBalancingPrefix(id bson.ObjectId) []byte {
	bytes := make([]byte, 13)
	bytes[0] = loadBalancingPrefix(id)
	copy(bytes[1:], id)
	return bytes
}

func percentFromWrittenAndSize(written, size int64) int {
	var percent int
	if size > 0 {
		percent = int(100 * written / size)
	} else {
		percent = 100
	}
	return percent
}

// Calculate compression ratio
func ratioFromWrittenAndCompressed(written, compressed int64) float64 {
	var ratio float64
	if compressed > 0 {
		ratio = float64(written) / float64(compressed)
	} else {
		ratio = 1
	}
	return ratio
}

// Is filename a type of file that can be compressed?
func doCompress(filename string) bool {
	switch strings.ToLower(filepath.Ext(filename)) {
	case
		".7z",
		".bz2",
		".deb",
		".dmg",
		".docx",
		".docm",
		".dotx",
		".dotm",
		".gif",
		".gz",
		".jar",
		".jpg",
		".jpeg",
		".mp4",
		".opus",
		".pdf",
		".png",
		".pptx",
		".pptm",
		".potx",
		".potm",
		".ppam",
		".ppsx",
		".ppsm",
		".rar",
		".tbz2",
		".tgz",
		".txz",
		".xlsx",
		".xlsm",
		".xltx",
		".xltm",
		".xlam",
		".xz",
		".vsdx",
		".vsdm",
		".vssx",
		".vssm",
		".vstx",
		".vstm",
		".vsix",
		".zip":
		return false
	default:
		return true
	}
}

// Print usage information
func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [OPTION]... [COMMAND [NAME_OR_ID...]]\n\n", os.Args[0])
	fmt.Fprintln(os.Stderr, "Manipulate FoundationDB object store using the command line.")
	fmt.Fprintln(os.Stderr, "\nCommands:")
	fmt.Fprintln(os.Stderr, "\tlist\t\tlist objects; NAME is an optional prefix which listed objects must begin with")
	fmt.Fprintln(os.Stderr, "\tput\t\tadd objects with given names")
	fmt.Fprintln(os.Stderr, "\tput_id\t\tadd objects with given ids")
	fmt.Fprintln(os.Stderr, "\tget\t\tget objects with given names")
	fmt.Fprintln(os.Stderr, "\tget_id\t\tget objects with given ids")
	fmt.Fprintln(os.Stderr, "\tresume\t\tresume adding objects with given ids - useful to continue a partial upload")
	fmt.Fprintln(os.Stderr, "\tdelete\t\tdelete all objects with given names")
	fmt.Fprintln(os.Stderr, "\tdelete_id\tdelete objects with given ids")
	fmt.Fprintln(os.Stderr, "\nOptions:")
	flag.PrintDefaults()
}

func uniqueID(bytes []byte) (id any, err error) {
	idIsBSONObjectID := len(bytes) == 12
	if idIsBSONObjectID {
		oid := bson.ObjectId(bytes)
		id = idWithLoadBalancingPrefix(oid)
	} else {
		id = tuple.UUID(bytes)
	}
	return
}

func uniqueIDFromString(s string) (id any, err error) {
	idIsBSONObjectID := len(s) == 24
	if idIsBSONObjectID {
		return uniqueID([]byte(bson.ObjectIdHex(s)))
	}

	var u uuid.UUID
	u, err = uuid.Parse(s)
	if err == nil {
		return uniqueID(u[:])
	}

	return
}

// List objects in object store
func list(db fdb.Database, transactionTimeout int64, allBuckets bool, bucketName string, prefix string) {
	_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Options().SetTimeout(transactionTimeout)
		var bucketNames []string
		var err error
		if allBuckets {
			bucketNames, err = directory.List(tr, nameIndexDirPrefix)
			if err != nil {
				panic(err)
			}
		} else {
			bucketNames = []string{bucketName}
		}
		objectDir, err := directory.Open(tr, objectPath, nil)
		if err != nil {
			panic(err)
		}
		for _, bucketName1 := range bucketNames {
			indexDir, err := directory.Open(tr, append(nameIndexDirPrefix, bucketName1), nil)
			if err != nil {
				panic(err)
			}

			prefixKey := indexDir.Bytes()
			if len(prefix) > 0 {
				prefixKey = append(prefixKey, 0x02)
				prefixKey = append(prefixKey, prefix...)
			}
			range2, err := fdb.PrefixRange(prefixKey)
			if err != nil {
				panic(err)
			}
			ri := tr.GetRange(range2, fdb.RangeOptions{}).Iterator()

			for ri.Advance() {
				kv := ri.MustGet()
				t, _ := tuple.Unpack(kv.Key)
				if t[2] != "count" {
					id, err := uniqueID(kv.Value)
					if err != nil {
						panic(err)
					}

					lengthKey := objectDir.Pack(tuple.Tuple{id, "len"})
					lengthValueFuture := tr.Get(lengthKey)

					partialKey := objectDir.Pack(tuple.Tuple{id, "partial"})
					partialValueFuture := tr.Get(partialKey)

					uploadDateKey := objectDir.Pack(tuple.Tuple{id, "meta", "uploadDate"})
					uploadDateFuture := tr.Get(uploadDateKey)

					v, err := tuple.Unpack(lengthValueFuture.MustGet())
					if err != nil {
						panic(err)
					}
					if v == nil {
						continue
					}
					length := v[0].(int64)

					var partialMark string
					if partialValueFuture.MustGet() == nil {
						partialMark = ""
					} else {
						partialMark = "*"
					}

					var uploadDate time.Time
					uploadDateValue := uploadDateFuture.MustGet()
					if uploadDateValue != nil {
						v, err = tuple.Unpack(uploadDateValue)
						if v == nil || err != nil {
							uploadDateValue = nil
						} else {
							ns := v[0].(int64)
							uploadDate = time.Unix(ns/1e9, ns%1e9)
						}
					}

					var oidPtr *bson.ObjectId
					if _, ok := id.(tuple.UUID); !ok {
						oid := bson.ObjectId(id.([]byte)[1:])
						oidPtr = &oid
						id = oid
					}
					if uploadDateValue == nil {
						uploadDate = oidPtr.Time()
					}
					name := t[1]
					revision := t[2].(int64)
					fmt.Printf("%s %s\t%d\t%s %d\t%s%s\n", id, bucketName1, revision, uploadDate.Format("2006-01-02T15:04:05.000000000-0700"), length, name, partialMark)
				}
			}
		}
		return nil, nil
	})
	if e != nil {
		panic(e)
	}
}

// Remove objects from objects store using their names
func remove(db fdb.Database, transactionTimeout int64, batchPriority bool, bucketName string, names []string, finishChannel chan bool) {
	indexDirPath := append(nameIndexDirPrefix, bucketName)
	for _, name1 := range names {
		go func(name string) {
			_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
				tr.Options().SetTimeout(transactionTimeout)
				if batchPriority {
					tr.Options().SetPriorityBatch()
				}
				indexDir, err := directory.Open(tr, indexDirPath, nil)
				if err != nil {
					panic(err)
				}

				objectDir, err := directory.Open(tr, objectPath, nil)
				if err != nil {
					panic(err)
				}

				indexPrefixKey := indexDir.Pack(tuple.Tuple{name})
				indexPrefixRange, err := fdb.PrefixRange(indexPrefixKey)
				if err != nil {
					panic(err)
				}

				startKey := indexDir.Pack(tuple.Tuple{name, 0})
				endKey := indexDir.Pack(tuple.Tuple{name, math.MaxInt64})
				keyRange := fdb.KeyRange{
					Begin: startKey,
					End:   endKey,
				}
				ri := tr.GetRange(keyRange, fdb.RangeOptions{}).Iterator()
				for ri.Advance() {
					kv := ri.MustGet()
					id, err := uniqueID(kv.Value)
					if err != nil {
						panic(err)
					}
					objectPrefixKey := objectDir.Pack(tuple.Tuple{id})
					objectPrefixRange, err := fdb.PrefixRange(objectPrefixKey)
					if err != nil {
						continue // Object already deleted.
					}
					tr.ClearRange(objectPrefixRange)
				}
				tr.ClearRange(indexPrefixRange)

				return nil, nil
			})
			if e != nil {
				panic(e)
			}
			finishChannel <- true
		}(name1)
	}
}

// Remove objects from objects store using their unique identifiers
func removeID(db fdb.Database, transactionTimeout int64, batchPriority bool, ids []string, finishChannel chan bool) {
	for _, id1 := range ids {
		go func(idString string) {
			id, err := uniqueIDFromString(idString)
			if err != nil {
				panic(err)
			}
			_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
				tr.Options().SetTimeout(transactionTimeout)
				if batchPriority {
					tr.Options().SetPriorityBatch()
				}
				objectDir, err := directory.Open(tr, objectPath, nil)
				if err != nil {
					panic(err)
				}

				bucketNameFuture := tr.Get(objectDir.Pack(tuple.Tuple{id, "bucket"}))
				nameFuture := tr.Get(objectDir.Pack(tuple.Tuple{id, "name"}))
				ndxFuture := tr.Get(objectDir.Pack(tuple.Tuple{id, "ndx"}))

				bucketName := string(bucketNameFuture.MustGet())
				indexDir, err := directory.Open(tr, append(nameIndexDirPrefix, bucketName), nil)
				if err != nil {
					panic(err)
				}

				name := string(nameFuture.MustGet())
				countKey := indexDir.Pack(tuple.Tuple{name, "count"})
				objectCountFuture := tr.Get(countKey)

				_index, err := tuple.Unpack(ndxFuture.MustGet())
				index := uint64(_index[0].(int64))
				firstUnusedIndex := binary.LittleEndian.Uint64(objectCountFuture.MustGet())

				if index+1 == firstUnusedIndex {
					// Need to reduce count so that (count - 1) always points to last used object version.
					for i := index - 1; ; i-- {
						previousVersionKey := indexDir.Pack(tuple.Tuple{name, int64(i)})
						previousVersionFuture := tr.Get(previousVersionKey)
						if previousVersionFuture.MustGet() != nil {
							// Found a previous version.
							bytes := make([]byte, 8)
							binary.LittleEndian.PutUint64(bytes, i+1)
							tr.Set(countKey, bytes)
							break
						}
						if i == 0 {
							// No other version found.
							firstUnusedIndex = 0
							break
						}
					}
				}
				if firstUnusedIndex == 0 {
					// No version left.
					tr.Clear(countKey)
				}

				objectPrefixRange, err := fdb.PrefixRange(objectDir.Pack(tuple.Tuple{id}))
				tr.ClearRange(objectPrefixRange)

				indexPrefixRange, err := fdb.PrefixRange(indexDir.Pack(tuple.Tuple{name, int64(index)}))
				tr.ClearRange(indexPrefixRange)

				return nil, nil
			})
			if e != nil {
				panic(e)
			}
			finishChannel <- true
		}(id1)
	}
}

func uncompressedTail(length int64) int {
	uncompressedSize := int(length % chunkSize)
	if uncompressedSize == 0 {
		uncompressedSize = chunkSize
	}
	return uncompressedSize
}

func readChunks(chunk, chunkCount int64, id any, tr fdb.Transaction, dir directory.DirectorySubspace, tailByteCount int, print bool, f *os.File) int64 {
	if chunk < chunkCount {
		var thisTransactionEndChunkIndex int64
		if print {
			thisTransactionEndChunkIndex = chunk + 1
		} else {
			thisTransactionEndChunkIndex = chunk + chunksPerTransaction
		}
		range2 := fdb.KeyRange{
			Begin: dir.Pack(tuple.Tuple{id, chunk}),
			End:   dir.Pack(tuple.Tuple{id, thisTransactionEndChunkIndex}),
		}
		ri := tr.GetRange(range2, fdb.RangeOptions{}).Iterator()
		var bytes []byte
		for ri.Advance() {
			kv := ri.MustGet()
			key, err := tuple.Unpack(kv.Key)
			if err != nil {
				panic(err)
			}
			thisChunk := key[2].(int64)
			if chunk+1 != thisChunk && chunk != thisChunk {
				panic("Invalid chunk.")
			}
			chunk = thisChunk
			if len(key) > 3 /*&& key[3] == "c"*/ {
				compressionAlgo := kv.Value
				var usedAlgo int64
				if len(compressionAlgo) == 0 {
					usedAlgo = compressionAlgorithmLZ4
				} else {
					v, err := tuple.Unpack(compressionAlgo)
					if err != nil {
						panic(err)
					}
					if v != nil {
						usedAlgo = v[0].(int64)
					}
				}
				switch usedAlgo {
				case compressionAlgorithmLZ4:
					var uncompressedSize int
					if chunk+1 == chunkCount {
						uncompressedSize = tailByteCount
					} else {
						uncompressedSize = chunkSize
					}
					uncompressed := make([]byte, uncompressedSize)
					_, err = lz4.DecompressFast(bytes, uncompressed, uncompressedSize)
					if err != nil {
						panic(err)
					}
					bytes = uncompressed
				}
			} else {
				if len(bytes) > 0 {
					if print {
						fmt.Print(string(bytes))
					} else {
						_, err := f.Write(bytes)
						if err != nil {
							panic(err)
						}
					}
					bytes = []byte{}
				}
				bytes = kv.Value
			}
		}
		if len(bytes) > 0 {
			if print {
				fmt.Print(string(bytes))
			} else {
				_, err := f.Write(bytes)
				if err != nil {
					panic(err)
				}
			}
			bytes = []byte{}
		}
		chunk = thisTransactionEndChunkIndex
	}
	return chunk
}

// Retrieve objects from objects store using their names
func get(localName string, db fdb.Database, transactionTimeout int64, bucketName string, names []string, allowPartial, verbose bool, finishChannel chan int64) {
	indexDirPath := append(nameIndexDirPrefix, bucketName)
	for _, name1 := range names {
		go func(name string) {
			var length int64
			var chunkCount int64
			var tailByteCount int
			var f *os.File
			var idWithPrefix []byte
			doPrint := localName == "-"
			var chunk int64
			if !doPrint {
				var path string
				if localName == "/dev/null" || (len(names) == 1 && len(localName) > 0) {
					path = localName
				} else {
					path = name
				}
				var err error
				f, err = os.Create(path)
				if err != nil {
					panic(err)
				}
				defer f.Close()
			}
			for {
				_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
					tr.Options().SetTimeout(transactionTimeout)
					dir, err := directory.Open(tr, objectPath, nil)
					if err != nil {
						panic(err)
					}
					if chunk == 0 {
						indexDir, err := directory.Open(tr, indexDirPath, nil)
						if err != nil {
							panic(err)
						}
						countTuple := tuple.Tuple{name, "count"}
						countKey := indexDir.Pack(countTuple)
						countValue := tr.Get(countKey).MustGet()
						if countValue == nil {
							return nil, nil
						}
						for count := binary.LittleEndian.Uint64(countValue); ; count-- {
							if count == 0 {
								panic("Non-partial upload not found.")
							}
							nameKey := indexDir.Pack(tuple.Tuple{name, int64(count - 1)}) // Use newest version of an object with this name.
							idFuture := tr.Get(nameKey)
							idBytes := idFuture.MustGet()
							if idBytes == nil {
								// This version doesn't exist.
								continue
							}
							id, err := uniqueID(idBytes)
							if err != nil {
								panic(err)
							}
							var isValid bool
							if allowPartial {
								isValid = true
							} else {
								partialKey := dir.Pack(tuple.Tuple{id, "partial"})
								partialValueFuture := tr.Get(partialKey)
								isValid = partialValueFuture.MustGet() == nil
							}
							if isValid {
								lengthKey := dir.Pack(tuple.Tuple{id, "len"})
								lengthFuture := tr.Get(lengthKey)
								lengthValue := lengthFuture.MustGet()
								v, err := tuple.Unpack(lengthValue)
								if err != nil {
									panic(err)
								}
								length = v[0].(int64)
								chunkCount = lengthToChunkCount(length)
								tailByteCount = uncompressedTail(length)
								break
							}
						}
					}
					chunk = readChunks(chunk, chunkCount, idWithPrefix, tr, dir, tailByteCount, doPrint, f)
					return nil, nil
				})
				if e != nil {
					panic(e)
				}
				if chunk >= chunkCount {
					break
				}
			}
			if verbose {
				fmt.Fprintf(os.Stderr, "Downloaded %s.\n", name)
			}
			finishChannel <- length
		}(name1)
	}
}

// Retrieve objects from objects store using their unique identifiers
func getID(localName string, db fdb.Database, transactionTimeout int64, ids []string, verbose bool, finishChannel chan bool) {
	for _, id1 := range ids {
		go func(idString string) {
			id, err := uniqueIDFromString(idString)
			if err != nil {
				panic(err)
			}

			var length int64
			var chunkCount int64
			var tailByteCount int
			var f *os.File
			doPrint := localName == "-"
			var chunk int64
			if !doPrint {
				var path string
				if len(ids) == 1 && len(localName) > 0 {
					path = localName
				} else {
					path = idString
				}
				var err error
				f, err = os.Create(path)
				if err != nil {
					panic(err)
				}
				defer f.Close()
			}
			timeStarted := time.Now()
			for {
				_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
					tr.Options().SetTimeout(transactionTimeout)
					dir, err := directory.Open(tr, objectPath, nil)
					if err != nil {
						panic(err)
					}

					if chunk == 0 {
						lengthKey := dir.Pack(tuple.Tuple{id, "len"})
						v, err := tuple.Unpack(tr.Get(lengthKey).MustGet())
						if err != nil {
							panic(err)
						}
						length = v[0].(int64)
						chunkCount = lengthToChunkCount(length)
						tailByteCount = uncompressedTail(length)
					}
					chunk = readChunks(chunk, chunkCount, id, tr, dir, tailByteCount, doPrint, f)
					return nil, nil
				})
				if e != nil {
					panic(e)
				}
				if chunk >= chunkCount {
					break
				}
			}
			if verbose {
				elapsed := time.Since(timeStarted)
				fmt.Fprintf(os.Stderr, "Downloaded %s (%.2fMB/s).\n", id, float64(length)/1e6/elapsed.Seconds())
			}
			finishChannel <- true
		}(id1)
	}
}

// Add objects to objects store using their names
func put(localName string, db fdb.Database, transactionTimeout int64, batchPriority bool, bucketName string, uniqueNames map[string]bool, tags map[string]string, compressionAlgorithm int, verbose bool, finishChannel chan int64) {
	indexDirPath := append(nameIndexDirPrefix, bucketName)
	for name1 := range uniqueNames {
		go func(name string) {
			var filename string
			if len(uniqueNames) == 1 && len(localName) > 0 {
				filename = localName
			} else {
				filename = name
			}
			f, err := os.Open(filename)
			if err != nil {
				panic(err)
			}
			defer f.Close()
			fi, err := f.Stat()
			if err != nil {
				panic(err)
			}
			totalSize := fi.Size()
			chunkCount := lengthToChunkCount(totalSize)
			multipleTransactions := chunkCount > chunksPerTransaction
			var totalWritten int64
			var totalWrittenCompressed int64

			var id tuple.UUID

			contentBuffer := make([]byte, chunkSize)
			var chunk int64
			var lastPercent int
			var lz4CompressedBytes []byte
			var myCompressionAlgorithm int
			if compressionAlgorithm == compressionAlgorithmAuto {
				if doCompress(filename) {
					myCompressionAlgorithm = compressionAlgorithmLZ4
				} else {
					myCompressionAlgorithm = compressionAlgorithmNone
				}
			} else {
				myCompressionAlgorithm = compressionAlgorithm
			}
			compressionAlgoValue := compressionAlgoValue(myCompressionAlgorithm)
			if myCompressionAlgorithm == compressionAlgorithmLZ4 {
				lz4CompressedBytes = make([]byte, lz4.CompressBound(chunkSize))
			}
			if verbose {
				fmt.Fprintf(os.Stderr, "Uploading %s...\n", filename)
			}
			for {
				_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
					tr.Options().SetTimeout(transactionTimeout)
					if batchPriority {
						tr.Options().SetPriorityBatch()
					}
					dir, err := directory.CreateOrOpen(tr, objectPath, nil)
					if err != nil {
						panic(err)
					}
					if chunk == 0 {
						var nameKey fdb.KeyConvertible
						for {
							id = tuple.UUID(uuid.New())
							nameKey = dir.Pack(tuple.Tuple{id, "name"})
							if tr.Get(nameKey).MustGet() == nil {
								break
							}
						}
						tr.Set(nameKey, []byte(filename))
						tr.Set(dir.Pack(tuple.Tuple{id, "bucket"}), []byte(bucketName))

						indexDir, _ := directory.CreateOrOpen(tr, indexDirPath, nil)
						countTuple := tuple.Tuple{filename, "count"}
						countKey := indexDir.Pack(countTuple)
						oldCountValue := tr.Get(countKey).MustGet()
						var oldCount int64
						if oldCountValue == nil {
							oldCount = 0
						} else {
							oldCount = int64(binary.LittleEndian.Uint64(oldCountValue))
						}
						tr.Add(countKey, one)
						tr.Set(indexDir.Pack(tuple.Tuple{filename, oldCount}), id[:])
						if multipleTransactions {
							tr.Set(dir.Pack(tuple.Tuple{id, "partial"}), []byte{})
						}
						tr.Set(dir.Pack(tuple.Tuple{id, "ndx"}), tuple.Tuple{oldCount}.Pack())
						tr.Set(dir.Pack(tuple.Tuple{id, "meta", "uploadDate"}), tuple.Tuple{time.Now().UnixNano()}.Pack())
						for key, value := range tags {
							tr.Set(dir.Pack(tuple.Tuple{id, "meta", key}), []byte(value))
						}
					}
					if totalSize > 0 {
						for {
							n, err := f.Read(contentBuffer)
							if err != nil && err != io.EOF {
								panic(err)
							}
							if chunk+1 < chunkCount && n != chunkSize {
								fmt.Fprintf(os.Stderr, "Failed writing chunk %d./%d: written only %d bytes.\n", chunk+1, chunkCount, n)
								panic("Failed writing chunk.")
							}
							data := contentBuffer[:n]
							switch myCompressionAlgorithm {
							case compressionAlgorithmNone:
								tr.Set(dir.Pack(tuple.Tuple{id, chunk}), data)
							case compressionAlgorithmLZ4:
								var compressedByteCount int
								compressedByteCount, err = lz4.CompressDefault(data, lz4CompressedBytes)
								if err != nil {
									panic(err)
								}
								chunkKey := dir.Pack(tuple.Tuple{id, chunk})
								if compressedByteCount < n {
									tr.Set(chunkKey, lz4CompressedBytes[:compressedByteCount])
									tr.Set(dir.Pack(tuple.Tuple{id, chunk, "c"}), compressionAlgoValue)
									totalWrittenCompressed += int64(compressedByteCount)
								} else {
									tr.Set(chunkKey, data)
									totalWrittenCompressed += int64(n)
								}
							}
							totalWritten += int64(n)
							chunk++
							if chunk == chunkCount || chunk%chunksPerTransaction == 0 {
								break
							}
						}
					}
					tr.Set(dir.Pack(tuple.Tuple{id, "len"}), tuple.Tuple{totalWritten}.Pack())
					if chunk == chunkCount && multipleTransactions {
						// Last transaction
						tr.Clear(dir.Pack(tuple.Tuple{id, "partial"}))
					}
					return nil, nil
				})
				if e != nil {
					panic(e)
				}
				if verbose {
					currentPercent := percentFromWrittenAndSize(totalWritten, totalSize)
					if lastPercent < currentPercent {
						ratio := ratioFromWrittenAndCompressed(totalWritten, totalWrittenCompressed)
						fmt.Fprintf(os.Stderr, "Uploaded %d%% of %s (compression ratio = %.2f).\n", currentPercent, filename, ratio)
						lastPercent = currentPercent
					}
				}
				if chunk == chunkCount {
					break
				}
			}
			finishChannel <- totalWritten
		}(name1)
	}
}

func compressionAlgoValue(compressionAlgorithm int) []byte {
	var v []byte
	switch compressionAlgorithm {
	case compressionAlgorithmLZ4:
		v = []byte{}
	case compressionAlgorithmNone:
	default:
		v = tuple.Tuple{compressionAlgorithm}.Pack()
	}
	return v
}

// Add objects to objects store using their unique identifiers
func putID(localName string, db fdb.Database, transactionTimeout int64, batchPriority bool, bucketName string, uniqueIds map[string]bool, tags map[string]string, compressionAlgorithm int, resume, verbose bool, finishChannel chan bool) {
	indexDirPath := append(nameIndexDirPrefix, bucketName)
	for id1 := range uniqueIds {
		go func(idString string) {
			id, err := uuid.Parse(idString)
			if err != nil {
				panic(err)
			}

			var filename string
			if len(uniqueIds) == 1 && len(localName) > 0 {
				filename = localName
			} else {
				filename = idString
			}
			f, err := os.Open(filename)
			if err != nil {
				panic(err)
			}
			defer f.Close()
			fi, err := f.Stat()
			if err != nil {
				panic(err)
			}
			totalSize := fi.Size()
			chunkCount := lengthToChunkCount(totalSize)
			var removePartial bool
			var totalWritten int64
			var totalWrittenCompressed int64
			contentBuffer := make([]byte, chunkSize)
			var chunk int64
			var lastPercent int
			var lz4CompressedBytes []byte
			var myCompressionAlgorithm int
			if compressionAlgorithm == compressionAlgorithmAuto {
				if doCompress(filename) {
					myCompressionAlgorithm = compressionAlgorithmLZ4
				} else {
					myCompressionAlgorithm = compressionAlgorithmNone
				}
			} else {
				myCompressionAlgorithm = compressionAlgorithm
			}
			compressionAlgoValue := compressionAlgoValue(myCompressionAlgorithm)
			if myCompressionAlgorithm == compressionAlgorithmLZ4 {
				lz4CompressedBytes = make([]byte, lz4.CompressBound(chunkSize))
			}
			if verbose {
				fmt.Fprintf(os.Stderr, "Uploading %s...\n", filename)
			}
			timeStarted := time.Now()
			for {
				_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
					tr.Options().SetTimeout(transactionTimeout)
					if batchPriority {
						tr.Options().SetPriorityBatch()
					}
					dir, err := directory.CreateOrOpen(tr, objectPath, nil)
					if err != nil {
						panic(err)
					}
					lenKey := dir.Pack(tuple.Tuple{id, "len"})
					if chunk == 0 {
						nameKey := dir.Pack(tuple.Tuple{id, "name"})
						if !resume && tr.Get(nameKey).MustGet() != nil {
							panic("Object with given id already exists!")
						} else if resume && tr.Get(nameKey).MustGet() == nil {
							panic("Object with given id doesn't exist!")
						}
						var setPartial bool
						if resume {
							lenFuture := tr.Get(lenKey)
							lenValue := lenFuture.MustGet()
							if lenValue == nil {
								panic("Missing len key")
							}
							lenValueUnpacked, err := tuple.Unpack(lenValue)
							if err != nil {
								panic(err)
							}
							totalWritten = lenValueUnpacked[0].(int64)
							if totalSize <= totalWritten {
								panic("File is not larger than written object!")
							}
							f.Seek(totalWritten, 0)
							chunk = totalWritten / chunkSize
							removePartial = true
						} else {
							tr.Set(nameKey, []byte(filename))
							tr.Set(dir.Pack(tuple.Tuple{id, "bucket"}), []byte(bucketName))

							indexDir, _ := directory.CreateOrOpen(tr, indexDirPath, nil)
							countTuple := tuple.Tuple{filename, "count"}
							countKey := indexDir.Pack(countTuple)
							oldCountValue := tr.Get(countKey).MustGet()
							var oldCount int64
							if oldCountValue == nil {
								oldCount = 0
							} else {
								oldCount = int64(binary.LittleEndian.Uint64(oldCountValue))
							}
							tr.Add(countKey, one)
							tr.Set(indexDir.Pack(tuple.Tuple{filename, oldCount}), id[:])
							tr.Set(dir.Pack(tuple.Tuple{id, "ndx"}), tuple.Tuple{oldCount}.Pack())
							tr.Set(dir.Pack(tuple.Tuple{id, "meta", "uploadDate"}), tuple.Tuple{time.Now().UnixNano()}.Pack())
							for key, value := range tags {
								tr.Set(dir.Pack(tuple.Tuple{id, "meta", key}), []byte(value))
							}
							removePartial = chunkCount > chunksPerTransaction
						}
						setPartial = (chunkCount - chunk) > chunksPerTransaction
						if setPartial {
							tr.Set(dir.Pack(tuple.Tuple{id, "partial"}), []byte{})
						}
					}
					if (totalSize - totalWritten) > 0 {
						for {
							n, err := f.Read(contentBuffer)
							if err != nil && err != io.EOF {
								panic(err)
							}
							if chunk+1 < chunkCount && n != chunkSize {
								fmt.Fprintf(os.Stderr, "Failed writing chunk %d./%d: written only %d\n", chunk+1, chunkCount, n)
								panic("Failed writing chunk.")
							}
							data := contentBuffer[:n]
							chunkKey := dir.Pack(tuple.Tuple{id, chunk})
							switch myCompressionAlgorithm {
							case compressionAlgorithmNone:
								tr.Set(chunkKey, data)
							case compressionAlgorithmLZ4:
								var compressedByteCount int
								compressedByteCount, err = lz4.CompressDefault(data, lz4CompressedBytes)
								if err != nil {
									panic(err)
								}
								if compressedByteCount < n {
									tr.Set(chunkKey, lz4CompressedBytes[:compressedByteCount])
									tr.Set(dir.Pack(tuple.Tuple{id, chunk, "c"}), compressionAlgoValue)
									totalWrittenCompressed += int64(compressedByteCount)
								} else {
									tr.Set(chunkKey, data)
									totalWrittenCompressed += int64(n)
								}
							}
							totalWritten += int64(n)
							chunk++
							if chunk == chunkCount || chunk%chunksPerTransaction == 0 {
								break
							}
						}
					}
					tr.Set(lenKey, tuple.Tuple{totalWritten}.Pack())
					if removePartial && chunk == chunkCount {
						// Last transaction
						tr.Clear(dir.Pack(tuple.Tuple{id, "partial"}))
					}
					return nil, nil
				})
				if e != nil {
					panic(e)
				}
				if verbose && !resume {
					currentPercent := percentFromWrittenAndSize(totalWritten, totalSize)
					if lastPercent < currentPercent {
						elapsed := time.Since(timeStarted)
						ratio := ratioFromWrittenAndCompressed(totalWritten, totalWrittenCompressed)
						fmt.Fprintf(os.Stderr, "Uploaded %d%% of %s (%.2fMB/s; compression ratio = %.2f).\n", currentPercent, filename, float64(totalWritten)/1e6/elapsed.Seconds(), ratio)
						lastPercent = currentPercent
					}
				}
				if chunk == chunkCount {
					break
				}
			}
			finishChannel <- true
		}(id1)
	}
}

// Open connection to database
func database(clusterFile string, datacenter string, machine string, verbose bool) fdb.Database {
	fdb.MustAPIVersion(510)
	var db fdb.Database
	if clusterFile == "" {
		db = fdb.MustOpenDefault()
	} else {
		db = fdb.MustOpen(clusterFile, []byte("DB"))
		if verbose {
			fmt.Fprintf(os.Stderr, "Cluster file = %s\n", clusterFile)
		}
	}

	if datacenter != "" {
		db.Options().SetDatacenterId(datacenter)
		if verbose {
			fmt.Fprintf(os.Stderr, "Data center identifier key = %s\n", datacenter)
		}
	}
	if machine != "" {
		db.Options().SetMachineId(machine)
		if verbose {
			fmt.Fprintf(os.Stderr, "Machine identifier key = %s\n", machine)
		}
	}
	return db
}

var allBuckets = flag.Bool("all_buckets", false, "Show all FoundationDB object store buckets when using list command")
var batchPriority = flag.Bool("batch", false, "Use batch priority which is a lower priority than normal")
var bucketName = flag.String("bucket", "objectstorage1", "FoundationDB object store bucket to use")
var clusterFile = flag.String("cluster_file", "", "Use FoundationDB cluster identified by the provided cluster file")
var algo = flag.String("compression", "auto", "Choose compression algorithm: auto, none or lz4")
var dataCenterID = flag.String("datacenter_id", "", "Data center identifier key (up to 16 hex characters)")
var help = flag.Bool("help", false, "Display usage info")
var localName = flag.String("local", "", "Local filename to use (use '-' to print to standard output)")
var machineID = flag.String("machine_id", "", "Machine identifier key (up to 16 hex characters) - defaults to a random value shared by all fdbserver processes on this machine")
var metadata = flag.String("metadata", "", "Add the given TAG with a value VAL (may be used multiple times)")
var allowPartial = flag.Bool("partial", false, "Don't skip a partially uploaded object when getting")
var transactionTimeout = flag.Duration("timeout", time.Duration(10*1000*1000*1000), "Use this transaction timeout")
var verbose = flag.Bool("verbose", false, "Be more verbose")
var version = flag.Bool("version", false, "Print the tool version and exit")

func init() {
	//	flag.BoolVar(help, "h", false, "Display usage info")
	//	flag.BoolVar(version, "v", false, "Be more verbose")
	flag.BoolVar(help, "h", false, "")
	flag.BoolVar(version, "v", false, "")
}

func main() {
	flag.Parse()
	if *help {
		usage()
		return
	}
	if *version {
		fmt.Printf("%s version 1.20180829\n\nCreated by Šimun Mikecin <numisemis@yahoo.com>.\n", os.Args[0])
		return
	}
	var tags map[string]string
	compressionAlgorithm := compressionAlgorithmAuto
	if *algo != "auto" {
		index := 0
		for ; index < len(compressionAlgorithms); index++ {
			if compressionAlgorithms[index] == *algo {
				compressionAlgorithm = index
				break
			}
		}
		if index == len(compressionAlgorithms) {
			usage()
			return
		}
	}
	if *metadata != "" {
		tag := strings.SplitAfter(*metadata, "=")[1]
		value := strings.SplitAfter(*metadata, "=")[2]
		tags[tag] = value
	}
	var db fdb.Database
	var msTimeout = transactionTimeout.Nanoseconds() / 1e6
	if len(flag.Args()) == 0 {
		usage()
		return
	}
	cmd := flag.Arg(0)
	switch cmd {
	case "list":
		db = database(*clusterFile, *dataCenterID, *machineID, *verbose)
		var prefix string
		if len(flag.Args()) > 1 {
			prefix = flag.Arg(1)
		}
		list(db, msTimeout, *allBuckets, *bucketName, prefix)
	case "put":
		if len(flag.Args()) == 1 {
			usage()
		} else {
			db = database(*clusterFile, *dataCenterID, *machineID, *verbose)
			uniqueNames := make(map[string]bool)
			for _, val := range flag.Args()[1:] {
				uniqueNames[val] = true
			}
			finishChannel := make(chan int64)
			timeStarted := time.Now()
			put(*localName, db, msTimeout, *batchPriority, *bucketName, uniqueNames, tags, compressionAlgorithm, *verbose, finishChannel)
			var totalBytes int64
			for range uniqueNames {
				totalBytes += <-finishChannel
			}
			if *verbose {
				elapsed := time.Since(timeStarted)
				var manySuffix string
				count := len(uniqueNames)
				if count > 1 {
					manySuffix = "s"
				}
				fmt.Fprintf(os.Stderr, "\nUploaded %d bytes in %d file%s (%.2fMB/s).\n", totalBytes, count, manySuffix, float64(totalBytes)/1e6/elapsed.Seconds())
			}
		}
	case "put_id", "resume":
		if len(flag.Args()) == 1 {
			usage()
		} else {
			db = database(*clusterFile, *dataCenterID, *machineID, *verbose)
			uniqueNames := make(map[string]bool)
			for _, val := range flag.Args()[1:] {
				uniqueNames[val] = true
			}
			finishChannel := make(chan bool)
			putID(*localName, db, msTimeout, *batchPriority, *bucketName, uniqueNames, tags, compressionAlgorithm, cmd == "resume", *verbose, finishChannel)
			for range uniqueNames {
				<-finishChannel
			}
		}
	case "get":
		if len(flag.Args()) == 1 {
			usage()
		} else {
			db = database(*clusterFile, *dataCenterID, *machineID, *verbose)
			finishChannel := make(chan int64)
			timeStarted := time.Now()
			get(*localName, db, msTimeout, *bucketName, flag.Args()[1:], *allowPartial, *verbose, finishChannel)
			var totalBytes int64
			for range flag.Args()[1:] {
				totalBytes += <-finishChannel
			}
			if *verbose {
				elapsed := time.Since(timeStarted)
				var manySuffix string
				count := len(flag.Args()[1:])
				if count > 1 {
					manySuffix = "s"
				}
				fmt.Fprintf(os.Stderr, "\nDownloaded %d bytes in %d file%s (%.2fMB/s).\n", totalBytes, count, manySuffix, float64(totalBytes)/1e6/elapsed.Seconds())
			}
		}
	case "get_id":
		if len(flag.Args()) == 1 {
			usage()
		} else {
			db = database(*clusterFile, *dataCenterID, *machineID, *verbose)
			finishChannel := make(chan bool)
			getID(*localName, db, msTimeout, flag.Args()[1:], *verbose, finishChannel)
			for range flag.Args()[1:] {
				<-finishChannel
			}
		}
	case "delete":
		if len(flag.Args()) == 1 {
			usage()
		} else {
			db = database(*clusterFile, *dataCenterID, *machineID, *verbose)
			finishChannel := make(chan bool)
			remove(db, msTimeout, *batchPriority, *bucketName, flag.Args()[1:], finishChannel)
			for range flag.Args()[1:] {
				<-finishChannel
			}
		}
	case "delete_id":
		if len(flag.Args()) == 1 {
			usage()
		} else {
			db = database(*clusterFile, *dataCenterID, *machineID, *verbose)
			finishChannel := make(chan bool)
			removeID(db, msTimeout, *batchPriority, flag.Args()[1:], finishChannel)
			for range flag.Args()[1:] {
				<-finishChannel
			}
		}
	default:
		usage()
	}
}
