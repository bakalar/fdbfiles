package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/hungys/go-lz4"
	"gopkg.in/mgo.v2/bson"
)

const (
	chunkSize            = 1e5
	chunksPerTransaction = 99
	transactionTimeout   = 10000 // ms

	compressionAlgorithmUnset = -1
	compressionAlgorithmNone  = 0
	compressionAlgorithmLZ4   = 1
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

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [OPTION]... [COMMAND [NAME_OR_ID...]]\n\n", os.Args[0])
	fmt.Fprintln(os.Stderr, "Manipulate FoundationDB object store using the command line.")
	fmt.Fprintln(os.Stderr, "\nPossible commands include:")
	fmt.Fprintln(os.Stderr, "\tlist\t\tlist objects; NAME is an optional prefix which listed objects must begin with")
	fmt.Fprintln(os.Stderr, "\tput\t\tadd objects with given names")
	fmt.Fprintln(os.Stderr, "\tput_id\t\tadd objects with given ids")
	fmt.Fprintln(os.Stderr, "\tget\t\tget objects with given names")
	fmt.Fprintln(os.Stderr, "\tget_id\t\tget objects with given ids")
	fmt.Fprintln(os.Stderr, "\tresume\t\tresume getting objects with given ids")
	fmt.Fprintln(os.Stderr, "\tdelete\t\tdelete all objects with given names")
	fmt.Fprintln(os.Stderr, "\tdelete_id\tdelete objects with given ids")
	fmt.Fprintln(os.Stderr, "\noptions:")
	fmt.Fprintln(os.Stderr, "\t--all_buckets\t\tshow all FoundationDB object store buckets when using list command")
	fmt.Fprintln(os.Stderr, "\t--batch\t\t\tuse batch priority which is a lower priority than normal")
	fmt.Fprintln(os.Stderr, "\t--bucket=BUCKET\t\tFoundationDB object store bucket to use (default: 'objectstorage1')")
	fmt.Fprintln(os.Stderr, "\t--cluster=FILE\t\tuse FoundationDB cluster identified by the provided cluster file")
	fmt.Fprintln(os.Stderr, "\t--compression=ALGO\tchoose compression algorithm: 'none' or 'lz4' (default)")
	fmt.Fprintln(os.Stderr, "\t--datacenter=ID\t\tspecify the datacenter ID")
	fmt.Fprintln(os.Stderr, "\t--local=FILENAME\tlocal filename to use (use '-' to print to standard output)")
	fmt.Fprintln(os.Stderr, "\t--machine=ID\t\tspecify the machine ID")
	fmt.Fprintln(os.Stderr, "\t--metadata=TAG=VAL\tadd the given TAG with a value VAL (may be used multiple times)")
	fmt.Fprintln(os.Stderr, "\t--partial\t\tdon't skip a partially uploaded object when getting")
	fmt.Fprintln(os.Stderr, "\t--verbose\t\tbe more verbose")
	fmt.Fprintln(os.Stderr, "\t--version\t\tprint the tool version and exit")
}

func list(db fdb.Database, allBuckets bool, bucketName string, prefix string) {
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
		for _, bucketName1 := range bucketNames {
			indexDir, err := directory.Open(tr, append(nameIndexDirPrefix, bucketName1), nil)
			if err != nil {
				panic(err)
			}
			objectDir, err := directory.Open(tr, objectPath, nil)
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
					lengthKey := objectDir.Pack(tuple.Tuple{kv.Value, "len"})
					lengthValueFuture := tr.Get(lengthKey)

					partialKey := objectDir.Pack(tuple.Tuple{kv.Value, "partial"})
					partialValueFuture := tr.Get(partialKey)

					uploadDateKey := objectDir.Pack(tuple.Tuple{kv.Value, "meta", "uploadDate"})
					uploadDateFuture := tr.Get(uploadDateKey)

					_id := bson.ObjectId(kv.Value)

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
					if uploadDateValue == nil {
						uploadDate = _id.Time()
					}
					name := t[1]
					revision := t[2].(int64)
					fmt.Printf("%s %s\t%d\t%s %d\t%s%s\n", _id, bucketName1, revision, uploadDate.Format("2006-01-02T15:04:05.000000000-0700"), length, name, partialMark)
				}
			}
		}
		return nil, nil
	})
	if e != nil {
		panic(e)
	}
}

func delete(db fdb.Database, batchPriority bool, bucketName string, names []string, finishChannel chan bool) {
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

				countKey := indexDir.Pack(tuple.Tuple{name, "count"})
				objectCountValueFuture := tr.Get(countKey)

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
				objectCountValue := objectCountValueFuture.MustGet()
				if objectCountValue == nil {
					return nil, nil
				}
				objectCount := binary.LittleEndian.Uint64(objectCountValue)

				endKey := indexDir.Pack(tuple.Tuple{name, int64(objectCount)})
				keyRange := fdb.KeyRange{
					Begin: startKey,
					End:   endKey,
				}
				ri := tr.GetRange(keyRange, fdb.RangeOptions{}).Iterator()
				for ri.Advance() {
					kv := ri.MustGet()
					objectPrefixKey := objectDir.Pack(tuple.Tuple{kv.Value})
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

func deleteID(db fdb.Database, batchPriority bool, ids []string, finishChannel chan bool) {
	for _, id1 := range ids {
		go func(id string) {
			idBytes := []byte(bson.ObjectIdHex(id))
			_, e := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
				tr.Options().SetTimeout(transactionTimeout)
				if batchPriority {
					tr.Options().SetPriorityBatch()
				}
				objectDir, err := directory.Open(tr, objectPath, nil)
				if err != nil {
					panic(err)
				}

				bucketNameFuture := tr.Get(objectDir.Pack(tuple.Tuple{idBytes, "bucket"}))
				nameFuture := tr.Get(objectDir.Pack(tuple.Tuple{idBytes, "name"}))
				ndxFuture := tr.Get(objectDir.Pack(tuple.Tuple{idBytes, "ndx"}))

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

				if index+1 == binary.LittleEndian.Uint64(objectCountFuture.MustGet()) {
					// Need to reduce count so that (count - 1) always points to last used object version.
					for i := index; ; i++ {
						previousVersionKey := indexDir.Pack(tuple.Tuple{name, int64(i)})
						previousVersionFuture := tr.Get(previousVersionKey)
						if previousVersionFuture.MustGet() != nil {
							// Found a previous version.
							bytes := make([]byte, 8)
							binary.LittleEndian.PutUint64(bytes, i)
							tr.Set(countKey, bytes)
							break
						}
						if i == 0 {
							// No other version found.
							index = 0
							break
						}
					}
				}
				if index == 0 {
					// No version left.
					tr.Clear(countKey)
				}

				objectPrefixRange, err := fdb.PrefixRange(objectDir.Pack(tuple.Tuple{idBytes}))
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

func get(localName string, db fdb.Database, bucketName string, names []string, allowPartial, verbose bool, finishChannel chan bool) {
	indexDirPath := append(nameIndexDirPrefix, bucketName)
	for _, name1 := range names {
		go func(name string) {
			var length int64
			var chunkCount int64
			var f *os.File
			var id []byte
			print := localName == "-"
			var chunk int64
			if !print {
				var path string
				if len(names) == 1 && len(localName) > 0 {
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
			timeStarted := time.Now()
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
							id = idFuture.MustGet()
							if id == nil {
								// This version doesn't exist.
								continue
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
								break
							}
						}
					}
					for chunk < chunkCount {
						key := dir.Pack(tuple.Tuple{id, chunk})
						bytesFuture := tr.Get(key)
						compressionKey := dir.Pack(tuple.Tuple{[]byte(id), chunk, "c"})
						compressionAlgoFuture := tr.Get(compressionKey)
						bytes := bytesFuture.MustGet()

						compressionAlgo := compressionAlgoFuture.MustGet()
						if compressionAlgo != nil {
							v, _ := tuple.Unpack(compressionAlgo)
							if v != nil {
								switch v[0].(int64) {
								case compressionAlgorithmLZ4:
									var uncompressedSize int64
									if chunk+1 == chunkCount {
										uncompressedSize = length % chunkSize
									} else {
										uncompressedSize = chunkSize
									}
									uncompressed := make([]byte, uncompressedSize)
									_, err = lz4.DecompressSafe(bytes, uncompressed)
									if err != nil {
										panic(err)
									}
									bytes = uncompressed
								}
							}
						}

						if print {
							fmt.Print(string(bytes))
						} else {
							_, err := f.Write(bytes)
							if err != nil {
								panic(err)
							}
						}
						chunk++
						if print || chunk%chunksPerTransaction == 0 {
							break
						}
					}
					return nil, nil
				})
				if e != nil {
					panic(e)
				}
				if chunk == chunkCount {
					break
				}
			}
			if verbose {
				elapsed := time.Since(timeStarted)
				fmt.Fprintf(os.Stderr, "Downloaded %s (%.2fMB/s).\n", name, float64(length)/1e6/elapsed.Seconds())
			}
			finishChannel <- true
		}(name1)
	}
}

func getID(localName string, db fdb.Database, ids []string, verbose bool, finishChannel chan bool) {
	for _, id1 := range ids {
		go func(id string) {
			idBytes := []byte(bson.ObjectIdHex(id))
			var length int64
			var chunkCount int64
			var f *os.File
			print := localName == "-"
			var chunk int64
			if !print {
				var path string
				if len(ids) == 1 && len(localName) > 0 {
					path = localName
				} else {
					path = id
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
						lengthKey := dir.Pack(tuple.Tuple{idBytes, "len"})
						v, err := tuple.Unpack(tr.Get(lengthKey).MustGet())
						if err != nil {
							panic(err)
						}
						length = v[0].(int64)
						chunkCount = lengthToChunkCount(length)
					}
					for chunk < chunkCount {
						key := dir.Pack(tuple.Tuple{idBytes, chunk})
						bytesFuture := tr.Get(key)
						compressionKey := dir.Pack(tuple.Tuple{idBytes, chunk, "c"})
						compressionAlgoFuture := tr.Get(compressionKey)
						bytes := bytesFuture.MustGet()

						compressionAlgo := compressionAlgoFuture.MustGet()
						if compressionAlgo != nil {
							v, _ := tuple.Unpack(compressionAlgo)
							if v != nil {
								switch v[0].(int64) {
								case compressionAlgorithmLZ4:
									var uncompressedSize int64
									if chunk+1 == chunkCount {
										uncompressedSize = length % chunkSize
									} else {
										uncompressedSize = chunkSize
									}
									uncompressed := make([]byte, uncompressedSize)
									_, err = lz4.DecompressSafe(bytes, uncompressed)
									if err != nil {
										panic(err)
									}
									bytes = uncompressed
								}
							}
						}

						if print {
							fmt.Print(string(bytes))
						} else {
							_, err := f.Write(bytes)
							if err != nil {
								panic(err)
							}
						}
						chunk++
						if print || chunk%chunksPerTransaction == 0 {
							break
						}
					}
					return nil, nil
				})
				if e != nil {
					panic(e)
				}
				if chunk == chunkCount {
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

func put(localName string, db fdb.Database, batchPriority bool, bucketName string, uniqueNames map[string]bool, tags map[string]string, compressionAlgorithm int, verbose bool, finishChannel chan bool) {
	var compressionAlgoValue []byte = nil
	switch compressionAlgorithm {
	case compressionAlgorithmUnset:
		compressionAlgoValue = tuple.Tuple{compressionAlgorithmLZ4}.Pack()
	case compressionAlgorithmNone:
	default:
		compressionAlgoValue = tuple.Tuple{compressionAlgorithm}.Pack()
	}
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
			var id []byte
			contentBuffer := make([]byte, chunkSize)
			var chunk int64
			var lastPercent int
			var lz4CompressedBytes []byte
			var myCompressionAlgorithm int
			if compressionAlgorithm == compressionAlgorithmUnset {
				if doCompress(filename) {
					myCompressionAlgorithm = compressionAlgorithmLZ4
				} else {
					myCompressionAlgorithm = compressionAlgorithmNone
				}
			} else {
				myCompressionAlgorithm = compressionAlgorithm
			}
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
					if chunk == 0 {
						var nameKey fdb.KeyConvertible
						for {
							id = []byte(bson.NewObjectId())
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
						tr.Set(indexDir.Pack(tuple.Tuple{filename, oldCount}), id)
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
								if compressedByteCount < n {
									tr.Set(dir.Pack(tuple.Tuple{id, chunk}), lz4CompressedBytes[:compressedByteCount])
									tr.Set(dir.Pack(tuple.Tuple{id, chunk, "c"}), compressionAlgoValue)
									totalWrittenCompressed += int64(compressedByteCount)
								} else {
									tr.Set(dir.Pack(tuple.Tuple{id, chunk}), data)
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
					currentPercent := int(100 * totalWritten / totalSize)
					if lastPercent < currentPercent {
						elapsed := time.Since(timeStarted)
						fTotalWritten := float64(totalWritten)
						var compressed float64
						if totalWrittenCompressed == 0 {
							compressed = fTotalWritten
						} else {
							compressed = float64(totalWrittenCompressed)
						}
						fmt.Fprintf(os.Stderr, "Uploaded %d%% of %s (%.2fMB/s; compression ratio = %.2f).\n", currentPercent, filename, fTotalWritten/1e6/elapsed.Seconds(), fTotalWritten/compressed)
						lastPercent = currentPercent
					}
				}
				if chunk == chunkCount {
					break
				}
			}
			finishChannel <- true
		}(name1)
	}
}

func putID(localName string, db fdb.Database, batchPriority bool, bucketName string, uniqueIds map[string]bool, tags map[string]string, compressionAlgorithm int, resume, verbose bool, finishChannel chan bool) {
	var compressionAlgoValue []byte = nil
	switch compressionAlgorithm {
	case compressionAlgorithmUnset:
		compressionAlgoValue = tuple.Tuple{compressionAlgorithmLZ4}.Pack()
	case compressionAlgorithmNone:
	default:
		compressionAlgoValue = tuple.Tuple{compressionAlgorithm}.Pack()
	}
	indexDirPath := append(nameIndexDirPrefix, bucketName)
	for id1 := range uniqueIds {
		go func(idString string) {
			id := []byte(bson.ObjectIdHex(idString))
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
			if compressionAlgorithm == compressionAlgorithmUnset {
				if doCompress(filename) {
					myCompressionAlgorithm = compressionAlgorithmLZ4
				} else {
					myCompressionAlgorithm = compressionAlgorithmNone
				}
			} else {
				myCompressionAlgorithm = compressionAlgorithm
			}
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
							tr.Set(indexDir.Pack(tuple.Tuple{filename, oldCount}), id)
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
							switch myCompressionAlgorithm {
							case compressionAlgorithmNone:
								tr.Set(dir.Pack(tuple.Tuple{id, chunk}), data)
							case compressionAlgorithmLZ4:
								var compressedByteCount int
								compressedByteCount, err = lz4.CompressDefault(data, lz4CompressedBytes)
								if err != nil {
									panic(err)
								}
								if compressedByteCount < n {
									tr.Set(dir.Pack(tuple.Tuple{id, chunk}), lz4CompressedBytes[:compressedByteCount])
									tr.Set(dir.Pack(tuple.Tuple{id, chunk, "c"}), compressionAlgoValue)
									totalWrittenCompressed += int64(compressedByteCount)
								} else {
									tr.Set(dir.Pack(tuple.Tuple{id, chunk}), data)
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
					currentPercent := int(100 * totalWritten / totalSize)
					if lastPercent < currentPercent {
						elapsed := time.Since(timeStarted)
						fTotalWritten := float64(totalWritten)
						var compressed float64
						if totalWrittenCompressed == 0 {
							compressed = fTotalWritten
						} else {
							compressed = float64(totalWrittenCompressed)
						}
						fmt.Fprintf(os.Stderr, "Uploaded %d%% of %s (%.2fMB/s; compression ratio = %.2f).\n", currentPercent, filename, fTotalWritten/1e6/elapsed.Seconds(), fTotalWritten/compressed)
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

func database(clusterFile string, datacenter string, machine string, verbose bool) fdb.Database {
	fdb.MustAPIVersion(510)
	if clusterFile == "" {
		return fdb.MustOpenDefault()
	} else if verbose {
		fmt.Fprintf(os.Stderr, "Cluster file = %s\n", clusterFile)
	}
	db := fdb.MustOpen(clusterFile, []byte("DB"))
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

func main() {
	if len(os.Args) < 2 || os.Args[1] == "--help" {
		usage()
		return
	}
	if len(os.Args) < 2 || os.Args[1] == "--version" {
		fmt.Printf("%s version 0.20180718\n\nCreated by Šimun Mikecin <numisemis@yahoo.com>.\n", os.Args[0])
		return
	}
	verbose := false
	var cmd string
	bucketName := "objectstorage1"
	var localName string
	allBuckets := false
	var clusterFile string
	var tags map[string]string
	compressionAlgorithm := compressionAlgorithmUnset
	var argsIndex int
	var datacenter string
	var machine string
	allowPartial := false
	batchPriority := false
	for i := 1; i < len(os.Args); i++ {
		if !strings.HasPrefix(os.Args[i], "-") {
			cmd = os.Args[i]
			i++
			if i < len(os.Args) {
				argsIndex = i
			} else {
				argsIndex = -1
			}
			break
		}
		if strings.HasPrefix(os.Args[i], "--all_buckets") {
			allBuckets = true
		}
		if strings.HasPrefix(os.Args[i], "--batch") {
			batchPriority = true
		}
		if strings.HasPrefix(os.Args[i], "--bucket=") {
			bucketName = strings.SplitAfter(os.Args[i], "=")[1]
		}
		if strings.HasPrefix(os.Args[i], "--cluster=") {
			clusterFile = strings.SplitAfter(os.Args[i], "=")[1]
		}
		if strings.HasPrefix(os.Args[i], "--compression=") {
			algo := strings.SplitAfter(os.Args[i], "=")[1]
			index := 0
			for ; index < len(compressionAlgorithms); index++ {
				if compressionAlgorithms[index] == algo {
					compressionAlgorithm = index
					break
				}
			}
			if index == len(compressionAlgorithms) {
				usage()
				return
			}
		}
		if strings.HasPrefix(os.Args[i], "--datacenter=") {
			datacenter = strings.SplitAfter(os.Args[i], "=")[1]
		}
		if strings.HasPrefix(os.Args[i], "--machine=") {
			machine = strings.SplitAfter(os.Args[i], "=")[1]
		}
		if strings.HasPrefix(os.Args[i], "--metadata=") {
			tag := strings.SplitAfter(os.Args[i], "=")[1]
			value := strings.SplitAfter(os.Args[i], "=")[2]
			tags[tag] = value
		}
		if strings.HasPrefix(os.Args[i], "--local=") {
			localName = strings.SplitAfter(os.Args[i], "=")[1]
		}
		if strings.HasPrefix(os.Args[i], "--partial") {
			allowPartial = true
		}
		if strings.HasPrefix(os.Args[i], "--verbose") {
			verbose = true
		}
	}
	var db fdb.Database
	switch cmd {
	case "list":
		db = database(clusterFile, datacenter, machine, verbose)
		var prefix string
		if argsIndex >= 0 {
			prefix = os.Args[argsIndex]
		}
		list(db, allBuckets, bucketName, prefix)
	case "put":
		if argsIndex < 0 {
			usage()
		} else {
			db = database(clusterFile, datacenter, machine, verbose)
			uniqueNames := make(map[string]bool)
			for _, val := range os.Args[argsIndex:] {
				uniqueNames[val] = true
			}
			finishChannel := make(chan bool)
			put(localName, db, batchPriority, bucketName, uniqueNames, tags, compressionAlgorithm, verbose, finishChannel)
			for range uniqueNames {
				<-finishChannel
			}
		}
	case "put_id", "resume":
		if argsIndex < 0 {
			usage()
		} else {
			db = database(clusterFile, datacenter, machine, verbose)
			uniqueNames := make(map[string]bool)
			for _, val := range os.Args[argsIndex:] {
				uniqueNames[val] = true
			}
			finishChannel := make(chan bool)
			putID(localName, db, batchPriority, bucketName, uniqueNames, tags, compressionAlgorithm, cmd == "resume", verbose, finishChannel)
			for range uniqueNames {
				<-finishChannel
			}
		}
	case "get":
		if argsIndex < 0 {
			usage()
		} else {
			db = database(clusterFile, datacenter, machine, verbose)
			finishChannel := make(chan bool)
			get(localName, db, bucketName, os.Args[argsIndex:], allowPartial, verbose, finishChannel)
			for index := argsIndex; index < len(os.Args); index++ {
				<-finishChannel
			}
		}
	case "get_id":
		if argsIndex < 0 {
			usage()
		} else {
			db = database(clusterFile, datacenter, machine, verbose)
			finishChannel := make(chan bool)
			getID(localName, db, os.Args[argsIndex:], verbose, finishChannel)
			for index := argsIndex; index < len(os.Args); index++ {
				<-finishChannel
			}
		}
	case "delete":
		if argsIndex < 0 {
			usage()
		} else {
			db = database(clusterFile, datacenter, machine, verbose)
			finishChannel := make(chan bool)
			delete(db, batchPriority, bucketName, os.Args[argsIndex:], finishChannel)
			for index := argsIndex; index < len(os.Args); index++ {
				<-finishChannel
			}
		}
	case "delete_id":
		if argsIndex < 0 {
			usage()
		} else {
			db = database(clusterFile, datacenter, machine, verbose)
			finishChannel := make(chan bool)
			deleteID(db, batchPriority, os.Args[argsIndex:], finishChannel)
			for index := argsIndex; index < len(os.Args); index++ {
				<-finishChannel
			}
		}
	default:
		usage()
	}
}
