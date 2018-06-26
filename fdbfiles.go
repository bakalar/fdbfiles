package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/hungys/go-lz4"
	"gopkg.in/mgo.v2/bson"
)

const (
	chunkSize                   = 1e5
	chunksPerTransaction        = 99
	defaultCompressionAlgorithm = compressionAlgorithmLZ4

	compressionAlgorithmNone = 0
	compressionAlgorithmLZ4  = 1
)

var (
	one          = []byte{1, 0, 0, 0, 0, 0, 0, 0}
	objectPath   = []string{"object"}
	indexDirPath = []string{"object", "index", "name"}

	compressionAlgorithms = []string{"none", "lz4"}
)

func lengthToChunkCount(length int64) int64 {
	count := length / chunkSize
	if length%chunkSize != 0 {
		count++
	}
	return count
}

func usage() {
	fmt.Printf("Usage: %s [OPTION]... [COMMAND [NAME_OR_ID...]]\n\n", os.Args[0])
	fmt.Println("Manipulate FoundationDB Object Store using the command line.")
	fmt.Println("\nPossible commands include:")
	fmt.Println("\tlist\t\tlist files; NAME is an optional prefix which listed filenames must begin with")
	//fmt.Println("\tsearch\t\tsearch all files; NAME is a substring which listed filenames must contain")
	fmt.Println("\tput\t\tadd files with given names")
	fmt.Println("\tput_id\t\tadd files with given ids")
	fmt.Println("\tget\t\tget files with given names")
	fmt.Println("\tget_id\t\tget files with given ids")
	fmt.Println("\tdelete\t\tdelete all files with given names")
	fmt.Println("\tdelete_id\tdelete files with given ids")
	fmt.Println("\ngeneral options:")
	//fmt.Println("\t--help\t\tprint usage")
	fmt.Println("\t--verbose\tbe more verbose.")
	fmt.Println("\t--version\tprint the tool version and exit")
	fmt.Println("\nstorage options:")
	fmt.Println("\t--all_buckets\t\tshow all FoundationDB Object Store buckets when using list command")
	fmt.Println("\t--compression=ALGO\tchoose compression algorithm: 'none' or 'lz4' (default)")
	fmt.Println("\t--bucket=BUCKET\t\tFoundationDB Object Store bucket to use (default: 'objectstorage1')")
	fmt.Println("\t--cluster=FILE\t\tuse FoundationDB cluster identified by the provided cluster file")
	fmt.Println("\t--metadata=TAG=VAL\tadd the given TAG with a value VAL (may be used multiple times)")
	fmt.Println("\t--local=FILENAME\tlocal filename to use (use '-' to print to standard output)")
}

func list(db fdb.Database, allBuckets bool, bucketName string, prefix string) {
	db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		indexDir, err := directory.Open(tr, indexDirPath, nil)
		if err != nil {
			panic(err)
		}
		objectDir, err := directory.Open(tr, objectPath, nil)
		if err != nil {
			panic(err)
		}

		var ri *fdb.RangeIterator
		if allBuckets {
			ri = tr.GetRange(indexDir, fdb.RangeOptions{}).Iterator()
		} else {
			key := indexDir.Pack(tuple.Tuple{bucketName})
			var prefixKey []byte
			if len(prefix) > 0 {
				prefixKey = append(key, 0x02)
				prefixKey = append(prefixKey, prefix...)
			} else {
				prefixKey = key
			}
			range2, err := fdb.PrefixRange(prefixKey)
			if err != nil {
				panic(err)
			}
			ri = tr.GetRange(range2, fdb.RangeOptions{}).Iterator()
		}

		for ri.Advance() {
			kv := ri.MustGet()
			t, _ := tuple.Unpack(kv.Key)
			if t[3] != "count" {
				_id := bson.ObjectId(kv.Value)
				id := []byte(_id)
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
				fmt.Printf("%s %s\t%s %d\t%s%s\n", _id, t[1], uploadDate.Format("2006-01-02T15:04:05.000000000-0700"), length, t[2], partialMark)
			}
		}
		return nil, nil
	})
}

func delete(db fdb.Database, bucketName string, names []string, finishChannel chan bool) {
	for _, name1 := range names {
		go func(name string) {
			db.Transact(func(tr fdb.Transaction) (interface{}, error) {
				indexDir, err := directory.Open(tr, indexDirPath, nil)
				if err != nil {
					panic(err)
				}

				countKey := indexDir.Pack(tuple.Tuple{bucketName, name, "count"})
				objectCountValueFuture := tr.Get(countKey)

				objectDir, err := directory.Open(tr, objectPath, nil)
				if err != nil {
					panic(err)
				}

				indexPrefixKey := indexDir.Pack(tuple.Tuple{bucketName, name})
				indexPrefixRange, err := fdb.PrefixRange(indexPrefixKey)
				if err != nil {
					panic(err)
				}

				startKey := indexDir.Pack(tuple.Tuple{bucketName, name, 0})
				objectCountValue := objectCountValueFuture.MustGet()
				if objectCountValue == nil {
					return nil, nil
				}
				objectCount := int64(binary.LittleEndian.Uint64(objectCountValue))

				endKey := indexDir.Pack(tuple.Tuple{bucketName, name, objectCount})
				keyRange := fdb.KeyRange{
					Begin: startKey,
					End:   endKey,
				}
				ri := tr.GetRange(keyRange, fdb.RangeOptions{}).Iterator()
				for ri.Advance() {
					kv := ri.MustGet()
					id := bson.ObjectId(kv.Value)
					objectPrefixKey := objectDir.Pack(tuple.Tuple{[]byte(id)})
					objectPrefixRange, err := fdb.PrefixRange(objectPrefixKey)
					if err != nil {
						continue // Object already deleted.
					}
					tr.ClearRange(objectPrefixRange)
				}
				tr.ClearRange(indexPrefixRange)

				return nil, nil
			})
			finishChannel <- true
		}(name1)
	}
}

func deleteID(db fdb.Database, ids []string, finishChannel chan bool) {
	for _, id1 := range ids {
		go func(id string) {
			db.Transact(func(tr fdb.Transaction) (interface{}, error) {
				_id := bson.ObjectIdHex(id)
				indexDir, err := directory.Open(tr, indexDirPath, nil)
				if err != nil {
					panic(err)
				}
				objectDir, err := directory.Open(tr, objectPath, nil)
				if err != nil {
					panic(err)
				}

				bucketNameFuture := tr.Get(objectDir.Pack(tuple.Tuple{[]byte(_id), "bucket"}))
				nameFuture := tr.Get(objectDir.Pack(tuple.Tuple{[]byte(_id), "name"}))
				ndxFuture := tr.Get(objectDir.Pack(tuple.Tuple{[]byte(_id), "ndx"}))

				bucketName := string(bucketNameFuture.MustGet())
				name := string(nameFuture.MustGet())

				countKey := indexDir.Pack(tuple.Tuple{bucketName, name, "count"})
				objectCountFuture := tr.Get(countKey)

				_index, err := tuple.Unpack(ndxFuture.MustGet())
				index := _index[0].(int64)

				if index+1 == int64(binary.LittleEndian.Uint64(objectCountFuture.MustGet())) {
					// Need to reduce count so that (count - 1) always points to last file version.
					bytes := make([]byte, 8)
					binary.LittleEndian.PutUint64(bytes, uint64(index))
					tr.Set(countKey, bytes)
				}

				objectPrefixRange, err := fdb.PrefixRange(objectDir.Pack(tuple.Tuple{[]byte(_id)}))
				tr.ClearRange(objectPrefixRange)

				indexPrefixRange, err := fdb.PrefixRange(indexDir.Pack(tuple.Tuple{bucketName, name, index}))
				tr.ClearRange(indexPrefixRange)

				return nil, nil
			})
			finishChannel <- true
		}(id1)
	}
}

func get(localName string, db fdb.Database, bucketName string, names []string, verbose bool, finishChannel chan bool) {
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
				db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
					dir, err := directory.Open(tr, objectPath, nil)
					if err != nil {
						panic(err)
					}
					if chunk == 0 {
						indexDir, err := directory.Open(tr, indexDirPath, nil)
						if err != nil {
							panic(err)
						}
						countTuple := tuple.Tuple{bucketName, name, "count"}
						countKey := indexDir.Pack(countTuple)
						countValue := tr.Get(countKey).MustGet()
						if countValue == nil {
							return nil, nil
						}
						count := int64(binary.LittleEndian.Uint64(countValue))
						nameKey := indexDir.Pack(tuple.Tuple{bucketName, name, count - 1}) // Uzmi najnoviju datoteku tog imena.
						id = tr.Get(nameKey).MustGet()
						if err != nil {
							panic(err)
						}
						lengthKey := dir.Pack(tuple.Tuple{id, "len"})
						v, err := tuple.Unpack(tr.Get(lengthKey).MustGet())
						if err != nil {
							panic(err)
						}
						length = v[0].(int64)
						chunkCount = lengthToChunkCount(length)
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
				if chunk == chunkCount {
					break
				}
			}
			if verbose {
				elapsed := time.Since(timeStarted)
				fmt.Printf("Downloaded %s (%.2fMB/s).\n", name, float64(length)/1e6/elapsed.Seconds())
			}
			finishChannel <- true
		}(name1)
	}
}

func getID(localName string, db fdb.Database, ids []string, verbose bool, finishChannel chan bool) {
	for _, id1 := range ids {
		go func(id string) {
			_id := bson.ObjectIdHex(id)
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
				db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
					dir, err := directory.Open(tr, objectPath, nil)
					if err != nil {
						panic(err)
					}

					if chunk == 0 {
						lengthKey := dir.Pack(tuple.Tuple{[]byte(_id), "len"})
						v, err := tuple.Unpack(tr.Get(lengthKey).MustGet())
						if err != nil {
							panic(err)
						}
						length = v[0].(int64)
						chunkCount = lengthToChunkCount(length)
					}
					for chunk < chunkCount {
						key := dir.Pack(tuple.Tuple{[]byte(_id), chunk})
						bytesFuture := tr.Get(key)
						compressionKey := dir.Pack(tuple.Tuple{[]byte(_id), chunk, "c"})
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
				if chunk == chunkCount {
					break
				}
			}
			if verbose {
				elapsed := time.Since(timeStarted)
				fmt.Printf("Downloaded %s (%.2fMB/s).\n", id, float64(length)/1e6/elapsed.Seconds())
			}
			finishChannel <- true
		}(id1)
	}
}

func put(localName string, db fdb.Database, bucketName string, uniqueNames map[string]bool, tags map[string]string, compressionAlgorithm int, verbose bool, finishChannel chan bool) {
	compressionAlgoValue := tuple.Tuple{compressionAlgorithm}.Pack()
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
			if compressionAlgorithm == compressionAlgorithmLZ4 {
				lz4CompressedBytes = make([]byte, lz4.CompressBound(chunkSize))
			}
			if verbose {
				fmt.Printf("Uploading %s...\n", filename)
			}
			timeStarted := time.Now()
			for {
				db.Transact(func(tr fdb.Transaction) (interface{}, error) {
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
						countTuple := tuple.Tuple{bucketName, filename, "count"}
						countKey := indexDir.Pack(countTuple)
						oldCountValue := tr.Get(countKey).MustGet()
						var oldCount int64
						if oldCountValue == nil {
							oldCount = 0
						} else {
							oldCount = int64(binary.LittleEndian.Uint64(oldCountValue))
						}
						tr.Add(countKey, one)
						tr.Set(indexDir.Pack(tuple.Tuple{bucketName, filename, oldCount}), id)
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
								fmt.Printf("Failed writing chunk %d./%d: written only %d bytes.\n", chunk+1, chunkCount, n)
								panic("Failed writing chunk.")
							}
							data := contentBuffer[:n]
							switch compressionAlgorithm {
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
						fmt.Printf("Uploaded %d%% of %s (%.2fMB/s; compression ratio = %.2f).\n", currentPercent, filename, fTotalWritten/1e6/elapsed.Seconds(), fTotalWritten/compressed)
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

func putID(localName string, db fdb.Database, bucketName string, uniqueIds map[string]bool, tags map[string]string, compressionAlgorithm int, verbose bool, finishChannel chan bool) {
	compressionAlgoValue := tuple.Tuple{compressionAlgorithm}.Pack()
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
			multipleTransactions := chunkCount > chunksPerTransaction
			var totalWritten int64
			var totalWrittenCompressed int64
			contentBuffer := make([]byte, chunkSize)
			var chunk int64
			var lastPercent int
			var lz4CompressedBytes []byte
			if compressionAlgorithm == compressionAlgorithmLZ4 {
				lz4CompressedBytes = make([]byte, lz4.CompressBound(chunkSize))
			}
			if verbose {
				fmt.Printf("Uploading %s...\n", filename)
			}
			timeStarted := time.Now()
			for {
				db.Transact(func(tr fdb.Transaction) (interface{}, error) {
					dir, err := directory.CreateOrOpen(tr, objectPath, nil)
					if err != nil {
						panic(err)
					}
					if chunk == 0 {
						nameKey := dir.Pack(tuple.Tuple{id, "name"})
						if tr.Get(nameKey).MustGet() != nil {
							panic("Object with given id already exists!")
						}
						tr.Set(nameKey, []byte(filename))
						tr.Set(dir.Pack(tuple.Tuple{id, "bucket"}), []byte(bucketName))

						indexDir, _ := directory.CreateOrOpen(tr, indexDirPath, nil)
						countTuple := tuple.Tuple{bucketName, filename, "count"}
						countKey := indexDir.Pack(countTuple)
						oldCountValue := tr.Get(countKey).MustGet()
						var oldCount int64
						if oldCountValue == nil {
							oldCount = 0
						} else {
							oldCount = int64(binary.LittleEndian.Uint64(oldCountValue))
						}
						tr.Add(countKey, one)
						tr.Set(indexDir.Pack(tuple.Tuple{bucketName, filename, oldCount}), id)
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
								fmt.Printf("Failed writing chunk %d./%d: written only %d\n", chunk+1, chunkCount, n)
								panic("Failed writing chunk.")
							}
							data := contentBuffer[:n]
							switch compressionAlgorithm {
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
						fmt.Printf("Uploaded %d%% of %s (%.2fMB/s; compression ratio = %.2f).\n", currentPercent, filename, fTotalWritten/1e6/elapsed.Seconds(), fTotalWritten/compressed)
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

func database(clusterFile string) fdb.Database {
	fdb.MustAPIVersion(510)
	if clusterFile == "" {
		return fdb.MustOpenDefault()
	}
	return fdb.MustOpen(clusterFile, []byte("DB"))
}

func main() {
	if len(os.Args) < 2 || os.Args[1] == "--help" {
		usage()
		return
	}
	if len(os.Args) < 2 || os.Args[1] == "--version" {
		fmt.Printf("%s version 0.20180622\n\nCreated by Šimun Mikecin <numisemis@yahoo.com>.\n", os.Args[0])
		return
	}
	verbose := false
	var cmd string
	bucketName := "objectstorage1"
	var localName string
	allBuckets := false
	var clusterFile string
	var tags map[string]string
	compressionAlgorithm := defaultCompressionAlgorithm
	var argsIndex int
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
		if strings.HasPrefix(os.Args[i], "--metadata=") {
			tag := strings.SplitAfter(os.Args[i], "=")[1]
			value := strings.SplitAfter(os.Args[i], "=")[2]
			tags[tag] = value
		}
		if strings.HasPrefix(os.Args[i], "--local=") {
			localName = strings.SplitAfter(os.Args[i], "=")[1]
		}
		if strings.HasPrefix(os.Args[i], "--verbose") {
			verbose = true
		}
	}
	var db fdb.Database
	switch cmd {
	case "list":
		db = database(clusterFile)
		var prefix string
		if argsIndex >= 0 {
			prefix = os.Args[argsIndex]
		}
		list(db, allBuckets, bucketName, prefix)
	case "put":
		if argsIndex < 0 {
			usage()
		} else {
			db = database(clusterFile)
			uniqueNames := make(map[string]bool)
			for _, val := range os.Args[argsIndex:] {
				uniqueNames[val] = true
			}
			finishChannel := make(chan bool)
			put(localName, db, bucketName, uniqueNames, tags, compressionAlgorithm, verbose, finishChannel)
			for range uniqueNames {
				<-finishChannel
			}
		}
	case "put_id":
		if argsIndex < 0 {
			usage()
		} else {
			db = database(clusterFile)
			uniqueNames := make(map[string]bool)
			for _, val := range os.Args[argsIndex:] {
				uniqueNames[val] = true
			}
			finishChannel := make(chan bool)
			putID(localName, db, bucketName, uniqueNames, tags, compressionAlgorithm, verbose, finishChannel)
			for range uniqueNames {
				<-finishChannel
			}
		}
	case "get":
		if argsIndex < 0 {
			usage()
		} else {
			db = database(clusterFile)
			finishChannel := make(chan bool)
			get(localName, db, bucketName, os.Args[argsIndex:], verbose, finishChannel)
			for index := argsIndex; index < len(os.Args); index++ {
				<-finishChannel
			}
		}
	case "get_id":
		if argsIndex < 0 {
			usage()
		} else {
			db = database(clusterFile)
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
			db = database(clusterFile)
			finishChannel := make(chan bool)
			delete(db, bucketName, os.Args[argsIndex:], finishChannel)
			for index := argsIndex; index < len(os.Args); index++ {
				<-finishChannel
			}
		}
	case "delete_id":
		if argsIndex < 0 {
			usage()
		} else {
			db = database(clusterFile)
			finishChannel := make(chan bool)
			deleteID(db, os.Args[argsIndex:], finishChannel)
			for index := argsIndex; index < len(os.Args); index++ {
				<-finishChannel
			}
		}
	default:
		usage()
	}
}
