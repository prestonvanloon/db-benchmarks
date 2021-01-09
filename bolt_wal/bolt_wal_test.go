package boltWalBenchmark

import (
	"errors"
	"fmt"
	"math/rand"
	"path"
	"sync"
	"testing"
	"time"

	. "github.com/rawfalafel/db-benchmarks"
	"github.com/tidwall/wal"
	bolt "go.etcd.io/bbolt"
)

func setupBolt(t *testing.T, noSync bool) (*bolt.DB, *wal.Log) {
	datadir := SetupDir("bolt", t)
	datafile := path.Join(datadir, "bolt.db")

	db, err := bolt.Open(datafile, 0600, nil)
	db.NoSync = noSync

	if err != nil {
		t.Fatalf("failed to open db: %v", err)
	}

	log, err := wal.Open(path.Join(datadir, "wal.db"), &wal.Options{
		NoCopy: true,
	})
	if err != nil {
		t.Fatalf("failed to open wal db: %v", err)
	}

	return db, log
}

func _randomWrite(log *wal.Log, r *rand.Rand) error {
	k, v := GenerateKV(r)
	d := append(k, v...)
	i, err := log.LastIndex()
	if err != nil {
		return err
	}
	return log.Write(i+1, d)
}

func _flush(db *bolt.DB, log *wal.Log) error {
	err := db.Update(func(tx *bolt.Tx) error {
		end, err := log.LastIndex()
		if err != nil {
			return err
		}
		b, err := tx.CreateBucketIfNotExists([]byte("MyBucket"))
		if err != nil {
			return fmt.Errorf("create bucket: %v", err)
		}
		if end <= 0 {
			return errors.New("no entries to write")
		}
		for i := uint64(0); i <= end; i++ {
			d, err := log.Read(i)
			if err != nil {
				return err
			}
			if len(d) != 332 { // test assumption
				return errors.New("data is incorrect length")
			}
			k, v := d[:31], d[32:]
			if err := b.Put(k, v); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("update failed: %v", err)
	}
	return log.TruncateBack(0)
}

func _batchWrite(db *bolt.DB, r *rand.Rand) error {
	k, v := GenerateKV(r)

	err := db.Batch(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte("MyBucket"))
		if err != nil {
			return fmt.Errorf("create bucket: %v", err)
		}

		return b.Put(k, v)
	})
	if err != nil {
		return fmt.Errorf("batch update failed: %v", err)
	}

	return nil
}

func _randomRead(db *bolt.DB, r *rand.Rand) error {
	key := make([]byte, 32)
	r.Read(key)

	value := make([]byte, 300)
	r.Read(value)

	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("MyBucket"))
		v := b.Get(key)
		if len(v) != len(value) {
			return fmt.Errorf("read value does not match: %v", v)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("update failed: %v", err)
	}

	return nil
}

func randomWrite(t *testing.T, db *bolt.DB, log *wal.Log, size int) {
	s := rand.NewSource(1)
	r := rand.New(s)
	start := time.Now()
	for i := 0; i < size; i++ {
		if err := _randomWrite(log, r); err != nil {
			t.Fatal(err)
		}
	}
	if i, err := log.LastIndex(); err != nil {
		t.Fatal(err)
	} else {
		t.Logf("There are %d keys in WAL", i)
	}
	TrackTime(start, "wal entries")
	_flush(db, log)
}

func randomRead(t *testing.T, db *bolt.DB, log *wal.Log, size int) {
	// TODO: Put some keys in the log to scan.
	s := rand.NewSource(1)
	r := rand.New(s)
	for i := 0; i < size; i++ {
		if err := _randomRead(db, r); err != nil {
			t.Fatal(err)
		}
	}
}

func concurrentWrite(t *testing.T, db *bolt.DB, log *wal.Log, size, partitions int) {
	wg := sync.WaitGroup{}
	sizePerPartition := size / partitions
	var batchErr error
	for i := 0; i < partitions; i++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()

			s := rand.NewSource(seed)
			r := rand.New(s)

			err := db.Batch(func(tx *bolt.Tx) error {
				b, err := tx.CreateBucketIfNotExists([]byte("mybucket"))
				if err != nil {
					return fmt.Errorf("failed to create bucket: %v", err)
				}

				for i := 0; i < sizePerPartition; i++ {
					k, v := GenerateKV(r)
					err := b.Put(k, v)
					if err != nil {
						return fmt.Errorf("failed to put bucket: %v", err)
					}
				}
				return nil
			})

			batchErr = err
		}(int64(i))
	}
	wg.Wait()
	if batchErr != nil {
		t.Fatalf("batch write failed: %v", batchErr)
	}
}

func TestBoltWrite(t *testing.T) {
	db, log := setupBolt(t, false)
	defer db.Close()
	defer TrackTime(time.Now(), "wal write + flush + bolt write")

	randomWrite(t, db, log, 1<<17)
}

// func TestBoltRead(t *testing.T) {
// 	db, log := setupBolt(t, true)
// 	defer db.Close()

// 	randomWrite(t, db, 1<<20)
// 	defer TrackTime(time.Now(), "bolt read")

// 	randomRead(t, db, log, 1<<20)
// }

func TestBoltConcurrentWrite(t *testing.T) {
	db, log := setupBolt(t, false)
	defer db.Close()

	defer TrackTime(time.Now(), "bolt concurrent write")
	concurrentWrite(t, db, log, 1<<17, 1<<12)
}
