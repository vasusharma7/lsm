package lsm

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

//Ideally all logs should not be fatal but I am just being lazy here ;)

const (
	dataLoc   = "./data"
	MEMC_SIZE = 2096
	DEBUG     = false
)

type segtree struct {
	segs []*os.File
	size int // in terms of number of blocks of 8KB (max)
	// not being tracked in terms of bytes since a segment can have size < 8KB and not exactly 8KB in size
}

type LSM struct {
	memc  AVL
	store map[int]*segtree // full compaction with each incrementing level being 2*MEMC_SIZE
	l     sync.Mutex
}

func (lsm *LSM) Insert(key string, val []byte) error {
	// Encodes any value to bytes
	// It expects that if structs are passed to store in the tree, then their Keys should be Public.

	// var valb bytes.Buffer
	// enc := gob.NewEncoder(&valb)
	// if err := enc.Encode(val); err != nil {
	// 	return fmt.Errorf("unable to encode value to bytes: %v", err.Error())
	// }
	// lsm.memc = lsm.memc.Insert(key, valb.Bytes())
	lsm.memc = lsm.memc.Insert(key, val)
	return nil
}

func (lsm *LSM) String() string {
	return "test"
}

func (lsm *LSM) persistSeg(seg Segment, fn string) *os.File {
	if f, err := os.Create(fn); err != nil {
		log.Fatal("[ERROR]: Creating new segment file\n", err)
		return nil
	} else {
		var segb bytes.Buffer
		enc := gob.NewEncoder(&segb)
		if err := enc.Encode(seg); err != nil {
			log.Fatal("[ERROR]: unable to encode segment :", err)
		}
		if n, err := f.Write(segb.Bytes()); err != nil {
			log.Fatal("file write error", err)
		} else if n != len(segb.Bytes()) {
			log.Fatal("[ERROR]: incomplete segment written to file")
		}
		return f
	}
}

func (lsm *LSM) PersistMemC() error {
	seg := NewSegment(lsm.memc)
	segf := fmt.Sprintf("%v/seg_0_%v", dataLoc, time.Now().UnixMilli())

	f := lsm.persistSeg(seg, segf)

	if lsm.store[0] == nil {
		lsm.store[0] = &segtree{size: 0, segs: []*os.File{}}
	}
	//compaction will take care of expanded size
	lsm.l.Lock()
	lsm.store[0].segs = append(lsm.store[0].segs, f)
	lsm.store[0].size += 1
	lsm.l.Unlock()
	lsm.memc = InitAVLTree()
	return nil
}

func New() (*LSM, error) {
	tree := map[int]*segtree{}
	if _, err := os.Stat(dataLoc); errors.Is(err, os.ErrNotExist) {
		if err = os.Mkdir(dataLoc, 0700); err != nil {
			return nil, err
		}
	} else {
		//load segment locations from memory
		// Assumes that the segments were persisted without any errors ,
		// full compaction and no need of WAL recovery etc.
		if DEBUG {
			log.Print("[DEBUG]: Data folder exists\n")
		}
		var files []fs.DirEntry
		if files, err = os.ReadDir(dataLoc); err != nil {
			return nil, err
		}
		sort.Slice(files, func(i, j int) bool {
			return strings.Compare(files[i].Name(), files[j].Name()) == -1
		})
		for _, file := range files {
			fLoc := fmt.Sprintf("%v/%v", dataLoc, file.Name())
			if f, err := os.Open(fLoc); err != nil {
				log.Fatal("[ERROR]: Error opening segment file \n", fLoc)
				continue
			} else {
				name := strings.Split(file.Name(), "_")
				if len(name) == 3 {
					if level, err := strconv.Atoi(name[1]); err == nil {
						if tree[level] == nil {
							tree[level] = &segtree{size: 0, segs: []*os.File{}}
						}
						tree[level].segs = append(tree[level].segs, f)
					}
				}
			}
		}
		if DEBUG {
			log.Printf("[DEBUG]: Loaded %v segments\n", len(files))
		}
	}
	lsm := &LSM{memc: InitAVLTree(), store: tree, l: sync.Mutex{}}
	go func() {
		for {
			size := lsm.memc.Size()
			if size > MEMC_SIZE {
				lsm.PersistMemC()
				lsm.Compact()
			} else {
				if DEBUG {
					log.Printf("[DEBUG]: memcache size: %d", size)
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()
	return lsm, nil
}

func (lsm *LSM) ReadSegmentFile(f *os.File) *Segment {
	var seg Segment
	buff := make([]byte, 256*1024*1024)
	// it is not gauranteed that someone didn't delete the file till now, next lock is not effective heree.
	f.Seek(0, 0)
	lsm.l.Lock()
	if _, err := f.Read(buff); err != nil {
		lsm.l.Unlock()
		log.Fatal("[ERROR]: reading segment ", err)
	} else {
		lsm.l.Unlock()
		// if DEBUG {
		// 	log.Printf("[DEBUG]: read %v segment bytes ", n)
		// }
		dec := gob.NewDecoder(bytes.NewBuffer(buff))
		if err := dec.Decode(&seg); err != nil {
			log.Fatal("[ERROR]: segment corrupted, decode error ", err)
		}
	}
	return &seg
}

func (lsm *LSM) Search(key string) []byte {
	if pair := lsm.memc.Search(key); pair != nil {
		if pair.Tomb {
			return nil
		}
		return pair.Val
	}
	for _, level := range lsm.store {
		for i := len(level.segs) - 1; i >= 0; i -= 1 {
			//hack for parallelisation
			if len(level.segs) <= i {
				continue
			}
			f := level.segs[i]
			seg := lsm.ReadSegmentFile(f)
			if strings.Compare(seg.Pairs[0].Key, key) == 1 {
				continue
			}
			if pair := seg.Search(key); pair != nil {
				return pair.Val
			}
			//Add Bloom filter in addition to binary search
		}
	}
	return nil
}

func (lsm *LSM) delOldSegs(files ...*os.File) {
	for _, f := range files {
		lsm.l.Lock()
		if err := os.Remove(f.Name()); err != nil {
			if DEBUG {
				log.Print("[ERROR]: unable to remove old segment ", err)
			}
		}
		lsm.l.Unlock()
	}
}

func (lsm *LSM) Compact() {
	if DEBUG {
		log.Print("[DEBUG]: Compaction Running")
	}
	currL := []int{}
	for k := range lsm.store {
		currL = append(currL, k)
	}
	for _, level := range currL {
		if DEBUG {
			log.Printf("[DEBUG]: level: %v, size : %v, segs: %v", level, lsm.store[level].size, len(lsm.store[level].segs))
		}
		if lsm.store[level].size <= level+1 {
			continue
		}
		segs := []*Segment{}
		for _, seg := range lsm.store[level].segs {
			segs = append(segs, lsm.ReadSegmentFile(seg))
		}
		if lsm.store[level+1] == nil {
			lsm.l.Lock()
			lsm.store[level+1] = &segtree{size: 0}
			lsm.l.Unlock()
		}
		segsN := []*Segment{}
		oldSegsN := lsm.store[level+1].segs
		for _, seg := range oldSegsN {
			segsN = append(segsN, lsm.ReadSegmentFile(seg))
		}
		m := MergeSegments(append(segs, segsN...)...)
		lsm.l.Lock()
		lsm.store[level+1].segs = []*os.File{lsm.persistSeg(m, fmt.Sprintf("%v/seg_%v_%v", dataLoc, level+1, time.Now().UnixMilli()))}
		lsm.store[level+1].size += 1
		lsm.l.Unlock()

		lsm.delOldSegs(append(lsm.store[level].segs, oldSegsN...)...)

		lsm.l.Lock()
		lsm.store[level].size = 0
		lsm.store[level].segs = []*os.File{}
		lsm.l.Unlock()
	}
}
