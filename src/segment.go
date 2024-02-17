package lsm

import (
	"strings"
)

type Segment struct {
	Pairs []Pair
}

//	func (seg *Segment) Insert(pair Pair) *Segment {
//		seg.Pairs = append(seg.Pairs, pair)
//		return seg
//	}
func NewSegment(tree AVL) Segment {
	Pairs := []Pair{}
	tree.Inorder(&Pairs)
	return Segment{Pairs}
}

func MergeSegments(segs ...*Segment) Segment {
	out := Segment{Pairs: []Pair{}}
	ptrs := make([]int, len(segs))
	for {
		smk := ""
		smi := -1
		cont := false
		for i, ptr := range ptrs {
			if ptr >= len(segs[i].Pairs) {
				continue
			}
			cont = true
			if smi == -1 {
				smk = segs[i].Pairs[ptr].Key
				smi = i
				continue
			}
			if strings.Compare(segs[i].Pairs[ptr].Key, smk) == -1 {
				smi = i
				smk = segs[i].Pairs[ptr].Key
			}
		}
		if !cont {
			break
		}
		if len(out.Pairs) > 0 && out.Pairs[len(out.Pairs)-1].Key == segs[smi].Pairs[ptrs[smi]].Key {
			//replace old value | Overwrite
			// TODO: delete if null
			out.Pairs[len(out.Pairs)-1] = segs[smi].Pairs[ptrs[smi]]
		} else {
			out.Pairs = append(out.Pairs, segs[smi].Pairs[ptrs[smi]])
		}
		ptrs[smi] += 1
	}

	return out
}

func (seg *Segment) Size() int {
	return len(seg.Pairs)
}

func (seg *Segment) Search(key string) *Pair {
	return seg.searchI(key, 0, len(seg.Pairs)-1)
}

func (seg *Segment) searchI(key string, start, end int) *Pair {
	if end < start {
		return nil
	}
	mid := start + (end-start)/2

	if seg.Pairs[mid].Key == key {
		return &seg.Pairs[mid]
	}
	if strings.Compare(seg.Pairs[mid].Key, key) == -1 {
		return seg.searchI(key, mid+1, end)
	} else {
		return seg.searchI(key, start, mid-1)
	}
}
