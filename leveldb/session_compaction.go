// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"sort"
	"sync/atomic"

	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/memdb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const (
	undefinedCompaction = iota
	level0Compaction
	nonLevel0Compaction
	seekCompaction
)

func (s *session) pickMemdbLevel(umin, umax []byte, maxLevel int) int {
	v := s.version()
	defer v.release()
	return v.pickMemdbLevel(umin, umax, maxLevel)
}

func (s *session) flushMemdb(rec *sessionRecord, mdb *memdb.DB, maxLevel int) (int, error) {
	// Create sorted table.
	iter := mdb.NewIterator(nil)
	defer iter.Release()
	t, n, err := s.tops.createFrom(iter)
	if err != nil {
		return 0, err
	}

	// Pick level other than zero can cause compaction issue with large
	// bulk insert and delete on strictly incrementing key-space. The
	// problem is that the small deletion markers trapped at lower level,
	// while key/value entries keep growing at higher level. Since the
	// key-space is strictly incrementing it will not overlaps with
	// higher level, thus maximum possible level is always picked, while
	// overlapping deletion marker pushed into lower level.
	// See: https://github.com/syndtr/goleveldb/issues/127.
	flushLevel := s.pickMemdbLevel(t.imin.ukey(), t.imax.ukey(), maxLevel)
	rec.addTableFile(flushLevel, t)

	s.logf("memdb@flush created L%d@%d N·%d S·%s %q:%q", flushLevel, t.fd.Num, n, shortenb(int(t.size)), t.imin, t.imax)
	return flushLevel, nil
}

// Pick a compaction based on current state; need external synchronization.
// func (s *session) pickCompaction() *compaction { // orig
// 	v := s.version()

// 	var sourceLevel int
// 	var t0 tFiles
// 	var typ int
// 	if v.cScore >= 1 {
// 		sourceLevel = v.cLevel
// 		cptr := s.getCompPtr(sourceLevel)
// 		tables := v.levels[sourceLevel]
// 		if cptr != nil && sourceLevel > 0 {
// 			n := len(tables)
// 			if i := sort.Search(n, func(i int) bool {
// 				return s.icmp.Compare(tables[i].imax, cptr) > 0
// 			}); i < n {
// 				t0 = append(t0, tables[i])
// 			}
// 		}
// 		if len(t0) == 0 {
// 			t0 = append(t0, tables[0])
// 		}
// 		if sourceLevel == 0 {
// 			typ = level0Compaction
// 		} else {
// 			typ = nonLevel0Compaction
// 		}
// 	} else {
// 		if p := atomic.LoadPointer(&v.cSeek); p != nil {
// 			ts := (*tSet)(p)
// 			sourceLevel = ts.level
// 			t0 = append(t0, ts.table)
// 			typ = seekCompaction
// 		} else {
// 			v.release()
// 			return nil
// 		}
// 	}

// 	return newCompaction(s, v, sourceLevel, t0, typ)
// }

// KIR
func (s *session) pickCompaction() []*compaction {
	v := s.version()

	var cLevels []int
	for i, score := range v.cScores {
		if score >= 1.0 {
			cLevels = append(cLevels, i)
		}
	}

	doParallelComp := (len(cLevels) != 0) && (s.o.Options.GetParallelCompOption() == int32(1))

	sourceLevel := make([]int, len(cLevels)+1)
	t0 := make([]tFiles, len(cLevels)+1)
	typ := make([]int, len(cLevels)+1)
	scores := make([]float64, len(cLevels)+1)

	if v.cScore >= 1 {
		sourceLevel[0] = v.cLevel
		cptr := s.getCompPtr(sourceLevel[0])
		tables := v.levels[sourceLevel[0]]
		scores[0] = v.cScore
		if cptr != nil && sourceLevel[0] > 0 {
			n := len(tables)
			if i := sort.Search(n, func(i int) bool {
				return s.icmp.Compare(tables[i].imax, cptr) > 0
			}); i < n {
				t0[0] = append(t0[0], tables[i])
			}
		}
		if len(t0[0]) == 0 {
			t0[0] = append(t0[0], tables[0])
		}
		if sourceLevel[0] == 0 {
			typ[0] = level0Compaction
		} else {
			typ[0] = nonLevel0Compaction
		}

		if doParallelComp {
			for i, cLevel := range cLevels {
				sourceLevel[i+1] = cLevel
				cptr := s.getCompPtr(sourceLevel[i+1])
				tables := v.levels[sourceLevel[i+1]]
				scores[i+1] = v.cScores[cLevel]
				if cptr != nil && sourceLevel[i+1] > 0 {
					n := len(tables)
					if j := sort.Search(n, func(i int) bool {
						return s.icmp.Compare(tables[i].imax, cptr) > 0
					}); j < n {
						t0[i+1] = append(t0[i+1], tables[j])
					}
				}
				if len(t0[i+1]) == 0 {
					t0[i+1] = append(t0[i+1], tables[0])
				}
				if sourceLevel[i+1] == 0 {
					typ[i+1] = level0Compaction
				} else {
					typ[i+1] = nonLevel0Compaction
				}
			}
		}
	} else {
		if p := atomic.LoadPointer(&v.cSeek); p != nil {
			ts := (*tSet)(p)
			sourceLevel[0] = ts.level
			t0[0] = append(t0[0], ts.table)
			typ[0] = seekCompaction
			scores[0] = v.cScore
		} else {
			v.release()
			return nil
		}
	}

	var cs []*compaction

	if doParallelComp {
		for i := 0; i < len(sourceLevel); i++ {
			cs = append(cs, newCompaction_m(s, v, sourceLevel[i], t0[i], typ[i], scores[i]))
		}
	} else {
		cs = append(cs, newCompaction_m(s, v, sourceLevel[0], t0[0], typ[0], scores[0]))
	}

	return cs
}

// Create compaction from given level and range; need external synchronization.
func (s *session) getCompactionRange(sourceLevel int, umin, umax []byte, noLimit bool) *compaction {
	v := s.version()

	if sourceLevel >= len(v.levels) {
		v.release()
		return nil
	}

	t0 := v.levels[sourceLevel].getOverlaps(nil, s.icmp, umin, umax, sourceLevel == 0)
	if len(t0) == 0 {
		v.release()
		return nil
	}

	// Avoid compacting too much in one shot in case the range is large.
	// But we cannot do this for level-0 since level-0 files can overlap
	// and we must not pick one file and drop another older file if the
	// two files overlap.
	if !noLimit && sourceLevel > 0 {
		limit := int64(v.s.o.GetCompactionSourceLimit(sourceLevel))
		total := int64(0)
		for i, t := range t0 {
			total += t.size
			if total >= limit {
				s.logf("table@compaction limiting F·%d -> F·%d", len(t0), i+1)
				t0 = t0[:i+1]
				break
			}
		}
	}

	typ := level0Compaction
	if sourceLevel != 0 {
		typ = nonLevel0Compaction
	}
	return newCompaction(s, v, sourceLevel, t0, typ)
}

func newCompaction(s *session, v *version, sourceLevel int, t0 tFiles, typ int) *compaction { //  orig
	c := &compaction{
		s:             s,
		v:             v,
		typ:           typ,
		sourceLevel:   sourceLevel,
		targetLevel:   sourceLevel + 1,
		levels:        [2]tFiles{t0, nil},
		maxGPOverlaps: int64(s.o.GetCompactionGPOverlaps(sourceLevel)),
		tPtrs:         make([]int, len(v.levels)),
	}
	c.expand()
	c.save()
	return c
}

// KIR
func newCompaction_m(s *session, v *version, sourceLevel int, t0 tFiles, typ int, cScore float64) *compaction {

	c := &compaction{
		s:             s,
		v:             v,
		typ:           typ,
		sourceLevel:   sourceLevel,
		targetLevel:   sourceLevel + 1,
		levels:        [2]tFiles{t0, nil},
		cScore:        cScore,
		maxGPOverlaps: int64(s.o.GetCompactionGPOverlaps(sourceLevel)),
		tPtrs:         make([]int, len(v.levels)),
	}
	if s.o.Options.GetRO() >= 1 && s.o.Options.GetParallelCompOption() == 0 {
		c.expand_RO()
	} else {
		c.expand()
	}
	c.save()
	return c
}

// compaction represent a compaction state.
type compaction struct {
	s *session
	v *version

	typ           int
	sourceLevel   int
	targetLevel   int //dk mod
	levels        [2]tFiles
	maxGPOverlaps int64

	gp                tFiles
	gpi               int
	seenKey           bool
	gpOverlappedBytes int64
	imin, imax        internalKey
	tPtrs             []int
	released          bool

	snapGPI               int
	snapSeenKey           bool
	snapGPOverlappedBytes int64
	snapTPtrs             []int

	//KIR
	compBuilder *tableCompactionBuilder
	stats       [2]cStatStaging
	cScore      float64
	cID         int
}

// KIR
type mergedCompaction struct {
	s *session
	v *version

	cs []*compaction

	stat    cStatStaging
	kerrCnt int
	dropCnt int
}

func (c *compaction) save() {
	c.snapGPI = c.gpi
	c.snapSeenKey = c.seenKey
	c.snapGPOverlappedBytes = c.gpOverlappedBytes
	c.snapTPtrs = append(c.snapTPtrs[:0], c.tPtrs...)
}

func (c *compaction) restore() {
	c.gpi = c.snapGPI
	c.seenKey = c.snapSeenKey
	c.gpOverlappedBytes = c.snapGPOverlappedBytes
	c.tPtrs = append(c.tPtrs[:0], c.snapTPtrs...)
}

func (c *compaction) release() {
	if !c.released {
		c.released = true
		c.v.release()
	}
}

// KIR
// Modified Expand for reducing Overlapping
// Expand compacted tables; need external synchronization.
func (c *compaction) expand_RO() {
	limit := int64(c.s.o.GetCompactionExpandLimit(c.sourceLevel))
	vt0 := c.v.levels[c.sourceLevel]
	vt1 := tFiles{}
	var t1mod tFiles
	vt1mod := tFiles{}
	var origTargetLevel int
	var newTargetLevel int

	if origTargetLevel = c.sourceLevel + 1; origTargetLevel < len(c.v.levels) {
		vt1 = c.v.levels[origTargetLevel]
	}

	t0, t1 := c.levels[0], c.levels[1]
	imin, imax := t0.getRange(c.s.icmp)

	// For non-zero levels, the ukey can't hop across tables at all.
	if c.sourceLevel == 0 {
		// We expand t0 here just incase ukey hop across tables.
		t0 = vt0.getOverlaps(t0, c.s.icmp, imin.ukey(), imax.ukey(), c.sourceLevel == 0)
		if len(t0) != len(c.levels[0]) {
			imin, imax = t0.getRange(c.s.icmp)
		}
	}
	t1 = vt1.getOverlaps(t1, c.s.icmp, imin.ukey(), imax.ukey(), false)

	if len(t1) == 0 {
		newTargetLevel = origTargetLevel + 1
		for len(t1mod) == 0 && newTargetLevel < len(c.v.levels) {
			vt1mod = c.v.levels[newTargetLevel]
			t1mod = vt1mod.getOverlaps(t1mod, c.s.icmp, imin.ukey(), imax.ukey(), false)
			newTargetLevel++
		}
	}
	if len(t1mod) > 0 {
		t1 = t1mod
		c.targetLevel = newTargetLevel - 1
	}
	// Get entire range covered by compaction.
	amin, amax := append(t0, t1...).getRange(c.s.icmp)

	// See if we can grow the number of inputs in "sourceLevel" without
	// changing the number of "sourceLevel+1" files we pick up.
	if len(t1) > 0 {
		exp0 := vt0.getOverlaps(nil, c.s.icmp, amin.ukey(), amax.ukey(), c.sourceLevel == 0)
		if len(exp0) > len(t0) && t1.size()+exp0.size() < limit {
			xmin, xmax := exp0.getRange(c.s.icmp)
			exp1 := vt1.getOverlaps(nil, c.s.icmp, xmin.ukey(), xmax.ukey(), false)
			if len(exp1) == len(t1) {
				c.s.logf("table@compaction expanding L%d+L%d (F·%d S·%s)+(F·%d S·%s) -> (F·%d S·%s)+(F·%d S·%s)",
					c.sourceLevel, c.targetLevel, len(t0), shortenb(int(t0.size())), len(t1), shortenb(int(t1.size())),
					len(exp0), shortenb(int(exp0.size())), len(exp1), shortenb(int(exp1.size())))
				imin, imax = xmin, xmax
				t0, t1 = exp0, exp1
				amin, amax = append(t0, t1...).getRange(c.s.icmp)
			}
		}
	}

	// Compute the set of grandparent files that overlap this compaction
	// (parent == sourceLevel+1; grandparent == sourceLevel+2)
	if level := c.sourceLevel + 2; level < len(c.v.levels) {
		c.gp = c.v.levels[level].getOverlaps(c.gp, c.s.icmp, amin.ukey(), amax.ukey(), false) // c.gp에 겹치는 gp 테이블들 저장
	}

	c.levels[0], c.levels[1] = t0, t1
	c.imin, c.imax = imin, imax
}

func (c *compaction) expand() {
	limit := int64(c.s.o.GetCompactionExpandLimit(c.sourceLevel))
	vt0 := c.v.levels[c.sourceLevel]
	vt1 := tFiles{}
	if level := c.sourceLevel + 1; level < len(c.v.levels) {
		vt1 = c.v.levels[level]
	}

	t0, t1 := c.levels[0], c.levels[1]
	imin, imax := t0.getRange(c.s.icmp)

	// For non-zero levels, the ukey can't hop across tables at all.
	if c.sourceLevel == 0 {
		// We expand t0 here just incase ukey hop across tables.
		t0 = vt0.getOverlaps(t0, c.s.icmp, imin.ukey(), imax.ukey(), c.sourceLevel == 0)
		if len(t0) != len(c.levels[0]) {
			imin, imax = t0.getRange(c.s.icmp)
		}
	}
	t1 = vt1.getOverlaps(t1, c.s.icmp, imin.ukey(), imax.ukey(), false)
	// Get entire range covered by compaction.
	amin, amax := append(t0, t1...).getRange(c.s.icmp)

	// See if we can grow the number of inputs in "sourceLevel" without
	// changing the number of "sourceLevel+1" files we pick up.
	if len(t1) > 0 {
		exp0 := vt0.getOverlaps(nil, c.s.icmp, amin.ukey(), amax.ukey(), c.sourceLevel == 0)
		if len(exp0) > len(t0) && t1.size()+exp0.size() < limit {
			xmin, xmax := exp0.getRange(c.s.icmp)
			exp1 := vt1.getOverlaps(nil, c.s.icmp, xmin.ukey(), xmax.ukey(), false)
			if len(exp1) == len(t1) {
				c.s.logf("table@compaction expanding L%d+L%d (F·%d S·%s)+(F·%d S·%s) -> (F·%d S·%s)+(F·%d S·%s)",
					c.sourceLevel, c.sourceLevel+1, len(t0), shortenb(int(t0.size())), len(t1), shortenb(int(t1.size())),
					len(exp0), shortenb(int(exp0.size())), len(exp1), shortenb(int(exp1.size())))
				imin, imax = xmin, xmax
				t0, t1 = exp0, exp1
				amin, amax = append(t0, t1...).getRange(c.s.icmp)
			}
		}
	}

	// Compute the set of grandparent files that overlap this compaction
	// (parent == sourceLevel+1; grandparent == sourceLevel+2)
	if level := c.sourceLevel + 2; level < len(c.v.levels) {
		c.gp = c.v.levels[level].getOverlaps(c.gp, c.s.icmp, amin.ukey(), amax.ukey(), false)
	}

	c.levels[0], c.levels[1] = t0, t1
	c.imin, c.imax = imin, imax

}

// Check whether compaction is trivial.
func (c *compaction) trivial() bool {
	return len(c.levels[0]) == 1 && len(c.levels[1]) == 0 && c.gp.size() <= c.maxGPOverlaps
}

func (c *compaction) baseLevelForKey(ukey []byte) bool {
	for level := c.sourceLevel + 2; level < len(c.v.levels); level++ {
		tables := c.v.levels[level]
		for c.tPtrs[level] < len(tables) {
			t := tables[c.tPtrs[level]]
			if c.s.icmp.uCompare(ukey, t.imax.ukey()) <= 0 {
				// We've advanced far enough.
				if c.s.icmp.uCompare(ukey, t.imin.ukey()) >= 0 {
					// Key falls in this file's range, so definitely not base level.
					return false
				}
				break
			}
			c.tPtrs[level]++
		}
	}
	return true
}

func (c *compaction) shouldStopBefore(ikey internalKey) bool {
	for ; c.gpi < len(c.gp); c.gpi++ {
		gp := c.gp[c.gpi]
		if c.s.icmp.Compare(ikey, gp.imax) <= 0 {
			break
		}
		if c.seenKey {
			c.gpOverlappedBytes += gp.size
		}
	}
	c.seenKey = true

	if c.gpOverlappedBytes > c.maxGPOverlaps {
		// Too much overlap for current output; start new output.
		c.gpOverlappedBytes = 0
		return true
	}
	return false
}

// Creates an iterator.
func (c *compaction) newIterator() iterator.Iterator {
	// Creates iterator slice.
	icap := len(c.levels)
	if c.sourceLevel == 0 {
		// Special case for level-0.
		icap = len(c.levels[0]) + 1
	}
	its := make([]iterator.Iterator, 0, icap)

	// Options.
	ro := &opt.ReadOptions{
		DontFillCache: true,
		Strict:        opt.StrictOverride,
	}
	strict := c.s.o.GetStrict(opt.StrictCompaction)
	if strict {
		ro.Strict |= opt.StrictReader
	}

	for i, tables := range c.levels {
		if len(tables) == 0 {
			continue
		}

		// Level-0 is not sorted and may overlaps each other.
		if c.sourceLevel+i == 0 {
			for _, t := range tables {
				its = append(its, c.s.tops.newIterator(t, nil, ro))
			}
		} else {
			it := iterator.NewIndexedIterator(tables.newIndexIterator(c.s.tops, c.s.icmp, nil, ro), strict)
			its = append(its, it)
		}
	}

	return iterator.NewMergedIterator(its, c.s.icmp, strict)
}
