// Copyright (c) 2012, Suryandaru Triandana <syndtr@gmail.com>
// All rights reserved.
//
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

package leveldb

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

var (
	errCompactionTransactExiting = errors.New("leveldb: compaction transact exiting")
)

type cStat struct {
	duration time.Duration
	read     int64
	write    int64
}

func (p *cStat) add(n *cStatStaging) {
	p.duration += n.duration
	p.read += n.read
	p.write += n.write
}

func (p *cStat) get() (duration time.Duration, read, write int64) {
	return p.duration, p.read, p.write
}

type cStatStaging struct {
	start    time.Time
	duration time.Duration
	on       bool
	read     int64
	write    int64
}

func (p *cStatStaging) startTimer() {
	if !p.on {
		p.start = time.Now()
		p.on = true
	}
}

func (p *cStatStaging) stopTimer() {
	if p.on {
		p.duration += time.Since(p.start)
		p.on = false
	}
}

type cStats struct {
	lk    sync.Mutex
	stats []cStat
	// KIR
	mergedCompStats cStat
}

// KIR
func (p *cStats) addmergedCompStat(n *cStatStaging) {
	p.lk.Lock()
	p.mergedCompStats.add(n)
	p.lk.Unlock()
}

func (p *cStats) addStat(level int, n *cStatStaging) {
	p.lk.Lock()
	if level >= len(p.stats) {
		newStats := make([]cStat, level+1)
		copy(newStats, p.stats)
		p.stats = newStats
	}
	p.stats[level].add(n)
	p.lk.Unlock()
}

func (p *cStats) getStat(level int) (duration time.Duration, read, write int64) {
	p.lk.Lock()
	defer p.lk.Unlock()
	if level < len(p.stats) {
		return p.stats[level].get()
	}
	return
}

func (db *DB) compactionError() {
	var err error
noerr:
	// No error.
	for {
		select {
		case err = <-db.compErrSetC:
			switch {
			case err == nil:
			case err == ErrReadOnly, errors.IsCorrupted(err):
				goto hasperr
			default:
				goto haserr
			}
		case <-db.closeC:
			return
		}
	}
haserr:
	// Transient error.
	for {
		select {
		case db.compErrC <- err:
		case err = <-db.compErrSetC:
			switch {
			case err == nil:
				goto noerr
			case err == ErrReadOnly, errors.IsCorrupted(err):
				goto hasperr
			default:
			}
		case <-db.closeC:
			return
		}
	}
hasperr:
	// Persistent error.
	for {
		select {
		case db.compErrC <- err:
		case db.compPerErrC <- err:
		case db.writeLockC <- struct{}{}:
			// Hold write lock, so that write won't pass-through.
			db.compWriteLocking = true
		case <-db.closeC:
			if db.compWriteLocking {
				// We should release the lock or Close will hang.
				<-db.writeLockC
			}
			return
		}
	}
}

type compactionTransactCounter int

func (cnt *compactionTransactCounter) incr() {
	*cnt++
}

type compactionTransactInterface interface {
	run(cnt *compactionTransactCounter) error
	revert() error
}

func (db *DB) compactionTransact(name string, t compactionTransactInterface) {
	defer func() {
		if x := recover(); x != nil {
			if x == errCompactionTransactExiting {
				if err := t.revert(); err != nil {
					db.logf("%s revert error %q", name, err)
				}
			}
			panic(x)
		}
	}()

	const (
		backoffMin = 1 * time.Second
		backoffMax = 8 * time.Second
		backoffMul = 2 * time.Second
	)
	var (
		backoff  = backoffMin
		backoffT = time.NewTimer(backoff)
		lastCnt  = compactionTransactCounter(0)

		disableBackoff = db.s.o.GetDisableCompactionBackoff()
	)
	for n := 0; ; n++ {
		// Check whether the DB is closed.
		if db.isClosed() {
			db.logf("%s exiting", name)
			db.compactionExitTransact()
		} else if n > 0 {
			db.logf("%s retrying N·%d", name, n)
		}

		// Execute.
		cnt := compactionTransactCounter(0)
		err := t.run(&cnt)
		if err != nil {
			db.logf("%s error I·%d %q", name, cnt, err)
		}

		// Set compaction error status.
		select {
		case db.compErrSetC <- err:
		case perr := <-db.compPerErrC:
			if err != nil {
				db.logf("%s exiting (persistent error %q)", name, perr)
				db.compactionExitTransact()
			}
		case <-db.closeC:
			db.logf("%s exiting", name)
			db.compactionExitTransact()
		}
		if err == nil {
			return
		}
		if errors.IsCorrupted(err) {
			db.logf("%s exiting (corruption detected)", name)
			db.compactionExitTransact()
		}

		if !disableBackoff {
			// Reset backoff duration if counter is advancing.
			if cnt > lastCnt {
				backoff = backoffMin
				lastCnt = cnt
			}

			// Backoff.
			backoffT.Reset(backoff)
			if backoff < backoffMax {
				backoff *= backoffMul
				if backoff > backoffMax {
					backoff = backoffMax
				}
			}
			select {
			case <-backoffT.C:
			case <-db.closeC:
				db.logf("%s exiting", name)
				db.compactionExitTransact()
			}
		}
	}
}

type compactionTransactFunc struct {
	runFunc    func(cnt *compactionTransactCounter) error
	revertFunc func() error
}

func (t *compactionTransactFunc) run(cnt *compactionTransactCounter) error {
	return t.runFunc(cnt)
}

func (t *compactionTransactFunc) revert() error {
	if t.revertFunc != nil {
		return t.revertFunc()
	}
	return nil
}

func (db *DB) compactionTransactFunc(name string, run func(cnt *compactionTransactCounter) error, revert func() error) {
	db.compactionTransact(name, &compactionTransactFunc{run, revert})
}

func (db *DB) compactionExitTransact() {
	panic(errCompactionTransactExiting)
}

func (db *DB) compactionCommit(name string, rec *sessionRecord) {
	db.compCommitLk.Lock()
	defer db.compCommitLk.Unlock() // Defer is necessary.
	db.compactionTransactFunc(name+"@commit", func(cnt *compactionTransactCounter) error {
		return db.s.commit(rec, true)
	}, nil)
}

func (db *DB) memCompaction() {
	mdb := db.getFrozenMem()
	if mdb == nil {
		return
	}
	defer mdb.decref()

	db.logf("memdb@flush N·%d S·%s", mdb.Len(), shortenb(mdb.Size()))

	// Don't compact empty memdb.
	if mdb.Len() == 0 {
		db.logf("memdb@flush skipping")
		// drop frozen memdb
		db.dropFrozenMem()
		return
	}

	// Pause table compaction.
	resumeC := make(chan struct{})
	select {
	case db.tcompPauseC <- (chan<- struct{})(resumeC):
	case <-db.compPerErrC:
		close(resumeC)
		resumeC = nil
	case <-db.closeC:
		db.compactionExitTransact()
	}

	var (
		rec        = &sessionRecord{}
		stats      = &cStatStaging{}
		flushLevel int
	)

	// Generate tables.
	db.compactionTransactFunc("memdb@flush", func(cnt *compactionTransactCounter) (err error) {
		stats.startTimer()
		flushLevel, err = db.s.flushMemdb(rec, mdb.DB, db.memdbMaxLevel)
		stats.stopTimer()
		return
	}, func() error {
		for _, r := range rec.addedTables {
			db.logf("memdb@flush revert @%d", r.num)
			if err := db.s.stor.Remove(storage.FileDesc{Type: storage.TypeTable, Num: r.num}); err != nil {
				return err
			}
		}
		return nil
	})

	rec.setJournalNum(db.journalFd.Num)
	rec.setSeqNum(db.frozenSeq)

	// Commit.
	stats.startTimer()
	db.compactionCommit("memdb", rec)
	stats.stopTimer()

	db.logf("memdb@flush committed F·%d T·%v", len(rec.addedTables), stats.duration)

	// Save compaction stats
	for _, r := range rec.addedTables {
		stats.write += r.size
	}
	db.compStats.addStat(flushLevel, stats)
	atomic.AddUint32(&db.memComp, 1)

	// Drop frozen memdb.
	db.dropFrozenMem()

	// Resume table compaction.
	if resumeC != nil {
		select {
		case <-resumeC:
			close(resumeC)
		case <-db.closeC:
			db.compactionExitTransact()
		}
	}

	// Trigger table compaction.
	db.compTrigger(db.tcompCmdC)
}

type tableCompactionBuilder struct {
	db           *DB
	s            *session
	c            *compaction
	rec          *sessionRecord
	stat0, stat1 *cStatStaging

	snapHasLastUkey bool
	snapLastUkey    []byte
	snapLastSeq     uint64
	snapIter        int
	snapKerrCnt     int
	snapDropCnt     int

	kerrCnt int
	dropCnt int

	minSeq    uint64
	strict    bool
	tableSize int

	tw *tWriter
}

func (b *tableCompactionBuilder) appendKV(key, value []byte) error {
	// Create new table if not already.
	if b.tw == nil {
		// Check for pause event.
		if b.db != nil {
			select {
			case ch := <-b.db.tcompPauseC:
				b.db.pauseCompaction(ch)
			case <-b.db.closeC:
				b.db.compactionExitTransact()
			default:
			}
		}

		// Create new table.
		var err error
		b.tw, err = b.s.tops.create(b.tableSize)
		if err != nil {
			return err
		}
	}

	// Write key/value into table.
	return b.tw.append(key, value)
}

func (b *tableCompactionBuilder) needFlush() bool {
	return b.tw.tw.BytesLen() >= b.tableSize
}

func (b *tableCompactionBuilder) flush() error {
	t, err := b.tw.finish()
	if err != nil {
		return err
	}
	b.rec.addTableFile(b.c.targetLevel, t)
	b.stat1.write += t.size
	b.s.logf("table@build created L%d@%d N·%d S·%s %q:%q", b.c.targetLevel, t.fd.Num, b.tw.tw.EntriesLen(), shortenb(int(t.size)), t.imin, t.imax)
	b.tw = nil
	return nil
}

func (b *tableCompactionBuilder) cleanup() {
	if b.tw != nil {
		b.tw.drop()
		b.tw = nil
	}
}

func (b *tableCompactionBuilder) run(cnt *compactionTransactCounter) error {
	snapResumed := b.snapIter > 0
	hasLastUkey := b.snapHasLastUkey // The key might has zero length, so this is necessary.
	lastUkey := append([]byte{}, b.snapLastUkey...)
	lastSeq := b.snapLastSeq
	b.kerrCnt = b.snapKerrCnt
	b.dropCnt = b.snapDropCnt
	// Restore compaction state.
	b.c.restore()

	defer b.cleanup()

	b.stat1.startTimer()
	defer b.stat1.stopTimer()

	iter := b.c.newIterator()
	defer iter.Release()
	for i := 0; iter.Next(); i++ {
		// Incr transact counter.
		cnt.incr()

		// Skip until last state.
		if i < b.snapIter {
			continue
		}

		resumed := false
		if snapResumed {
			resumed = true
			snapResumed = false
		}

		ikey := iter.Key()
		ukey, seq, kt, kerr := parseInternalKey(ikey)

		if kerr == nil {
			shouldStop := !resumed && b.c.shouldStopBefore(ikey)

			if !hasLastUkey || b.s.icmp.uCompare(lastUkey, ukey) != 0 {
				// First occurrence of this user key.

				// Only rotate tables if ukey doesn't hop across.
				if b.tw != nil && (shouldStop || b.needFlush()) {
					if err := b.flush(); err != nil {
						return err
					}

					// Creates snapshot of the state.
					b.c.save()
					b.snapHasLastUkey = hasLastUkey
					b.snapLastUkey = append(b.snapLastUkey[:0], lastUkey...)
					b.snapLastSeq = lastSeq
					b.snapIter = i
					b.snapKerrCnt = b.kerrCnt
					b.snapDropCnt = b.dropCnt
				}

				hasLastUkey = true
				lastUkey = append(lastUkey[:0], ukey...)
				lastSeq = keyMaxSeq
			}

			switch {
			case lastSeq <= b.minSeq:
				// Dropped because newer entry for same user key exist
				fallthrough // (A)
			case kt == keyTypeDel && seq <= b.minSeq && b.c.baseLevelForKey(lastUkey):
				// For this user key:
				// (1) there is no data in higher levels
				// (2) data in lower levels will have larger seq numbers
				// (3) data in layers that are being compacted here and have
				//     smaller seq numbers will be dropped in the next
				//     few iterations of this loop (by rule (A) above).
				// Therefore this deletion marker is obsolete and can be dropped.
				lastSeq = seq
				b.dropCnt++
				continue
			default:
				lastSeq = seq
			}
		} else {
			if b.strict {
				return kerr
			}

			// Don't drop corrupted keys.
			hasLastUkey = false
			lastUkey = lastUkey[:0]
			lastSeq = keyMaxSeq
			b.kerrCnt++
		}

		if err := b.appendKV(ikey, iter.Value()); err != nil {
			return err
		}
	}

	if err := iter.Error(); err != nil {
		return err
	}

	// Finish last table.
	if b.tw != nil && !b.tw.empty() {
		return b.flush()
	}
	return nil
}

func (b *tableCompactionBuilder) revert() error {
	for _, at := range b.rec.addedTables {
		b.s.logf("table@build revert @%d", at.num)
		if err := b.s.stor.Remove(storage.FileDesc{Type: storage.TypeTable, Num: at.num}); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) tableCompaction(c *compaction, noTrivial bool) {
	defer c.release()

	rec := &sessionRecord{}
	rec.addCompPtr(c.sourceLevel, c.imax)

	if !noTrivial && c.trivial() {
		t := c.levels[0][0]
		db.logf("table@move L%d@%d -> L%d", c.sourceLevel, t.fd.Num, c.targetLevel)
		rec.delTable(c.sourceLevel, t.fd.Num)
		rec.addTableFile(c.targetLevel, t)
		db.compactionCommit("table-move", rec)
		return
	}

	var stats [2]cStatStaging
	for i, tables := range c.levels {
		for _, t := range tables {
			c.stats[i].read += t.size
			// Insert deleted tables into record
			// rec.delTable(c.sourceLevel+i, t.fd.Num)
			if i == 0 {
				rec.delTable(c.sourceLevel, t.fd.Num)
			} else {
				rec.delTable(c.targetLevel, t.fd.Num)
			}
		}
	}
	sourceSize := int(stats[0].read + stats[1].read)
	minSeq := db.minSeq()
	db.logf("table@compaction L%d·%d -> L%d·%d S·%s Q·%d", c.sourceLevel, len(c.levels[0]), c.targetLevel, len(c.levels[1]), shortenb(sourceSize), minSeq)

	b := &tableCompactionBuilder{
		db:  db,
		s:   db.s,
		c:   c,
		rec: rec,
		// stat1:  &stats[1],
		stat1:  &c.stats[1],
		minSeq: minSeq,
		strict: db.s.o.GetStrict(opt.StrictCompaction),
		// tableSize: db.s.o.GetCompactionTableSize(c.sourceLevel + 1),
		tableSize: db.s.o.GetCompactionTableSize(c.targetLevel),
	}
	db.compactionTransact("table@build", b)

	c.stats[1].startTimer()
	db.compactionCommit("table", rec)
	c.stats[1].stopTimer()

	resultSize := int(stats[1].write)
	db.logf("table@compaction committed F%s S%s Ke·%d D·%d T·%v", sint(len(rec.addedTables)-len(rec.deletedTables)), sshortenb(resultSize-sourceSize), b.kerrCnt, b.dropCnt, stats[1].duration)

	// Save compaction stats
	for i := range stats {
		// db.compStats.addStat(c.sourceLevel+1, &stats[i])
		db.compStats.addStat(c.targetLevel, &stats[i])
	}
	switch c.typ {
	case level0Compaction:
		atomic.AddUint32(&db.level0Comp, 1)
	case nonLevel0Compaction:
		atomic.AddUint32(&db.nonLevel0Comp, 1)
	case seekCompaction:
		atomic.AddUint32(&db.seekComp, 1)
	}

}

func (db *DB) ptableCompaction(c *compaction, noTrivial bool) {
	rec := &sessionRecord{}
	rec.addCompPtr(c.sourceLevel, c.imax)

	if !noTrivial && c.trivial() {
		t := c.levels[0][0]
		db.logf("table@move L%d@%d -> L%d", c.sourceLevel, t.fd.Num, c.sourceLevel+1)
		rec.delTable(c.sourceLevel, t.fd.Num)
		rec.addTableFile(c.sourceLevel+1, t)
		defer db.pCompCloseW.Done()
		c.compBuilder = &tableCompactionBuilder{
			db:    db,
			s:     db.s,
			c:     c,
			rec:   rec,
			stat1: &c.stats[1],

			strict:    db.s.o.GetStrict(opt.StrictCompaction),
			tableSize: db.s.o.GetCompactionTableSize(c.sourceLevel + 1),
		}
		//db.compactionCommit("table-move", rec)
		return
	}

	defer db.pCompCloseW.Done()

	for i, tables := range c.levels {
		for _, t := range tables {
			c.stats[i].read += t.size
			// Insert deleted tables into record
			// KIR
			if i == 0 {
				rec.delTable(c.sourceLevel, t.fd.Num)
			} else {
				rec.delTable(c.targetLevel, t.fd.Num)
			}
		}
	}
	sourceSize := int(c.stats[0].read + c.stats[1].read)

	minSeq := db.minSeq()

	db.logf("table@compaction L%d·%d -> L%d·%d S·%s Q·%d", c.sourceLevel, len(c.levels[0]), c.targetLevel, len(c.levels[1]), shortenb(sourceSize), minSeq)

	c.compBuilder = &tableCompactionBuilder{
		db:    db,
		s:     db.s,
		c:     c,
		rec:   rec,
		stat1: &c.stats[1],

		minSeq:    minSeq,
		strict:    db.s.o.GetStrict(opt.StrictCompaction),
		tableSize: db.s.o.GetCompactionTableSize(c.sourceLevel + 1),
	}
	db.compactionTransact("p-table-build", c.compBuilder)
}

func (db *DB) tableRangeCompaction(level int, umin, umax []byte) error {
	db.logf("table@compaction range L%d %q:%q", level, umin, umax)
	if level >= 0 {
		if c := db.s.getCompactionRange(level, umin, umax, true); c != nil {
			db.tableCompaction(c, true)
		}
	} else {
		// Retry until nothing to compact.
		for {
			compacted := false

			// Scan for maximum level with overlapped tables.
			v := db.s.version()
			m := 1
			for i := m; i < len(v.levels); i++ {
				tables := v.levels[i]
				if tables.overlaps(db.s.icmp, umin, umax, false) {
					m = i
				}
			}
			v.release()

			for level := 0; level < m; level++ {
				if c := db.s.getCompactionRange(level, umin, umax, false); c != nil {
					db.tableCompaction(c, true)
					compacted = true
				}
			}

			if !compacted {
				break
			}
		}
	}

	return nil
}

// func (db *DB) tableAutoCompaction() { // orig
// 	if c := db.s.pickCompaction(); c != nil {
// 		db.tableCompaction(c, false)
// 	}
// }

func (db *DB) tableAutoCompaction() (err error) {
	if cs := db.s.pickCompaction(); cs != nil {

		// drop compaction with lesser cScore
		// which has overlapping sstable as a compaction range with higher one
		cs = db.dropIfOverlaps(cs)

		for _, c := range cs {
			if c.sourceLevel == 0 {
				atomic.AddUint32(&db.compCntLvl0, 1)
			} else if c.sourceLevel == 1 {
				atomic.AddUint32(&db.compCntLvl1, 1)
			} else if c.sourceLevel == 2 {
				atomic.AddUint32(&db.compCntLvl2, 1)
			} else if c.sourceLevel == 3 {
				atomic.AddUint32(&db.compCntLvl3, 1)
			}
		}

		if len(cs) == 1 {
			start := time.Now()

			atomic.StoreInt32(&db.CountGetDuringCompaction, 1)

			db.tableCompaction(cs[0], false)

			defer func() {
				elapsed := time.Since(start).Microseconds()
				db.cTotalElapsed += elapsed

				atomic.StoreInt32(&db.CountGetDuringCompaction, 0)
				atomic.StoreInt64(&db.getCountDuringCompaction, 0)
				atomic.StoreInt64(&db.totalElapsedDuringCompaction, 0)

			}()
		} else {
			start := time.Now()

			atomic.StoreInt32(&db.CountGetDuringCompaction, 1)

			db.pCompCloseW.Add(len(cs))
			for _, c := range cs {
				// do compaction without trivial
				go db.ptableCompaction(c, false)
			}
			db.pCompCloseW.Wait()

			// merge sessionRecords for commit
			var recs = make([]*sessionRecord, len(cs))
			var lmadded int
			var lmdeleted int
			//var stats = make([]*[2]cStatStaging, len(cs))

			for i, c := range cs {
				rec := c.compBuilder.rec
				recs[i] = rec
				lmadded += len(recs[i].addedTables)
				lmdeleted += len(recs[i].deletedTables)
				//stats = append(stats, &c.stats)
			}

			var madded = make([]atRecord, lmadded)
			var mdeleted = make([]dtRecord, lmdeleted)

			var stagedAddedTablesLen int
			var stagedDeletedTablesLen int

			for _, rec := range recs {
				for j, t := range rec.addedTables {
					madded[j+stagedAddedTablesLen] = t
				}
				stagedAddedTablesLen += len(rec.addedTables)
				for j, t := range rec.deletedTables {
					mdeleted[j+stagedDeletedTablesLen] = t
				}
				stagedDeletedTablesLen += len(rec.deletedTables)
			}

			mrec := &sessionRecord{
				addedTables:   madded,
				deletedTables: mdeleted,
			}
			/*
				nv := db.s.version().spawn(mrec, true)

				// abandon useless version id to prevent blocking version processing loop.
				defer func() {
					if err != nil {
						db.s.abandon <- nv.id
						db.s.logf("commit@abandon useless vid D%d", nv.id)
					}
				}()

				if db.s.manifest == nil {
					// manifest journal writer not yet created, create one
					err = db.s.newManifest(mrec, nv)
				} else {
					err = db.s.flushManifest(mrec)
				}

				if err == nil {

				}
			*/
			mc := &mergedCompaction{
				s: db.s,
				//v:    db.s.version(),
				cs:   cs,
				stat: cStatStaging{},
			}

			defer func() {
				mc.cs[0].release()
				for _, c := range cs {
					if c == cs[0] {
						continue
					}
					c.released = true
				}

				elapsed := time.Since(start).Microseconds()
				db.cTotalElapsed += elapsed

				atomic.StoreInt32(&db.CountGetDuringCompaction, 0)
				atomic.StoreInt64(&db.getCountDuringCompaction, 0)
				atomic.StoreInt64(&db.totalElapsedDuringCompaction, 0)

			}()

			for _, c := range mc.cs {
				for _, stat := range c.stats {
					mc.stat.read += stat.read
				}
				mc.stat.write += c.stats[1].write
				mc.kerrCnt += c.compBuilder.kerrCnt
				mc.dropCnt += c.compBuilder.dropCnt
			}

			mc.stat.startTimer()
			db.compactionCommit("merged-table", mrec)
			mc.stat.stopTimer()

			// Save compaction stats
			db.compStats.addmergedCompStat(&mc.stat)

			for _, c := range mc.cs {
				switch c.typ {
				case level0Compaction:
					atomic.AddUint32(&db.level0Comp, 1)
				case nonLevel0Compaction:
					atomic.AddUint32(&db.nonLevel0Comp, 1)
				case seekCompaction:
					atomic.AddUint32(&db.seekComp, 1)
				}
			}
		}
	}
	return
}

// KIR
func (db *DB) dropIfOverlaps(cs []*compaction) []*compaction {

	if len(cs) == 1 {
		return cs
	}

	// 1. sort cs(compactions) ascending, sourcelevel upward(e.g. 0 -> 4)
	sort.Slice(cs, func(i, j int) bool {
		return cs[i].sourceLevel < cs[j].sourceLevel
	})

	// 2. get pairs of compactions having contiguous sourceLevel
	var cpairs [][2]*compaction
	for i := 1; i < len(cs); i++ {
		if cs[i-1].sourceLevel == cs[i].sourceLevel-1 {
			cpairs = append(cpairs, [2]*compaction{cs[i-1], cs[i]})
		}
	}

	// 3. check whether there are duplicated sst table
	// within overlapped level of two compaction having contiguous sourceLevel
	for _, cpair := range cpairs {
		var duplicates []int64
		m := make(map[int64]bool)

		for _, tFile := range cpair[0].levels[1] {
			m[tFile.fd.Num] = true
		}

		for _, tFile := range cpair[1].levels[0] {
			if m[tFile.fd.Num] {
				duplicates = append(duplicates, tFile.fd.Num)
			}
		}

		// 4. if duplicated sstable exists, drop the one which has lesser cScore
		if len(duplicates) != 0 {
			if cpair[0].cScore >= cpair[1].cScore {
				for i, c := range cs {
					if c == cpair[1] {
						cs = append(cs[:i], cs[i+1:]...)
					}
				}
			} else {
				for i, c := range cs {
					if c == cpair[0] {
						cs = append(cs[:i], cs[i+1:]...)
					}
				}
			}
		}
	}
	return cs
}

func (db *DB) tableNeedCompaction() bool {
	v := db.s.version()
	defer v.release()
	return v.needCompaction()
}

// resumeWrite returns an indicator whether we should resume write operation if enough level0 files are compacted.
func (db *DB) resumeWrite() bool {
	v := db.s.version()
	defer v.release()
	if v.tLen(0) < db.s.o.GetWriteL0PauseTrigger() {
		return true
	}
	return false
}

func (db *DB) pauseCompaction(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	case <-db.closeC:
		db.compactionExitTransact()
	}
}

type cCmd interface {
	ack(err error)
}

type cAuto struct {
	// Note for table compaction, an non-empty ackC represents it's a compaction waiting command.
	ackC chan<- error
}

func (r cAuto) ack(err error) {
	if r.ackC != nil {
		defer func() {
			recover()
		}()
		r.ackC <- err
	}
}

type cRange struct {
	level    int
	min, max []byte
	ackC     chan<- error
}

func (r cRange) ack(err error) {
	if r.ackC != nil {
		defer func() {
			recover()
		}()
		r.ackC <- err
	}
}

// This will trigger auto compaction but will not wait for it.
func (db *DB) compTrigger(compC chan<- cCmd) {
	select {
	case compC <- cAuto{}:
	default:
	}
}

// This will trigger auto compaction and/or wait for all compaction to be done.
func (db *DB) compTriggerWait(compC chan<- cCmd) (err error) {
	ch := make(chan error)
	defer close(ch)
	// Send cmd.
	select {
	case compC <- cAuto{ch}:
	case err = <-db.compErrC:
		return
	case <-db.closeC:
		return ErrClosed
	}
	// Wait cmd.
	select {
	case err = <-ch:
	case err = <-db.compErrC:
	case <-db.closeC:
		return ErrClosed
	}
	return err
}

// Send range compaction request.
func (db *DB) compTriggerRange(compC chan<- cCmd, level int, min, max []byte) (err error) {
	ch := make(chan error)
	defer close(ch)
	// Send cmd.
	select {
	case compC <- cRange{level, min, max, ch}:
	case err := <-db.compErrC:
		return err
	case <-db.closeC:
		return ErrClosed
	}
	// Wait cmd.
	select {
	case err = <-ch:
	case err = <-db.compErrC:
	case <-db.closeC:
		return ErrClosed
	}
	return err
}

func (db *DB) mCompaction() {
	var x cCmd

	defer func() {
		if x := recover(); x != nil {
			if x != errCompactionTransactExiting {
				panic(x)
			}
		}
		if x != nil {
			x.ack(ErrClosed)
		}
		db.closeW.Done()
	}()

	for {
		select {
		case x = <-db.mcompCmdC:
			switch x.(type) {
			case cAuto:
				db.memCompaction()
				x.ack(nil)
				x = nil
			default:
				panic("leveldb: unknown command")
			}
		case <-db.closeC:
			return
		}
	}
}

func (db *DB) tCompaction() {
	var (
		x     cCmd
		waitQ []cCmd
	)

	defer func() {
		if x := recover(); x != nil {
			if x != errCompactionTransactExiting {
				panic(x)
			}
		}
		for i := range waitQ {
			waitQ[i].ack(ErrClosed)
			waitQ[i] = nil
		}
		if x != nil {
			x.ack(ErrClosed)
		}
		db.closeW.Done()
	}()

	for {
		if db.tableNeedCompaction() {
			select {
			case x = <-db.tcompCmdC:
			case ch := <-db.tcompPauseC:
				db.pauseCompaction(ch)
				continue
			case <-db.closeC:
				return
			default:
			}
			// Resume write operation as soon as possible.
			if len(waitQ) > 0 && db.resumeWrite() {
				for i := range waitQ {
					waitQ[i].ack(nil)
					waitQ[i] = nil
				}
				waitQ = waitQ[:0]
			}
		} else {
			for i := range waitQ {
				waitQ[i].ack(nil)
				waitQ[i] = nil
			}
			waitQ = waitQ[:0]
			select {
			case x = <-db.tcompCmdC:
			case ch := <-db.tcompPauseC:
				db.pauseCompaction(ch)
				continue
			case <-db.closeC:
				return
			}
		}
		if x != nil {
			switch cmd := x.(type) {
			case cAuto:
				if cmd.ackC != nil {
					// Check the write pause state before caching it.
					if db.resumeWrite() {
						x.ack(nil)
					} else {
						waitQ = append(waitQ, x)
					}
				}
			case cRange:
				x.ack(db.tableRangeCompaction(cmd.level, cmd.min, cmd.max))
			default:
				panic("leveldb: unknown command")
			}
			x = nil
		}
		db.tableAutoCompaction()
	}
}

// KIR
func (db *DB) monitoring() {
	for {
		select {
		case <-db.ticker.C:
			db.epochCnt++
			getCnt := atomic.LoadInt64(&db.getCnt)
			atomic.StoreInt64(&db.getCnt, 0)

			if getCnt != 0 {
				// totalElapsedPerEpoch := atomic.LoadInt64(&db.totalElapsedPerEpoch)
				// atomic.StoreInt64(&db.totalElapsedPerEpoch, 0)

				// avgGetLat := totalElapsedPerEpoch / getCnt

				// compCntLvl0 := atomic.LoadUint32(&db.compCntLvl0)
				// atomic.StoreUint32(&db.compCntLvl0, 0)
				// compCntLvl1 := atomic.LoadUint32(&db.compCntLvl1)
				// atomic.StoreUint32(&db.compCntLvl1, 0)
				// compCntLvl2 := atomic.LoadUint32(&db.compCntLvl2)
				// atomic.StoreUint32(&db.compCntLvl2, 0)
				// compCntLvl3 := atomic.LoadUint32(&db.compCntLvl3)
				// atomic.StoreUint32(&db.compCntLvl3, 0)
				// compCntLvl4 := atomic.LoadUint32(&db.compCntLvl4)
				// atomic.StoreUint32(&db.compCntLvl4, 0)

				// fmt.Printf("monitoring: epochCnt %d getCnt %d avgGetLat %d compLvl0 %d compLvl1 %d compLvl2 %d compLvl3 %d compLvl4 %d \n",
				// 		db.epochCnt, getCnt, avgGetLat /*alpha, beta,*/, compCntLvl0, compCntLvl1, compCntLvl2, compCntLvl3, compCntLvl4)
			}
		case <-db.closeC:
			return
		}
	}
}

func (db *DB) delayCompactionIfNeeded() {
	for {
		select {
		case <-db.tickerForCompactionDelay.C:
			getCnt := atomic.LoadInt64(&db.getCountForCompactionDelay)
			if getCnt >= 500 { // target for change
				db.s.o.Options.SetBeta(5)
				backoffTimer := *time.NewTicker(time.Second * 10)
				select {
				case <-backoffTimer.C:
					db.s.o.Options.SetBeta(1)
				case <-db.closeC:
					return
				}
				backoffTimer.Stop()
			}
			atomic.StoreInt64(&db.getCountForCompactionDelay, 0)
		case <-db.closeC:
			return
		}
	}
}

// parallel
func (db *DB) disableParallelCompactionIfNeeded() {
	for {
		select {
		case <-db.tickerForParallelCompactionDisable.C:
			getCnt := atomic.LoadInt64(&db.getCountForParallelCompactionDisable)
			if getCnt <= 200 { // target for change
				db.s.o.Options.EnableParallelComp()
				backoffTimer := *time.NewTicker(time.Second * 10)
				select {
				case <-backoffTimer.C:
					db.s.o.Options.DisableParallelComp()
				case <-db.closeC:
					return
				}
				backoffTimer.Stop()
			}
			atomic.StoreInt64(&db.getCountForParallelCompactionDisable, 0)
		case <-db.closeC:
			return
		}
	}
}

// RO
func (db *DB) disableROIfNeeded() {
	for {
		select {
		case <-db.tickerForRODisable.C:
			compCnt0 := atomic.LoadUint32(&db.compCntLvl0)
			compCnt1 := atomic.LoadUint32(&db.compCntLvl1)
			compCnt2 := atomic.LoadUint32(&db.compCntLvl2)
			compCnt3 := atomic.LoadUint32(&db.compCntLvl3)
			compCnt := compCnt0 + compCnt1 + compCnt2 + compCnt3
			if compCnt <= 2 && compCnt > 0 {
				db.s.o.Options.EnableRO()
				backoffTimer := *time.NewTicker(time.Second * 10)
				select {
				case <-backoffTimer.C:
					db.s.o.Options.DisableRO()
				case <-db.closeC:
					return
				}
				backoffTimer.Stop()
			}
		case <-db.closeC:
			return
		}
	}
}
