# KIR_Klaytn_LevelDB
The 13th KIR: Improving the Read Performance of KV database in Klaytn

# 1. Description

This project is for improving read performance of KV (key-value) database in Klaytn. When synchronizing the global chain of blocks in the Klaytn network up-to-date, the Klaytn nodes need to perform I/O operations in KV database during copying and validating the blocks. During validation, especially the read operations in KV database are executed intensively. In order to reduce the bottleneck and performance degradation, we propose several features in the KV database in Klaytn, LevelDB.

# 2. Features and modified locations

## /leveldb/db.go

Additional fields are appended in DB structure according to our needs for multi-level concurrent compaction(MLCC) and compaction delay implementation.

Under openDB() method, three additional go routines are created other than two original go routines performing table compaction and memory compaction. The role of each newly created go routine is as follows:

monitoring() method records logs of status information regarding GET operation count triggered during each epoch (10 secs), the average latency of GET operations, and the compaction invoked count during an epoch.

delayCompactionIfNeeded() method delays compaction operation to be triggered for a given time(we call this “betaDuration”) by adjusting the “Beta” parameter if the count of GET operation triggered in a second is more than a given GET count threshold(GCT). Note that “Beta“ parameter is used for degrading cScore, which results in delayed compaction

disableParallelCompactionIfNeeded() method disables multi-level concurrent compaction(MLCC) feature if the count of GET operation called in a second exceeds a given GCT. 

disableROIfNeeded() method disables reducing overlapping (RO) across SST files in multiple levels during compaction if the compaction overhead is going to be increased. Disabling RO is triggered based on the count of compaction in a second.

```go
    // from line 196
    go db.monitoring()
    go db.delayCompactionIfNeeded()
    go db.disableParallelCompactionIfNeeded()
    go db.disableROIfNeeded()
```

## /leveldb/session_compaction.go

Additional fields are created in compaction structure in order to enable MLCC and RO.

pickCompaction() method is modified for MLCC. It can now create multiple compaction objects if there are numerous target levels(one compaction object per one level), and “doParallelCompaction” option is turned on. 

expand_RO() method is modified for reducing overlapping (RO). It skips consecutive level for locating target SST file during compaction if there is no overlapping SST files and trivial compaction is going to happen. Then it locates target SST files in next level and perform compaction with level larger than 1. 

## /leveldb/db_compaction.go

tableAutoCompaction() method is revised for MLCC. In case MLCC is enabled, it checks if compaction objects contain overlapping SST files as the compaction target SST files. If files overlap, it discards the compaction object which has a smaller cScore. 

## /leveldb/version.go

computeCompaction() method is revised for the application of the compaction delay mechanism, specifically, the portion where cScore is calculated has been modified. Beta parameter is multiplied to “CompactionTotalSize”, the denominator part of cScore formula. Note that lowering cScore results in delayed compaction.
