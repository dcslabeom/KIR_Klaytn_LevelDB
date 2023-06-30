# KIR_Klaytn_LevelDB
The 13th KIR: Improving the Read Performance of KV database in Klaytn

In this project, we aim to solve the I/O performance degradation. 

We implement features such as reducing key range overlapping, vertical-wise multi-threaded compaction, and workload-aware Compaction Delaying.

This prototype is based on goLevelDB (https://github.com/syndtr/goleveldb).
