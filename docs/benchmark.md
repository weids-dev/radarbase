# Benchmark

## Random Read

### Indicator (mmap `O(1)`)
```                               
mmap(): Loaded 100000 items in 757ms
mmap(): Random read 100000 items in 43ms
mmap(): Random read 100000 items in 25ms
mmap(): Random read 100000 items in 25ms
```

### Milestones



**Timeline (newest to oldest)**


* **[19-08-2023]**

```
 Running benches/benchmark.rs (target/release/deps/benchmark-35091a204f461d5
5)
lmdb-rkv: Bulk loaded 100000 items in 230ms
lmdb-rkv: Wrote 100 individual items in 9ms
lmdb-rkv: Random read 100000 items in 75ms
lmdb-rkv: Random read 100000 items in 66ms
lmdb-rkv: Random read 100000 items in 65ms
radarbase: Bulk loaded 100000 items in 1067ms
radarbase: Wrote 100 individual items in 5659ms
radarbase: Random read 100000 items in 116ms
radarbase: Random read 100000 items in 133ms
radarbase: Random read 100000 items in 117ms
sled: Bulk loaded 100000 items in 2001ms
sled: Wrote 100 individual items in 469ms
sled: Random read 100000 items in 95ms
sled: Random read 100000 items in 74ms
sled: Random read 100000 items in 76ms

Running benches/common.rs (target/release/deps/common-e2fdd093503f2a8a)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finis
hed in 0.00s

Running benches/syscall_benchmark.rs (target/release/deps/syscall_benchmark-
dbe9bc3126a40778)

lmdb-zero: Loaded 100000 items in 245ms
lmdb-zero: Random read 100000 items in 81ms
lmdb-zero: Random read 100000 items in 74ms
lmdb-zero: Random read 100000 items in 74ms
read()/write(): Loaded 100000 items in 450ms
read()/write(): Random read 100000 items in 130ms
read()/write(): Random read 100000 items in 130ms
read()/write(): Random read 100000 items in 143ms
mmap(): Loaded 100000 items in 82ms
mmap(): Random read 100000 items in 25ms
mmap(): Random read 100000 items in 15ms
mmap(): Random read 100000 items in 15ms
```
* **[07-04-2023](https://github.com/weids-dev/radarbase/commit/d45112bf681cfdd0d4ba662ff2e8a6f9e44409ca) Implement Bulk Insert Version**
```
     Running benches/benchmark.rs (target/release/deps/benchmark-0322d0ccc42aabb8)
lmdb-rkv: Bulk loaded 100000 items in 2263ms
lmdb-rkv: Wrote 100 individual items in 1348ms
lmdb-rkv: Random read 100000 items in 138ms
lmdb-rkv: Random read 100000 items in 92ms
lmdb-rkv: Random read 100000 items in 92ms
radarbase: Bulk loaded 100000 items in 1643ms
radarbase: Wrote 100 individual items in 2814ms
radarbase: Random read 100000 items in 211ms
radarbase: Random read 100000 items in 207ms
radarbase: Random read 100000 items in 209ms
sled: Bulk loaded 100000 items in 2393ms
sled: Wrote 100 individual items in 243ms
sled: Random read 100000 items in 267ms
sled: Random read 100000 items in 206ms
sled: Random read 100000 items in 206ms
     Running benches/common.rs (target/release/deps/common-ba49b4c18cf58a44)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running benches/syscall_benchmark.rs (target/release/deps/syscall_benchmark-114836c52d12a9a6)
lmdb-zero: Loaded 100000 items in 1848ms
lmdb-zero: Random read 100000 items in 116ms
lmdb-zero: Random read 100000 items in 92ms
lmdb-zero: Random read 100000 items in 94ms
read()/write(): Loaded 100000 items in 868ms
read()/write(): Random read 100000 items in 357ms
read()/write(): Random read 100000 items in 339ms
read()/write(): Random read 100000 items in 337ms
mmap(): Loaded 100000 items in 512ms
mmap(): Random read 100000 items in 40ms
mmap(): Random read 100000 items in 24ms
mmap(): Random read 100000 items in 25ms
```

* **[06-29-2023](https://github.com/weids-dev/radarbase/commit/78ec31c6bb1b26e612c3254d524cf214139ae232) Exclusively Using Btree**
```
     Running benches/benchmark.rs (target/release/deps/benchmark-1bb2932149779cda)
lmdb-rkv: Loaded 100000 items in 1798ms
lmdb-rkv: Random read 100000 items in 135ms
lmdb-rkv: Random read 100000 items in 94ms
lmdb-rkv: Random read 100000 items in 93ms
radarbase: Loaded 100000 items in 31151ms
radarbase: Random read 100000 items in 1034ms
radarbase: Random read 100000 items in 296ms
radarbase: Random read 100000 items in 300ms
sled: Loaded 100000 items in 2827ms
sled: Random read 100000 items in 244ms
sled: Random read 100000 items in 208ms
sled: Random read 100000 items in 209ms
     Running benches/common.rs (target/release/deps/common-b5810a9343a19dcd)

running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running benches/syscall_benchmark.rs (target/release/deps/syscall_benchmark-5bfe02ef08ac8aa5)
lmdb-zero: Loaded 100000 items in 2425ms
lmdb-zero: Random read 100000 items in 119ms
lmdb-zero: Random read 100000 items in 95ms
lmdb-zero: Random read 100000 items in 96ms
read()/write(): Loaded 100000 items in 1303ms
read()/write(): Random read 100000 items in 339ms
read()/write(): Random read 100000 items in 338ms
read()/write(): Random read 100000 items in 337ms
mmap(): Loaded 100000 items in 953ms
mmap(): Random read 100000 items in 26ms
mmap(): Random read 100000 items in 26ms
mmap(): Random read 100000 items in 26ms
```

****

### Below are the benches on my macbook, I've now run all the benches inside my Arch Linux Server, so all the benches above is on that server.

****

* **[06-28-2023](https://github.com/weids-dev/radarbase/commit/fa52bd2629503123fd3634e62bbc98dd239de250) Binary Tree with node-page Implementation**
```
radarbase: Loaded 100000 items in 1503ms
radarbase: Random read 100000 items in 224ms
radarbase: Random read 100000 items in 127ms
radarbase: Random read 100000 items in 130ms
```

* **[06-25-2023](https://github.com/weids-dev/radarbase/commit/df81f1e14c117ecd801cc16668ef7c8883f183b7) Binary Tree Index** 
```
radarbase: Loaded 100000 items in 409ms
radarbase: Random read 100000 items in 51ms
radarbase: Random read 100000 items in 41ms
radarbase: Random read 100000 items in 42ms
```

* **[06-23-2023](https://github.com/weids-dev/radarbase/commit/909ca5743f37e8e1b3e7c51affd9c9d01673d85f) Binary Tree Index**
```
radb: Loaded 100000 items in 450ms
radb: Random read 100000 items in 53ms
radb: Random read 100000 items in 43ms
radb: Random read 100000 items in 43ms
```

* **[06-22-2023](https://github.com/weids-dev/radarbase/commit/f588965274909ee273edcad1f0112988efbda46b) Toy Version (Linear Scan)**
```
radb: Loaded 100000 items in 95584ms
radb: Random read 100000 items in 88584ms
radb: Random read 100000 items in 88452ms
radb: Random read 100000 items in 89260ms
```

* **[06-22-2023](https://github.com/weids-dev/radarbase/commit/6717f275bb5cad2443cf67d4f3be76f77633945b) In-memory Toy Version**
```
radb: Loaded 100000 items in 49ms
radb: Random read 100000 items in 26ms
radb: Random read 100000 items in 23ms
radb: Random read 100000 items in 23ms
```
