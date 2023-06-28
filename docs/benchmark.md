# Benchmark

## Random Read

### Indicator (mmap O(1))
```                               
mmap(): Loaded 100000 items in 75ms
mmap(): Random read 100000 items in 25ms
mmap(): Random read 100000 items in 15ms
mmap(): Random read 100000 items in 15ms
```

## MileStones

### Timeline (newest to oldest)

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

* **[06-23-2023 Binary](https://github.com/weids-dev/radarbase/commit/909ca5743f37e8e1b3e7c51affd9c9d01673d85f) Tree Index**
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

* **[06-22-2023](https://github.com/weids-dev/radarbase/commit/6717f275bb5cad2443cf67d4f3be76f77633945b) In-memory Toy Version
```
radb: Loaded 100000 items in 49ms
radb: Random read 100000 items in 26ms
radb: Random read 100000 items in 23ms
radb: Random read 100000 items in 23ms
```
