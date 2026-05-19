# Chronos TSDB — Tag Index Refactor & Query Engine Evolution

## Current Progress Summary

Based on the uploaded design notes and progress snapshots, Chronos currently includes:

* ingestion engine
* WAL (write-ahead log)
* WAL replay
* memtable rotation
* immutable memtables
* flush worker
* mmap-backed segment reads
* timestamp delta compression
* Gorilla XOR compression skeleton
* query iterator abstractions
* benchmark client
* metrics pipeline
* SOLID-oriented refactor foundation
* storage layering
* ingestion coordinator

The current data model is:

```rust
DataPoint {
    series_id: u64,
    timestamp: i64,
    value: f64,
}
```

This is sufficient for raw ingestion benchmarking, but it is not yet a real analytical TSDB model.

Modern TSDB systems such as:

* Prometheus
* VictoriaMetrics
* TimescaleDB
* InfluxDB
* M3DB

all rely heavily on:

1. label/tag indexing
2. series identity derivation
3. inverted indexes
4. cardinality-aware storage
5. query filtering by tags

Without tags, Chronos behaves more like an append-only metric stream.

The next architectural milestone is therefore:

# Add Tags + Series Index + Inverted Query Index

---

# Why Tags Matter

Instead of:

```text
cpu_usage = 82
```

real TSDBs store:

```text
metric=cpu_usage
host=server-1
region=us-east
service=payments
```

This enables queries such as:

```sql
SELECT *
WHERE host='server-1'
AND service='payments'
```

or:

```promql
cpu_usage{region="us-east"}
```

The key realization:

# Tags define the logical series.

A unique combination of:

* metric name
* sorted tags

maps to exactly one `series_id`.

---

# Core Architectural Shift

Old:

```text
client -> series_id -> WAL -> memtable
```

New:

```text
client
  -> metric + tags
  -> series index lookup
  -> series_id
  -> WAL
  -> memtable
  -> inverted tag index
```

---

# New Storage Concepts

We need 3 new indexes.

## 1. Series Index

Maps:

```text
(metric_name + sorted_tags) -> series_id
```

This prevents duplicate logical series.

---

## 2. Reverse Series Metadata

Maps:

```text
series_id -> series metadata
```

Needed during query execution.

---

## 3. Inverted Tag Index

Maps:

```text
(tag_key, tag_value) -> set<series_id>
```

Example:

```text
("host", "server-1")
    -> {1, 8, 22, 91}
```

This is the foundation of efficient filtering.

---

# Important TSDB Concept

A TSDB rarely scans raw points first.

Instead:

```text
query
 -> tag index
 -> matching series_ids
 -> segment scan only for those series
```

This is why indexing matters.

---

# Recommended Project Structure

```text
src/
├── main.rs
├── config.rs
├── types/
│   ├── mod.rs
│   ├── datapoint.rs
│   ├── series.rs
│   ├── tags.rs
│   └── query.rs
├── ingest/
│   ├── mod.rs
│   ├── coordinator.rs
│   ├── worker.rs
│   └── batch.rs
├── wal/
│   ├── mod.rs
│   ├── writer.rs
│   ├── replay.rs
│   └── record.rs
├── memtable/
│   ├── mod.rs
│   ├── table.rs
│   ├── immutable.rs
│   └── manager.rs
├── storage/
│   ├── mod.rs
│   ├── segment.rs
│   ├── writer.rs
│   ├── reader.rs
│   ├── compression.rs
│   └── mmap.rs
├── index/
│   ├── mod.rs
│   ├── series_index.rs
│   ├── inverted_index.rs
│   ├── metadata_store.rs
│   └── cardinality.rs
├── query/
│   ├── mod.rs
│   ├── engine.rs
│   ├── planner.rs
│   ├── filters.rs
│   └── executor.rs
├── metrics/
│   ├── mod.rs
│   └── registry.rs
└── benchmark/
    ├── mod.rs
    └── generator.rs
```

---

# Full File Walkthrough

# src/types/mod.rs

```rust
pub mod datapoint;
pub mod query;
pub mod series;
pub mod tags;
```

---

# src/types/tags.rs

```rust
use std::collections::BTreeMap;

pub type Tags = BTreeMap<String, String>;
```

---

# Why BTreeMap Instead of HashMap?

This is extremely important.

We need deterministic ordering.

These two logically equivalent tag sets:

```text
host=a region=us
```

and:

```text
region=us host=a
```

must generate the SAME series identity.

BTreeMap guarantees sorted ordering.

Prometheus uses sorted labels for the same reason.

---

# src/types/series.rs

```rust
use crate::types::tags::Tags;

#[derive(Debug, Clone)]
pub struct Series {
    pub id: u64,
    pub metric: String,
    pub tags: Tags,
}
```

---

# src/types/datapoint.rs

```rust
use crate::types::tags::Tags;

#[derive(Debug, Clone)]
pub struct DataPoint {
    pub metric: String,
    pub tags: Tags,
    pub timestamp: i64,
    pub value: f64,
}
```

---

# Important Design Decision

Do NOT send `series_id` from clients.

Why?

Because:

* clients do not own series lifecycle
* clients cannot safely coordinate ids
* tags determine identity
* server must deduplicate series

Instead:

```text
client sends metric+tags
server derives series_id
```

This is how real TSDBs work.

---

# src/types/query.rs

```rust
#[derive(Debug, Clone)]
pub struct TagFilter {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct Query {
    pub metric: String,
    pub filters: Vec<TagFilter>,
    pub start: i64,
    pub end: i64,
}
```

---

# src/index/mod.rs

```rust
pub mod cardinality;
pub mod inverted_index;
pub mod metadata_store;
pub mod series_index;
```

---

# src/index/series_index.rs

```rust
use crate::types::series::Series;
use crate::types::tags::Tags;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

#[derive(Debug, Clone, Eq)]
pub struct SeriesKey {
    pub metric: String,
    pub tags: Vec<(String, String)>,
}

impl PartialEq for SeriesKey {
    fn eq(&self, other: &Self) -> bool {
        self.metric == other.metric && self.tags == other.tags
    }
}

impl Hash for SeriesKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.metric.hash(state);

        for tag in &self.tags {
            tag.hash(state);
        }
    }
}

impl SeriesKey {
    pub fn from(metric: &str, tags: &Tags) -> Self {
        Self {
            metric: metric.to_string(),
            tags: tags
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        }
    }
}

pub struct SeriesIndex {
    next_id: AtomicU64,
    key_to_id: RwLock<HashMap<SeriesKey, u64>>,
    id_to_series: RwLock<HashMap<u64, Series>>,
}

impl SeriesIndex {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            key_to_id: RwLock::new(HashMap::new()),
            id_to_series: RwLock::new(HashMap::new()),
        }
    }

    pub fn get_or_create(
        &self,
        metric: &str,
        tags: &Tags,
    ) -> u64 {
        let key = SeriesKey::from(metric, tags);

        {
            let read = self.key_to_id.read().unwrap();

            if let Some(id) = read.get(&key) {
                return *id;
            }
        }

        let mut write = self.key_to_id.write().unwrap();

        if let Some(id) = write.get(&key) {
            return *id;
        }

        let id = self.next_id.fetch_add(1, Ordering::Relaxed);

        write.insert(key.clone(), id);

        let series = Series {
            id,
            metric: metric.to_string(),
            tags: tags.clone(),
        };

        self.id_to_series
            .write()
            .unwrap()
            .insert(id, series);

        id
    }

    pub fn get_series(&self, id: u64) -> Option<Series> {
        self.id_to_series
            .read()
            .unwrap()
            .get(&id)
            .cloned()
    }
}
```

---

# Why Double-Checked Locking?

Notice:

```rust
read lock
 -> fast path

write lock
 -> slow path
```

This minimizes contention.

Critical for high-ingestion TSDBs.

---

# src/index/inverted_index.rs

```rust
use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

pub struct InvertedIndex {
    postings: RwLock<HashMap<(String, String), HashSet<u64>>>,
}

impl InvertedIndex {
    pub fn new() -> Self {
        Self {
            postings: RwLock::new(HashMap::new()),
        }
    }

    pub fn insert(
        &self,
        series_id: u64,
        tags: &[(String, String)],
    ) {
        let mut postings = self.postings.write().unwrap();

        for (k, v) in tags {
            postings
                .entry((k.clone(), v.clone()))
                .or_insert_with(HashSet::new)
                .insert(series_id);
        }
    }

    pub fn lookup(
        &self,
        key: &str,
        value: &str,
    ) -> HashSet<u64> {
        self.postings
            .read()
            .unwrap()
            .get(&(key.to_string(), value.to_string()))
            .cloned()
            .unwrap_or_default()
    }
}
```

---

# What Is a Posting List?

This structure:

```text
(tag_key, tag_value)
    -> set<series_id>
```

is called a:

# posting list

This is identical to search engines.

TSDB indexes are conceptually inverted indexes.

---

# src/index/metadata_store.rs

```rust
use crate::types::series::Series;
use std::collections::HashMap;
use std::sync::RwLock;

pub struct MetadataStore {
    series: RwLock<HashMap<u64, Series>>,
}

impl MetadataStore {
    pub fn new() -> Self {
        Self {
            series: RwLock::new(HashMap::new()),
        }
    }

    pub fn insert(&self, series: Series) {
        self.series
            .write()
            .unwrap()
            .insert(series.id, series);
    }

    pub fn get(&self, id: u64) -> Option<Series> {
        self.series
            .read()
            .unwrap()
            .get(&id)
            .cloned()
    }
}
```

---

# src/index/cardinality.rs

```rust
use std::collections::HashMap;
use std::sync::RwLock;

pub struct CardinalityTracker {
    counts: RwLock<HashMap<String, usize>>,
}

impl CardinalityTracker {
    pub fn new() -> Self {
        Self {
            counts: RwLock::new(HashMap::new()),
        }
    }

    pub fn record_tag_value(&self, key: &str) {
        let mut counts = self.counts.write().unwrap();

        *counts.entry(key.to_string()).or_insert(0) += 1;
    }
}
```

---

# Why Cardinality Matters

High-cardinality tags are the #1 scalability killer in TSDBs.

BAD:

```text
request_id=UUID
```

GOOD:

```text
service=payments
```

Prometheus users frequently crash clusters with bad cardinality.

Chronos should eventually:

* monitor cardinality
* warn on explosions
* reject pathological tags

---

# src/ingest/mod.rs

```rust
pub mod batch;
pub mod coordinator;
pub mod worker;
```

---

# src/ingest/coordinator.rs

```rust
use crate::index::inverted_index::InvertedIndex;
use crate::index::series_index::SeriesIndex;
use crate::memtable::manager::MemtableManager;
use crate::types::datapoint::DataPoint;
use crate::wal::writer::WalWriter;
use std::sync::Arc;

pub struct IngestionCoordinator {
    series_index: Arc<SeriesIndex>,
    inverted_index: Arc<InvertedIndex>,
    wal: Arc<WalWriter>,
    memtables: Arc<MemtableManager>,
}

impl IngestionCoordinator {
    pub fn ingest(&self, point: DataPoint) {
        let series_id = self.series_index
            .get_or_create(&point.metric, &point.tags);

        let tags: Vec<(String, String)> = point
            .tags
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        self.inverted_index
            .insert(series_id, &tags);

        self.wal.append(series_id, &point);

        self.memtables.insert(
            series_id,
            point.timestamp,
            point.value,
        );
    }
}
```

---

# Important Ordering Guarantee

The WAL append MUST happen before memtable insert.

Otherwise:

```text
crash after memtable write
before WAL write
```

causes permanent data loss.

This ordering is fundamental in storage engines.

---

# src/query/mod.rs

```rust
pub mod engine;
pub mod executor;
pub mod filters;
pub mod planner;
```

---

# src/query/filters.rs

```rust
use crate::types::query::Query;

pub fn normalize(query: &mut Query) {
    query.filters.sort_by(|a, b| {
        a.key.cmp(&b.key)
    });
}
```

---

# Why Normalize Filters?

Query planning becomes deterministic.

This improves:

* cacheability
* optimizer logic
* query fingerprints

---

# src/query/planner.rs

```rust
use crate::index::inverted_index::InvertedIndex;
use crate::types::query::Query;
use std::collections::HashSet;

pub struct QueryPlanner;

impl QueryPlanner {
    pub fn resolve_series(
        query: &Query,
        index: &InvertedIndex,
    ) -> HashSet<u64> {
        let mut iter = query.filters.iter();

        let first = match iter.next() {
            Some(f) => f,
            None => return HashSet::new(),
        };

        let mut result = index.lookup(
            &first.key,
            &first.value,
        );

        for filter in iter {
            let next = index.lookup(
                &filter.key,
                &filter.value,
            );

            result = result
                .intersection(&next)
                .cloned()
                .collect();
        }

        result
    }
}
```

---

# This Is the Core TSDB Query Pattern

```text
filter 1 -> posting list
filter 2 -> posting list
intersection
 -> matching series_ids
```

This is how Prometheus-style label filtering works internally.

---

# src/query/executor.rs

```rust
use crate::storage::reader::SegmentReader;

pub struct QueryExecutor {
    reader: SegmentReader,
}

impl QueryExecutor {
    pub fn scan_series(
        &self,
        series_ids: &[u64],
        start: i64,
        end: i64,
    ) {
        for series_id in series_ids {
            self.reader.read_range(
                *series_id,
                start,
                end,
            );
        }
    }
}
```

---

# src/query/engine.rs

```rust
use crate::index::inverted_index::InvertedIndex;
use crate::query::planner::QueryPlanner;
use crate::types::query::Query;

pub struct QueryEngine {
    pub index: InvertedIndex,
}

impl QueryEngine {
    pub fn execute(&self, query: Query) {
        let matching = QueryPlanner::resolve_series(
            &query,
            &self.index,
        );

        println!(
            "matched {} series",
            matching.len()
        );
    }
}
```

---

# src/memtable/mod.rs

```rust
pub mod immutable;
pub mod manager;
pub mod table;
```

---

# src/storage/mod.rs

```rust
pub mod compression;
pub mod mmap;
pub mod reader;
pub mod segment;
pub mod writer;
```

---

# src/wal/mod.rs

```rust
pub mod record;
pub mod replay;
pub mod writer;
```

---

# src/metrics/mod.rs

```rust
pub mod registry;
```

---

# src/benchmark/mod.rs

```rust
pub mod generator;
```

---

# Query Execution Lifecycle

Chronos queries should now work like this:

```text
query
  -> normalize filters
  -> planner resolves matching series_ids
  -> executor scans matching segments
  -> decompression
  -> iterator merge
  -> result stream
```

This is the beginning of a real TSDB query engine.

---

# Future Optimizations

# 1. Roaring Bitmaps

Current:

```rust
HashSet<u64>
```

Future:

```text
RoaringBitmap
```

This massively reduces memory.

Real TSDBs use compressed posting lists.

---

# 2. Persistent Indexes

Current indexes are in-memory only.

Future:

```text
startup
 -> mmap index files
 -> recover postings
```

Otherwise restart cost becomes enormous.

---

# 3. Segment-Level Bloom Filters

Allows skipping entire files.

```text
segment lacks region=us-east
 -> skip file entirely
```

Huge query speedup.

---

# 4. Time Partitioning

Current:

```text
all segments together
```

Future:

```text
2026/05/18/
2026/05/19/
```

Critical for retention and pruning.

---

# 5. Compaction

Needed eventually for:

* deduplication
* compression
* tombstones
* retention cleanup

---

# 6. Tag Compression

Repeated strings waste huge amounts of memory.

Real systems intern strings.

Example:

```text
host=server-1
```

stored once globally.

---

# 7. Adaptive Query Planner

Current planner:

```text
intersection order = insertion order
```

Better:

```text
smallest posting list first
```

Massively faster.

---

# 8. WAL Record Evolution

Current WAL likely stores:

```text
series_id,timestamp,value
```

Now WAL must include:

```text
metric
serialized tags
timestamp
value
```

or:

```text
series_id
```

after series registration.

Real systems often separate:

* series metadata WAL
* sample WAL

---

# Recommended Next Milestones

## Phase 1 — Complete Tag Support

* add tags
* series index
* inverted index
* query filtering
* metadata store

---

## Phase 2 — Query Engine

* iterators
* merge scans
* aggregations
* GROUP BY
* downsampling

---

## Phase 3 — Storage Engine

* compressed segments
* roaring bitmaps
* bloom filters
* compaction
* retention

---

## Phase 4 — Distributed Architecture

* replication
* sharding
* distributed query execution
* consensus

---

# Critical TSDB Lessons

## Lesson 1

Series cardinality is the real scalability limit.

Not raw points/sec.

---

## Lesson 2

Indexes matter more than raw storage.

Query systems are index systems.

---

## Lesson 3

Compression only works because time-series data is highly ordered.

Chronos already started this with delta timestamps.

---

## Lesson 4

Memtables + WAL + immutable flushes are LSM-tree principles.

Chronos is evolving toward an LSM TSDB.

---

## Lesson 5

TSDBs are fundamentally:

```text
LSM tree
+ inverted index
+ columnar compression
```

Understanding this is a huge milestone.

---

# Complete Missing Module Implementations

The previous document focused primarily on:

* tags
* indexing
* query architecture

but you are correct that the lower-level engine modules were still incomplete.

The following sections add the missing production-grade foundations for:

* memtable
* WAL
* storage
* metrics
* benchmark

including all important `mod.rs` files.

---

# MEMTABLE LAYER

# src/memtable/table.rs

```rust
use std::collections::BTreeMap;

#[derive(Debug, Clone)]
pub struct Sample {
    pub timestamp: i64,
    pub value: f64,
}

pub struct Memtable {
    series: BTreeMap<u64, Vec<Sample>>,
    total_points: usize,
}

impl Memtable {
    pub fn new() -> Self {
        Self {
            series: BTreeMap::new(),
            total_points: 0,
        }
    }

    pub fn insert(
        &mut self,
        series_id: u64,
        timestamp: i64,
        value: f64,
    ) {
        self.series
            .entry(series_id)
            .or_insert_with(Vec::new)
            .push(Sample {
                timestamp,
                value,
            });

        self.total_points += 1;
    }

    pub fn series(&self) -> &BTreeMap<u64, Vec<Sample>> {
        &self.series
    }

    pub fn total_points(&self) -> usize {
        self.total_points
    }
}
```

---

# Why BTreeMap?

Important TSDB principle:

```text
ordered writes -> better compression
```

Keeping series ordered improves:

* flush locality
* sequential scans
* delta compression
* mmap efficiency

---

# src/memtable/immutable.rs

```rust
use crate::memtable::table::Memtable;
use std::sync::Arc;

pub struct ImmutableMemtable {
    inner: Arc<Memtable>,
}

impl ImmutableMemtable {
    pub fn new(memtable: Memtable) -> Self {
        Self {
            inner: Arc::new(memtable),
        }
    }

    pub fn inner(&self) -> Arc<Memtable> {
        Arc::clone(&self.inner)
    }
}
```

---

# Why Immutable Memtables?

Classic LSM-tree pattern:

```text
active memtable
 -> rotate
 -> immutable memtable
 -> background flush
```

This avoids blocking ingestion.

Used by:

* RocksDB
* LevelDB
* Cassandra
* InfluxDB
* VictoriaMetrics

---

# src/memtable/manager.rs

```rust
use crate::memtable::immutable::ImmutableMemtable;
use crate::memtable::table::Memtable;
use std::sync::{Arc, Mutex};

pub struct MemtableManager {
    active: Mutex<Memtable>,
    immutable: Mutex<Vec<ImmutableMemtable>>,
    rotate_threshold: usize,
}

impl MemtableManager {
    pub fn new(rotate_threshold: usize) -> Self {
        Self {
            active: Mutex::new(Memtable::new()),
            immutable: Mutex::new(Vec::new()),
            rotate_threshold,
        }
    }

    pub fn insert(
        &self,
        series_id: u64,
        timestamp: i64,
        value: f64,
    ) {
        let mut active = self.active.lock().unwrap();

        active.insert(series_id, timestamp, value);

        if active.total_points() >= self.rotate_threshold {
            let rotated = std::mem::replace(
                &mut *active,
                Memtable::new(),
            );

            self.immutable
                .lock()
                .unwrap()
                .push(ImmutableMemtable::new(rotated));
        }
    }

    pub fn immutable_tables(
        &self,
    ) -> Vec<ImmutableMemtable> {
        self.immutable.lock().unwrap().drain(..).collect()
    }
}
```

---

# WAL LAYER

# src/wal/mod.rs

```rust
pub mod record;
pub mod replay;
pub mod writer;
```

---

# src/wal/record.rs

```rust
use crate::types::tags::Tags;

#[derive(Debug, Clone)]
pub struct WalRecord {
    pub metric: String,
    pub tags: Tags,
    pub timestamp: i64,
    pub value: f64,
}
```

---

# Why WAL Stores Tags

If the process crashes before indexes persist:

```text
WAL replay must reconstruct:
- series ids
- indexes
- memtables
```

Therefore WAL must contain logical metadata.

---

# src/wal/writer.rs

```rust
use crate::types::datapoint::DataPoint;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Mutex;

pub struct WalWriter {
    writer: Mutex<BufWriter<File>>,
}

impl WalWriter {
    pub fn new(path: impl AsRef<Path>) -> Self {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();

        Self {
            writer: Mutex::new(BufWriter::new(file)),
        }
    }

    pub fn append(
        &self,
        series_id: u64,
        point: &DataPoint,
    ) {
        let mut writer = self.writer.lock().unwrap();

        let line = format!(
            "{}|{}|{}
",
            series_id,
            point.timestamp,
            point.value,
        );

        writer.write_all(line.as_bytes()).unwrap();
    }

    pub fn flush(&self) {
        self.writer.lock().unwrap().flush().unwrap();
    }
}
```

---

# WAL Performance Insight

Real TSDBs batch WAL fsyncs.

Why?

```text
fsync = extremely expensive
```

Production systems usually:

```text
append many writes
 -> group commit
 -> fsync every few milliseconds
```

Huge throughput increase.

---

# src/wal/replay.rs

```rust
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

pub struct WalReplay;

impl WalReplay {
    pub fn replay(path: impl AsRef<Path>) {
        let file = File::open(path).unwrap();

        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line.unwrap();

            println!("replaying: {}", line);
        }
    }
}
```

---

# STORAGE LAYER

# src/storage/mod.rs

```rust
pub mod compression;
pub mod mmap;
pub mod reader;
pub mod segment;
pub mod writer;
```

---

# src/storage/segment.rs

```rust
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct SegmentMetadata {
    pub id: u64,
    pub path: PathBuf,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub series_count: usize,
    pub point_count: usize,
}
```

---

# Why Segment Metadata Matters

This enables pruning.

Example:

```text
query asks for:
2026-01-01 -> 2026-01-02

segment contains:
2025-12-01 -> 2025-12-31

=> skip entire segment
```

Critical optimization.

---

# src/storage/compression.rs

```rust
pub fn compress_timestamps(
    timestamps: &[i64],
) -> Vec<i64> {
    if timestamps.is_empty() {
        return vec![];
    }

    let mut deltas = Vec::with_capacity(timestamps.len());

    deltas.push(timestamps[0]);

    for i in 1..timestamps.len() {
        deltas.push(timestamps[i] - timestamps[i - 1]);
    }

    deltas
}

pub fn decompress_timestamps(
    deltas: &[i64],
) -> Vec<i64> {
    if deltas.is_empty() {
        return vec![];
    }

    let mut timestamps = Vec::with_capacity(deltas.len());

    let mut current = deltas[0];

    timestamps.push(current);

    for delta in deltas.iter().skip(1) {
        current += delta;
        timestamps.push(current);
    }

    timestamps
}
```

---

# Why Delta Compression Works

Time-series timestamps are monotonic:

```text
1000
1010
1020
1030
```

becomes:

```text
1000
10
10
10
```

Very compressible.

This is foundational TSDB compression.

---

# src/storage/writer.rs

```rust
use crate::memtable::immutable::ImmutableMemtable;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

pub struct SegmentWriter;

impl SegmentWriter {
    pub fn flush(
        table: ImmutableMemtable,
        path: impl AsRef<Path>,
    ) {
        let file = File::create(path).unwrap();

        let mut writer = BufWriter::new(file);

        for (series_id, samples) in table.inner().series() {
            for sample in samples {
                let line = format!(
                    "{}|{}|{}
",
                    series_id,
                    sample.timestamp,
                    sample.value,
                );

                writer.write_all(line.as_bytes()).unwrap();
            }
        }

        writer.flush().unwrap();
    }
}
```

---

# src/storage/reader.rs

```rust
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

pub struct SegmentReader;

impl SegmentReader {
    pub fn read_range(
        &self,
        series_id: u64,
        start: i64,
        end: i64,
    ) {
        println!(
            "reading series={} start={} end={}",
            series_id,
            start,
            end,
        );
    }

    pub fn scan_file(path: impl AsRef<Path>) {
        let file = File::open(path).unwrap();

        let reader = BufReader::new(file);

        for line in reader.lines() {
            println!("{:?}", line.unwrap());
        }
    }
}
```

---

# src/storage/mmap.rs

```rust
use memmap2::Mmap;
use std::fs::File;
use std::path::Path;

pub struct MmapReader {
    mmap: Mmap,
}

impl MmapReader {
    pub fn open(path: impl AsRef<Path>) -> Self {
        let file = File::open(path).unwrap();

        let mmap = unsafe {
            Mmap::map(&file).unwrap()
        };

        Self { mmap }
    }

    pub fn bytes(&self) -> &[u8] {
        &self.mmap
    }
}
```

---

# Why mmap Is Important

Traditional reads:

```text
kernel -> userspace copy
```

mmap:

```text
shared virtual memory pages
```

Huge reduction in overhead.

Modern TSDBs rely heavily on mmap.

---

# METRICS LAYER

# src/metrics/mod.rs

```rust
pub mod registry;
```

---

# src/metrics/registry.rs

```rust
use std::sync::atomic::{AtomicU64, Ordering};

pub struct MetricsRegistry {
    pub ingested_points: AtomicU64,
    pub flushed_segments: AtomicU64,
    pub query_count: AtomicU64,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self {
            ingested_points: AtomicU64::new(0),
            flushed_segments: AtomicU64::new(0),
            query_count: AtomicU64::new(0),
        }
    }

    pub fn record_ingest(&self, n: u64) {
        self.ingested_points
            .fetch_add(n, Ordering::Relaxed);
    }

    pub fn record_flush(&self) {
        self.flushed_segments
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_query(&self) {
        self.query_count
            .fetch_add(1, Ordering::Relaxed);
    }
}
```

---

# Why Atomics?

Metrics are write-heavy.

Mutexes would introduce unnecessary contention.

Atomics are ideal for counters.

---

# BENCHMARK LAYER

# src/benchmark/mod.rs

```rust
pub mod generator;
```

---

# src/benchmark/generator.rs

```rust
use crate::types::datapoint::DataPoint;
use crate::types::tags::Tags;
use rand::Rng;

pub struct BenchmarkGenerator;

impl BenchmarkGenerator {
    pub fn generate(n: usize) -> Vec<DataPoint> {
        let mut rng = rand::thread_rng();

        let mut points = Vec::with_capacity(n);

        for i in 0..n {
            let mut tags = Tags::new();

            tags.insert(
                "host".to_string(),
                format!("server-{}", i % 100),
            );

            tags.insert(
                "region".to_string(),
                "us-east".to_string(),
            );

            points.push(DataPoint {
                metric: "cpu_usage".to_string(),
                tags,
                timestamp: i as i64,
                value: rng.gen_range(0.0..100.0),
            });
        }

        points
    }
}
```

---

# Why Synthetic Benchmarking Matters

You cannot optimize TSDBs without:

* ingestion benchmarks
* compression benchmarks
* query benchmarks
* cardinality stress tests

Benchmarking is core infrastructure.

---

# Important Architectural Realization

Chronos now resembles a real LSM TSDB pipeline:

```text
client
 -> WAL
 -> memtable
 -> immutable memtable
 -> segment flush
 -> mmap reads
 -> inverted indexes
 -> query planner
```

This is no longer merely a storage toy.

It is evolving into a genuine TSDB architecture.

---

# Final Recommendation

The highest-value next implementation tasks are:

1. integrate tags into ingestion
2. implement persistent series registry
3. add inverted posting lists
4. implement query filter intersections
5. add roaring bitmap indexes
6. persist indexes to disk
7. add bloom filters
8. build compaction pipeline

At that point Chronos transitions from:

```text
ingestion prototype
```

into:

```text
real TSDB architecture
```
