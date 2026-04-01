# Log-Structured Merge-Tree (LSM-Tree) Storage Engine

## Overview
A production-grade, high-performance Key-Value storage engine built from scratch in Python.

## Architecture
1. **Write-Ahead Log (WAL)**: Ensures append-only durability and instant crash recovery.
2. **MemTable**: In-memory sorted map for O(log N) writes and reads before flushing.
3. **SSTable (Sorted String Table)**: Immutable disk files featuring binary block layout, Sparse Indexes, and Bloom Filters.
4. **Bloom Filter**: Eliminates unnecessary disk reads for non-existent keys.
5. **MVCC & Tombstones**: Handles atomic updates and soft deletions.

## Usage
```python
from lsm_tree import LSMStorageEngine

engine = LSMStorageEngine(data_dir='./data', memtable_threshold=100)
engine.put('key1', 'value1')
val = engine.get('key1')
engine.delete('key1')
```

## Running Unit Tests
```bash
python -m unittest discover -s tests
```
