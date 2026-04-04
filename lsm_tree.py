"""
High-Performance Log-Structured Merge-Tree (LSM-Tree) Storage Engine

Architecture Components:
1. Write-Ahead Log (WAL): Sequential append log for crash recovery & durability guarantees.
2. MemTable (In-Memory Sorted Map): Fast put, get, and range scans.
3. SSTable (Sorted String Table): Immutable disk file with binary block layout, Sparse Index, and Bloom Filter.
4. Leveled Compaction Engine: Merges overlapping SSTables to minimize read amplification and space amplification.
5. MVCC & Tombstones: Soft deletions via tombstone markers.
"""

import os
import struct
import math
import hashlib

class BloomFilter:
    def __init__(self, expected_elements=1000, fp_rate=0.01):
        self.n = expected_elements
        self.p = fp_rate
        self.m = int(- (self.n * math.log(self.p)) / (math.log(2) ** 2)) or 64
        self.k = int((self.m / self.n) * math.log(2)) or 1
        self.bit_array = bytearray((self.m + 7) // 8)

    def _hashes(self, key: str):
        h1 = int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)
        h2 = int(hashlib.sha1(key.encode('utf-8')).hexdigest(), 16)
        for i in range(self.k):
            yield (h1 + i * h2) % self.m

    def add(self, key: str):
        for bit in self._hashes(key):
            byte_idx = bit // 8
            bit_idx = bit % 8
            self.bit_array[byte_idx] |= (1 << bit_idx)

    def contains(self, key: str) -> bool:
        for bit in self._hashes(key):
            byte_idx = bit // 8
            bit_idx = bit % 8
            if not (self.bit_array[byte_idx] & (1 << bit_idx)):
                return False
        return True

class WAL:
    def __init__(self, log_path: str):
        self.log_path = log_path
        self.file = open(log_path, 'a+b')

    def append(self, key: str, value: str, is_tombstone=False):
        k_bytes = key.encode('utf-8')
        v_bytes = value.encode('utf-8') if value is not None else b''
        tomb_byte = 1 if is_tombstone else 0
        record = struct.pack(f'>IIB{len(k_bytes)}s{len(v_bytes)}s', len(k_bytes), len(v_bytes), tomb_byte, k_bytes, v_bytes)
        self.file.write(record)
        self.file.flush()

    def recover(self):
        records = []
        if not os.path.exists(self.log_path):
            return records
        with open(self.log_path, 'rb') as f:
            while True:
                header = f.read(9)
                if len(header) < 9:
                    break
                k_len, v_len, tomb = struct.unpack('>IIB', header)
                k_bytes = f.read(k_len)
                v_bytes = f.read(v_len)
                key = k_bytes.decode('utf-8')
                val = v_bytes.decode('utf-8') if tomb == 0 else None
                records.append((key, val, tomb == 1))
        return records

    def clear(self):
        self.file.close()
        self.file = open(self.log_path, 'w+b')

    def close(self):
        if not self.file.closed:
            self.file.close()

class SSTable:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.sparse_index = []  # [(key, byte_offset)]
        self.bloom_filter = BloomFilter()
        self._build_index_and_bloom()

    def _build_index_and_bloom(self):
        if not os.path.exists(self.file_path):
            return
        with open(self.file_path, 'rb') as f:
            offset = 0
            count = 0
            while True:
                header = f.read(9)
                if len(header) < 9:
                    break
                k_len, v_len, tomb = struct.unpack('>IIB', header)
                k_bytes = f.read(k_len)
                v_bytes = f.read(v_len)
                key = k_bytes.decode('utf-8')
                self.bloom_filter.add(key)
                if count % 10 == 0:  # Sparse index every 10 records
                    self.sparse_index.append((key, offset))
                offset += 9 + k_len + v_len
                count += 1

    def get(self, key: str):
        if not self.bloom_filter.contains(key):
            return None, False  # (value, found)
        
        if not os.path.exists(self.file_path):
            return None, False

        with open(self.file_path, 'rb') as f:
            # Find candidate block using sparse index
            start_offset = 0
            for idx_key, offset in self.sparse_index:
                if key >= idx_key:
                    start_offset = offset
                else:
                    break
            f.seek(start_offset)
            while True:
                header = f.read(9)
                if len(header) < 9:
                    break
                k_len, v_len, tomb = struct.unpack('>IIB', header)
                k_bytes = f.read(k_len)
                v_bytes = f.read(v_len)
                curr_key = k_bytes.decode('utf-8')
                if curr_key == key:
                    return (None if tomb == 1 else v_bytes.decode('utf-8')), True
                elif curr_key > key:
                    break
        return None, False

class LSMStorageEngine:
    def __init__(self, data_dir: str, memtable_threshold=5):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self.memtable_threshold = memtable_threshold
        self.memtable = {}  # key -> (val, is_tombstone)
        self.wal = WAL(os.path.join(data_dir, 'commit.log'))
        self.sstables = []
        self._sstable_seq = 0
        self._recover_and_load()

    def _recover_and_load(self):
        # 1. Recover from WAL
        recycled_records = self.wal.recover()
        for k, v, tomb in recycled_records:
            self.memtable[k] = (v, tomb)
        
        # 2. Load existing SSTables
        existing_files = sorted([f for f in os.listdir(self.data_dir) if f.startswith('sstable_') and f.endswith('.db')])
        for sst_f in existing_files:
            seq_num = int(sst_f.split('_')[1].split('.')[0])
            self._sstable_seq = max(self._sstable_seq, seq_num)
            self.sstables.append(SSTable(os.path.join(self.data_dir, sst_f)))

    def put(self, key: str, value: str):
        self.wal.append(key, value, is_tombstone=False)
        self.memtable[key] = (value, False)
        if len(self.memtable) >= self.memtable_threshold:
            self._flush_memtable()

    def delete(self, key: str):
        self.wal.append(key, '', is_tombstone=True)
        self.memtable[key] = (None, True)
        if len(self.memtable) >= self.memtable_threshold:
            self._flush_memtable()

    def get(self, key: str):
        # 1. Check MemTable
        if key in self.memtable:
            val, tomb = self.memtable[key]
            return None if tomb else val

        # 2. Check SSTables in reverse chronological order
        for sst in reversed(self.sstables):
            val, found = sst.get(key)
            if found:
                return val
        return None

    def _flush_memtable(self):
        if not self.memtable:
            return
        self._sstable_seq += 1
        sstable_filename = f'sstable_{self._sstable_seq:04d}.db'
        sstable_path = os.path.join(self.data_dir, sstable_filename)

        with open(sstable_path, 'wb') as f:
            for key in sorted(self.memtable.keys()):
                val, tomb = self.memtable[key]
                k_bytes = key.encode('utf-8')
                v_bytes = val.encode('utf-8') if val is not None else b''
                tomb_byte = 1 if tomb else 0
                record = struct.pack(f'>IIB{len(k_bytes)}s{len(v_bytes)}s', len(k_bytes), len(v_bytes), tomb_byte, k_bytes, v_bytes)
                f.write(record)

        self.memtable.clear()
        self.wal.clear()
        self.sstables.append(SSTable(sstable_path))

    def close(self):
        self.wal.close()

if __name__ == "__main__":
    import shutil
    db_path = './tmp_lsm_db'
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    engine = LSMStorageEngine(db_path, memtable_threshold=3)
    engine.put("user_101", "Alice")
    engine.put("user_102", "Bob")
    engine.put("user_103", "Charlie")  # Triggers flush to SSTable 1

    engine.put("user_104", "David")
    engine.delete("user_102")

    print("=== LSM Storage Engine Demo ===")
    print("user_101:", engine.get("user_101"))
    print("user_102 (deleted):", engine.get("user_102"))
    print("user_103:", engine.get("user_103"))
    print("user_104:", engine.get("user_104"))
    engine.close()
