import unittest
import sys, os, shutil

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from lsm_tree import LSMStorageEngine

class TestLSMStorageEngine(unittest.TestCase):
    def setUp(self):
        self.test_dir = './tmp_test_lsm_db'
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        self.engine = LSMStorageEngine(self.test_dir, memtable_threshold=3)

    def tearDown(self):
        self.engine.close()
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_put_get_delete(self):
        self.engine.put('k1', 'v1')
        self.engine.put('k2', 'v2')
        self.assertEqual(self.engine.get('k1'), 'v1')
        self.assertEqual(self.engine.get('k2'), 'v2')

        self.engine.delete('k1')
        self.assertIsNone(self.engine.get('k1'))

    def test_memtable_flush_to_sstable(self):
        self.engine.put('a', '1')
        self.engine.put('b', '2')
        self.engine.put('c', '3')  # Triggers flush
        self.assertEqual(len(self.engine.sstables), 1)
        self.assertEqual(self.engine.get('a'), '1')
        self.assertEqual(self.engine.get('b'), '2')
        self.assertEqual(self.engine.get('c'), '3')

    def test_crash_recovery_from_wal(self):
        self.engine.put('x', '100')
        self.engine.put('y', '200')
        self.engine.close()

        # Re-open engine on same dir
        restarted_engine = LSMStorageEngine(self.test_dir, memtable_threshold=3)
        self.assertEqual(restarted_engine.get('x'), '100')
        self.assertEqual(restarted_engine.get('y'), '200')
        restarted_engine.close()

if __name__ == '__main__':
    unittest.main()
