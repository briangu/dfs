import os
import tempfile
import threading
import unittest

from dfs.file_cache import FileCache
import time
import platform

# TODO: add MacOS RAM disk
# hdiutil attach -nomount ram://$((2 * 1024 * 100))
# diskutil eraseVolume HFS+ RAMDisk /dev/disk3
# https://stackoverflow.com/questions/1854/how-to-identify-which-os-python-is-running-on
tmp_dir = "/dev/shm" if platform.system() == "Linux" else None


class FileCacheTests(unittest.TestCase):
    def setUp(self):
        print("setting up")
        self.test_file_1 = tempfile.NamedTemporaryFile(delete=False, dir=tmp_dir)
        self.test_file_2 = tempfile.NamedTemporaryFile(delete=False, dir=tmp_dir)
        self.test_file_3 = tempfile.NamedTemporaryFile(delete=False, dir=tmp_dir)
        self.test_file_4 = tempfile.NamedTemporaryFile(delete=False, dir=tmp_dir)
        self.file_cache = FileCache(max_memory=(len(b'test_file_1') * 4 - 1))
        print("setting up done")

    def test_get_file(self):
        print("test_get_file")
        file_len = len(b'test_file_1')
        print(f"file_len == {file_len}")
        self.file_cache = FileCache(max_memory=(file_len * 4 - 1))
        self.test_file_1.write(b'test_file_1')
        self.test_file_1.seek(0)
        self.test_file_2.write(b'test_file_2')
        self.test_file_2.seek(0)
        self.test_file_3.write(b'test_file_3')
        self.test_file_3.seek(0)
        self.test_file_4.write(b'test_file_4')
        self.test_file_4.seek(0)

        print("calling get_file")
        file_1_content = self.file_cache.get_file(self.test_file_1.name)
        print(f"current_memory_usage: {self.file_cache.current_memory_usage}")
        self.assertEqual(self.file_cache.current_memory_usage, file_len)

        file_2_content = self.file_cache.get_file(self.test_file_2.name)
        self.assertEqual(self.file_cache.current_memory_usage, file_len * 2)

        self.assertEqual(file_1_content, b'test_file_1')
        self.assertEqual(file_2_content, b'test_file_2')

        # Test that the file is read from the cache
        file_1_content_cached = self.file_cache.get_file(self.test_file_1.name)
        self.assertEqual(file_1_content_cached, b'test_file_1')

        file_2_content_cached = self.file_cache.get_file(self.test_file_2.name)
        self.assertEqual(file_2_content_cached, b'test_file_2')

        # Test that the oldest file is evicted when the cache is full
        file_3_content = self.file_cache.get_file(self.test_file_3.name)
        self.assertEqual(file_3_content, b'test_file_3')

        print(f"current_memory_usage: {self.file_cache.current_memory_usage}")
        file_4_content = self.file_cache.get_file(self.test_file_4.name)
        self.assertEqual(file_4_content, b'test_file_4')

        print(f"current_memory_usage: {self.file_cache.current_memory_usage}")
        print(self.file_cache.file_futures.keys())
        self.assertNotIn(self.test_file_1.name, self.file_cache.file_futures)
        print("test_get_file done")

    def test_update_file(self):
        print("test_update_file")
        self.test_file_1.write(b'test_file_1')
        self.test_file_1.seek(0)
        file_1_content = self.file_cache.get_file(self.test_file_1.name)
        self.assertEqual(file_1_content, b'test_file_1')

        # Update the file in the cache
        updated = self.file_cache.update_file(self.test_file_1.name, b'updated_test_file_1')
        self.assertTrue(updated)

        # Check that the updated file is returned
        updated_file_1_content = self.file_cache.get_file(self.test_file_1.name)
        self.assertEqual(updated_file_1_content, b'updated_test_file_1')
        print("test_update_file done")

    def test_update_file_multithreaded(self):
        self.test_file_1.write(b'test_file_1')
        self.test_file_1.seek(0)
        file_1_content = self.file_cache.get_file(self.test_file_1.name)
        self.assertEqual(file_1_content, b'test_file_1')

        def update_file_thread(file_name, contents):
            updated = self.file_cache.update_file(file_name, contents)
            while not updated:
                updated = self.file_cache.update_file(file_name, contents)

        # Update the test file in multiple threads
        threads = []
        for i in range(10):
            thread = threading.Thread(target=update_file_thread, args=(self.test_file_1.name, b'updated_test_file_1'))
            thread.start()
            threads.append(thread)

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Check that the updated file is returned
        updated_file_1_content = self.file_cache.get_file(self.test_file_1.name)
        self.assertEqual(updated_file_1_content, b'updated_test_file_1')

    def test_update_file_multithreaded_expected(self):
        self.test_file_1.write(b'test_file_1')
        self.test_file_1.seek(0)
        file_1_content = self.file_cache.get_file(self.test_file_1.name)
        self.assertEqual(file_1_content, b'test_file_1')
        self.failed = False
        self.run = True

        # Create a list of expected values for each thread
        expected_values = [b'updated_test_file_1_thread_1', b'updated_test_file_1_thread_2', b'updated_test_file_1_thread_3',
                           b'updated_test_file_1_thread_4', b'updated_test_file_1_thread_5', b'updated_test_file_1_thread_6',
                           b'updated_test_file_1_thread_7', b'updated_test_file_1_thread_8', b'updated_test_file_1_thread_9',
                           b'updated_test_file_1_thread_10']
        all_read_expected_values = [b'test_file_1', *expected_values]

        def update_file_thread(file_name, contents):
            while self.run:
                updated = self.file_cache.update_file(file_name, contents)
                while not updated:
                    updated = self.file_cache.update_file(file_name, contents)

        def get_file_thread():
            while self.run:
                updated_file_1_content = self.file_cache.get_file(self.test_file_1.name)
                if updated_file_1_content not in all_read_expected_values:
                    self.failed = True
                self.assertIn(updated_file_1_content, all_read_expected_values)

        # Update the test file in multiple threads
        threads = []
        for i in range(10):
            thread = threading.Thread(target=update_file_thread, args=(self.test_file_1.name, expected_values[i]))
            thread.start()
            threads.append(thread)

        for i in range(100):
            thread = threading.Thread(target=get_file_thread)
            thread.start()
            threads.append(thread)

        time.sleep(3)
        self.run = False

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        self.assertFalse(self.failed)

        # Check that the final contents of the file match one of the expected values
        updated_file_1_content = self.file_cache.get_file(self.test_file_1.name)
        self.assertIn(updated_file_1_content, expected_values)

    def tearDown(self):
        os.unlink(self.test_file_1.name)
        os.unlink(self.test_file_2.name)
        os.unlink(self.test_file_3.name)
        os.unlink(self.test_file_4.name)

if __name__ == '__main__':
  unittest.main()
