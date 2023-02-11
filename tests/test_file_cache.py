import os
import platform
import random
import tempfile
import threading
import time
import unittest
from multiprocessing.pool import ThreadPool

from dfs.file_cache import FileCache

# TODO: add MacOS RAM disk
# hdiutil attach -nomount ram://$((2 * 1024 * 100))
# diskutil eraseVolume HFS+ RAMDisk /dev/disk3
# https://stackoverflow.com/questions/1854/how-to-identify-which-os-python-is-running-on

def gen_file(tmp_dir=None):
    tmp_dir = tmp_dir or ("/dev/shm" if platform.system() == "Linux" else None)
    f = tempfile.NamedTemporaryFile(delete=False, dir=tmp_dir)
    d = bytearray(os.urandom(random.randint(10,50)))
    f.write(d)
    f.seek(0)
    return f,d,len(d)

class FileCacheTests(unittest.TestCase):
    def setUp(self):
        self.file_contents = {i:gen_file() for i in range(4)}
        # make cache just under capactiy to hold all 4 files to force eviction
        self.file_cache = FileCache(max_memory=(sum([x[2] for x in self.file_contents.values()]) - 1))

    def test_get_file(self):
        info = self.file_contents[0]
        contents = self.file_cache.get_file(info[0].name)
        self.assertEqual(self.file_cache.current_memory_usage, info[2])
        self.assertTrue(info[0].name in self.file_cache.file_futures)
        self.assertEqual(self.file_cache.file_futures[info[0].name][1], info[2])
        self.assertEqual(contents, info[1])
        self.assertEqual(len(self.file_cache.file_access_times), 1)
        self.assertEqual(info[0].name, self.file_cache.file_access_times[0][1])

    def test_get_file_with_eviction(self):
        for i in range(2):
            for info in self.file_contents.values():
                expected_memory_usage = self.file_cache.current_memory_usage + info[2]
                if expected_memory_usage > self.file_cache.max_memory:
                    oldest_size = self.file_cache.file_futures[self.file_cache.file_access_times[0][1]][1]
                    expected_memory_usage -= oldest_size
                contents = self.file_cache.get_file(info[0].name)
                self.assertEqual(self.file_cache.current_memory_usage, expected_memory_usage)
                self.assertTrue(info[0].name in self.file_cache.file_futures)
                self.assertEqual(self.file_cache.file_futures[info[0].name][1], info[2])
                self.assertEqual(contents, info[1])
                self.assertEqual(info[0].name, self.file_cache.file_access_times[-1][1])

    def test_get_file_multithreaded(self):
        def get_file_thread(file, data, size):
            self.assertEqual(self.file_cache.get_file(file.name), data)

        with ThreadPool() as pool:
            pool.starmap(get_file_thread, random.choices(list(self.file_contents.values()), k=100))

    def test_unload_file(self):
        for info in self.file_contents.values():
            self.file_cache.get_file(info[0].name)
            self.assertEqual(self.file_cache.current_memory_usage, info[2])
            self.file_cache.unload_file(info[0].name)
            self.assertFalse(info[0].name in self.file_cache.file_futures)
            self.assertEqual(self.file_cache.current_memory_usage, 0)
            self.assertEqual(len(self.file_cache.file_access_times), 0)

    def test_update_file(self):
        info = self.file_contents[0]
        content = self.file_cache.get_file(info[0].name)
        self.assertEqual(content, info[1])

        # Update the file in the cache
        new_contents = bytearray(os.urandom(random.randint(10,50)))
        updated = self.file_cache.update_file(info[0].name, new_contents)
        self.assertTrue(updated)
        self.assertTrue(info[0].name in self.file_cache.file_futures)
        self.assertEqual(self.file_cache.current_memory_usage, len(new_contents))
        self.assertEqual(len(self.file_cache.file_access_times), 1)
        self.assertEqual(info[0].name, self.file_cache.file_access_times[0][1])

        # Check that the updated file is returned
        content = self.file_cache.get_file(info[0].name)
        self.assertEqual(content, new_contents)

        self.file_cache.unload_file(info[0].name)
        self.assertFalse(info[0].name in self.file_cache.file_futures)
        self.assertEqual(self.file_cache.current_memory_usage, 0)
        self.assertEqual(len(self.file_cache.file_access_times), 0)

    # def test_update_file_multithreaded(self):
    #     file_1_content = self.file_cache.get_file(self.test_file_1.name)
    #     self.assertEqual(file_1_content, b'test_file_1')

    #     def update_file_thread(file_name, contents):
    #         updated = self.file_cache.update_file(file_name, contents)
    #         while not updated:
    #             updated = self.file_cache.update_file(file_name, contents)

    #     # Update the test file in multiple threads
    #     threads = []
    #     for i in range(10):
    #         thread = threading.Thread(target=update_file_thread, args=(self.test_file_1.name, b'updated_test_file_1'))
    #         thread.start()
    #         threads.append(thread)

    #     # Wait for all threads to complete
    #     for thread in threads:
    #         thread.join()

    #     # Check that the updated file is returned
    #     updated_file_1_content = self.file_cache.get_file(self.test_file_1.name)
    #     self.assertEqual(updated_file_1_content, b'updated_test_file_1')

    # def test_update_file_multithreaded_expected(self):
    #     self.test_file_1.write(b'test_file_1')
    #     self.test_file_1.seek(0)
    #     file_1_content = self.file_cache.get_file(self.test_file_1.name)
    #     self.assertEqual(file_1_content, b'test_file_1')
    #     self.failed = False
    #     self.run = True

    #     # Create a list of expected values for each thread
    #     expected_values = [b'updated_test_file_1_thread_1', b'updated_test_file_1_thread_2', b'updated_test_file_1_thread_3',
    #                        b'updated_test_file_1_thread_4', b'updated_test_file_1_thread_5', b'updated_test_file_1_thread_6',
    #                        b'updated_test_file_1_thread_7', b'updated_test_file_1_thread_8', b'updated_test_file_1_thread_9',
    #                        b'updated_test_file_1_thread_10']
    #     all_read_expected_values = [b'test_file_1', *expected_values]

    #     def update_file_thread(file_name, contents):
    #         while self.run:
    #             updated = self.file_cache.update_file(file_name, contents)
    #             while not updated:
    #                 updated = self.file_cache.update_file(file_name, contents)

    #     def get_file_thread():
    #         while self.run:
    #             updated_file_1_content = self.file_cache.get_file(self.test_file_1.name)
    #             if updated_file_1_content not in all_read_expected_values:
    #                 self.failed = True
    #             self.assertIn(updated_file_1_content, all_read_expected_values)

    #     # Update the test file in multiple threads
    #     threads = []
    #     for i in range(10):
    #         thread = threading.Thread(target=update_file_thread, args=(self.test_file_1.name, expected_values[i]))
    #         thread.start()
    #         threads.append(thread)

    #     for i in range(100):
    #         thread = threading.Thread(target=get_file_thread)
    #         thread.start()
    #         threads.append(thread)

    #     time.sleep(3)
    #     self.run = False

    #     # Wait for all threads to complete
    #     for thread in threads:
    #         thread.join()

    #     self.assertFalse(self.failed)

    #     # Check that the final contents of the file match one of the expected values
    #     updated_file_1_content = self.file_cache.get_file(self.test_file_1.name)
    #     self.assertIn(updated_file_1_content, expected_values)

    def tearDown(self):
        for v in self.file_contents.values():
            os.unlink(v[0].name)

if __name__ == '__main__':
  unittest.main()
