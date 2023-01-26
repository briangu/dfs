import heapq
import os
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Lock


class FileCache:
    def __init__(self, max_memory=None, root_path=None):
        self.max_memory = max_memory or 2**20
        self.root_path = root_path or os.getcwd()
        self.current_memory_usage = 0
        self.file_futures = {}
        self.file_sizes = {}
        self.file_write_futures = {}
        self.file_access_times = []
        self.file_futures_lock = Lock()
        self.executor = ThreadPoolExecutor()

    def process_contents(self, contents):
        return contents, len(contents)

    def _load_file(self, file_name):
        with open(os.path.join(self.root_path, file_name), 'rb') as file:
            contents, memory_usage = self.process_contents(file.read())
            self.update_file_futures_and_memory(file_name, memory_usage=memory_usage)
            return contents

    def _write_file(self, file_name, new_file_contents):
        with open(os.path.join(self.root_path, file_name), 'wb') as f:
            f.write(new_file_contents)
            os.fsync(f.fileno())
        contents, memory_usage = self.process_contents(new_file_contents)
        self.update_file_futures_and_memory(file_name, memory_usage=memory_usage)
        return contents

    def update_file_access_time(self, file_name):
        self.file_access_times = [(t, fn) for t, fn in self.file_access_times if fn != file_name]
        heapq.heappush(self.file_access_times, (time.time_ns(), file_name))

    def update_file_futures_and_memory(self, file_name, memory_usage):
        with self.file_futures_lock:
            if file_name in self.file_sizes:
                self.current_memory_usage -= self.file_sizes[file_name]
            self.file_sizes[file_name] = memory_usage
            self.current_memory_usage += self.file_sizes[file_name]
            self.update_file_access_time(file_name)
            if file_name in self.file_write_futures:
                self.file_write_futures.pop(file_name)

    def update_file(self, file_name, new_file_contents):
        claim = len(new_file_contents)
        if claim > self.max_memory:
            raise MemoryError(f"requested file update larger than max_memory: {file_name} {claim} {self.max_memory}")
        with self.file_futures_lock:
            write_future = self.file_write_futures.get(file_name)
            if write_future is None:
                if not self.recover_memory(claim):
                    raise MemoryError(f"unable to recover memory for requsted file: {file_name} {claim}")
                write_future = self.executor.submit(self._write_file, file_name, new_file_contents)
                self.file_write_futures[file_name] = write_future
                self.file_futures[file_name] = write_future
                write_applied = True
            else:
                write_applied = False
        write_future.result()
        return write_applied

    def _unload_file(self, file_name):
        assert self.file_futures_lock.locked()
        self.current_memory_usage -= self.file_sizes[file_name]
        del self.file_futures[file_name]
        del self.file_sizes[file_name]

    def unload_file(self, file_name):
        with self.file_futures_lock:
            self._unload_file(file_name)

    def recover_memory(self, claim):
        assert self.file_futures_lock.locked()
        while (self.current_memory_usage + claim) > self.max_memory:
            oldest_file = heapq.heappop(self.file_access_times)[1]
            self._unload_file(oldest_file)
        return (self.current_memory_usage + claim) <= self.max_memory

    def get_file(self, file_name):
        full_file_path = os.path.join(self.root_path, file_name)
        if not os.path.exists(full_file_path):
            raise FileNotFoundError(file_name)
        claim = os.path.getsize(full_file_path)
        if claim > self.max_memory:
            raise MemoryError(f"requested file larger than max_memory: {file_name} {claim} {self.max_memory}")
        future = None
        with self.file_futures_lock:
            future = self.file_futures.get(file_name)
            if future is None:
                write_future = self.file_write_futures.get(file_name)
                if write_future is not None:
                    return write_future.result()
                if not self.recover_memory(claim):
                    raise MemoryError(f"unable to recover memory for requsted file: {file_name} {claim}")
                future = self.executor.submit(self._load_file, file_name)
                self.file_futures[file_name] = future
            else:
                self.update_file_access_time(file_name)
        return future.result()
