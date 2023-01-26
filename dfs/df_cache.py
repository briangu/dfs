import threading
import weakref

import pandas as pd
from .file_cache import FileCache
from .helpers import deserialize_df, df_memory_usage, serialize_df


class PandasDataFrameCache(FileCache):

    def __init__(self, max_memory=None, root_path=None):
        super().__init__(max_memory=max_memory, root_path=root_path)
        self.append_locks = weakref.WeakValueDictionary()

    def process_contents(self, contents):
        df = pd.DataFrame() if len(contents) == 0 else deserialize_df(contents)
        return df, df_memory_usage(df)

    def get_dataframe(self, file_name, range_start=None, range_end=None, range_type=None):
        df = self.get_file(file_name)
        if range_start is None:
            if range_end is None:
                return df
            else:
                return df[(df.index <= range_end)] if range_type == "timestamp" else df[:range_end]
        else:
            if range_end is None:
                return df[(df.index >= range_start)] if range_type == "timestamp" else df[range_start:]
            else:
                return df[(df.index >= range_start)&(df.index <= range_end)] if range_type == "timestamp" else df[range_start:range_end]

    def append(self, file_name, new_df):
        with self.file_futures_lock:
            flock = self.append_locks.get(file_name)
            if flock is None:
                flock = threading.Lock()
                self.append_locks[file_name] = flock
        with flock:
            try:
                df = self.get_file(file_name)
                df = pd.concat([df, new_df])
            except FileNotFoundError:
                df = new_df
            df = df.sort_index()
            df = df[~df.index.duplicated(keep='first')]
            update_applied = self.update_file(file_name, serialize_df(df))
            return df if update_applied else self.append(file_name, new_df)
