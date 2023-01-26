Pandas DataFrame Service (DFS)

# Features

    * Simple, ~100 lines multi-threaded file cache implementation
    * Key-value store for Panda DataFrames with basic index querying
    * Fixed budget memory consumption w/ LRU eviction
    * Supports updates on files and dataframes
    * Simple TCP client/server interface w/ client-side connection pooling

# Limitations

    1. Currently does not support replication, though the file system can be (e.g. NAS)


