# Pandas DataFrame Service (DFS)

## Features

    * Simple, ~100 lines multi-threaded file cache implementation
    * Key-value store for Panda DataFrames with basic index querying
    * Fixed budget memory consumption w/ LRU eviction
    * Supports updates on files and dataframes
    * Simple TCP client/server interface w/ client-side connection pooling

## Limitations

    1. Currently does not support replication, though the file system can be (e.g. NAS)

## Usage

Install

```bash
$ pip3 install git+https://github.com/briangu/dfs.git
```

Start server

```bash
$ dfs_server
Serving on 0.0.0.0 port 8000 with max memory 1073741824 at root directory <current dir>
```

Run Script
```bash
$ git clone
$ python3 scripts/show_stats.py
{
  "memory": "0",
  "loaded_files": {}
}
mem: 0 sizes: 0
```
