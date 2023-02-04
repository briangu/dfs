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
$ pip3 install dataframe-service 
```

Start server

```bash
$ dfs_server
Serving on 0.0.0.0 port 8000 with max memory 1073741824 at root directory <current dir>
```

Run DFS REPL
```bash
$ rlwrap dfs_repl
Welcome to DFS REPL
author: Brian Guarraci
repo  : https://github.com/briangu/dfs
crtl-c to quit

?> stats
{
  "memory": {
    "used": "0",
    "free": "1073741824",
    "max": "1073741824"
  }
}
```
