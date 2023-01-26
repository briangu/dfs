import argparse
import os

from dfs.df_client import *

parser = argparse.ArgumentParser(description='Run DataFrame Service command')
parser.add_argument('--port', type=int, help='specify alternate port (default: 8000)', default=8000)
parser.add_argument('--host', type=str, help='specify alternate host address (default: 127.0.0.1)', default="127.0.0.1")
parser.add_argument('--max_connections', type=int, help='specify alternate source data directory (default: 8)', default=8)
parser.add_argument('--dir', type=str, help='specify alternate files directory (default: current dir)', default=os.getcwd())

args = parser.parse_args()


def compare_dfs(stats, df, dfs_root_path, *args):
    m = df_memory_usage(df)
    print(f"{args} stats: {stats['memory']} df: {m}")
    if int(stats['memory']) != int(m):
        gzip_fname = os.path.join(dfs_root_path, get_file_path_from_key_path(*args))
        fdf = read_df(gzip_fname)
        print(len(df), len(fdf), df.equals(fdf))
    assert int(stats['memory']) == int(m)


def verify_file(pool, dfs_root_path, *args):
    with pool.get_connection() as c:
        c.load(*args)
        stats = c.get_stats()
        df = c.get_data(*args)
        compare_dfs(stats, df, dfs_root_path, *args)
        c.unload(*args)


with DataFrameConnectionPool(args.host, args.port, max_connections=args.max_connections) as pool:

    with pool.get_connection() as c:
        stats = c.get_stats(level=2)

        for key in stats['loaded_files'].keys():
            c.unload(*key)

        files = stats['all_files']

        stats = c.get_stats(level=0)
        assert int(stats['memory']) == 0

    for key in files:
        verify_file(pool, args.dir, *key)
