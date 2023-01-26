import argparse
from multiprocessing.pool import ThreadPool
from dfs.df_client import *
from itertools import repeat

parser = argparse.ArgumentParser(description='Run DataFrame Service command')
parser.add_argument('--port', type=int, help='specify alternate port (default: 8000)', default=8000)
parser.add_argument('--host', type=str, help='specify alternate host address (default: 127.0.0.1)', default="127.0.0.1")
parser.add_argument('--max_connections', type=int, help='specify alternate source data directory (default: 8)', default=8)

args = parser.parse_args()

def scan_file(pool, *args):
    try:
        with pool.get_connection() as c:
            start_t = time.time_ns()
            data = c.load(*args[0])
            stop_t = time.time_ns()
            return (stop_t - start_t)/(10**9), data['length'] / 1024
    except ConnectionError as e:
        print(f"failed to connect")
        return (0, 0)
    except Exception as e:
        import traceback
        print(e)
        traceback.print_exc(e)
        return (0, 0)


with DataFrameConnectionPool(args.host, args.port, max_connections=args.max_connections) as pool:
    with pool.get_connection() as c:
        stats = c.get_stats(level=2)
    files = stats['all_files']

    print(f"scanning {len(files)} files...")

    with ThreadPool() as p:
        start_t = time.time()
        timings = p.starmap(scan_file, zip(repeat(pool), files))
        stop_t = time.time()
        time_s = sum([x[0] for x in timings])
        data_s = sum([x[1] for x in timings])
        if time_s > 0:
            print(f"{int((data_s / time_s) / 1000)} MB/s  {(stop_t - start_t)/len(files)} s/file")
