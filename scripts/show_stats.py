import argparse
from dfs.df_client import *
import simdjson as json

parser = argparse.ArgumentParser(description='Run DataFrame Service command')
parser.add_argument('--port', type=int, help='specify alternate port (default: 8000)', default=8000)
parser.add_argument('--host', type=str, help='specify alternate host address (default: 127.0.0.1)', default="127.0.0.1")

args = parser.parse_args()

with DataFrameConnectionPool(args.host, args.port).get_connection() as c:
    stats = c.get_stats(level=1)
    print(json.dumps(stats, indent=2))
    s = sum([int(x) for x in stats['file_sizes'].values()])
    print(f"mem: {stats['memory']} sizes: {s}")
    stats = c.get_stats(level=1)
