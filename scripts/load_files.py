import argparse
import pandas as pd
from dfs.df_client import *
import os
import multiprocessing as mp
from itertools import repeat


parser = argparse.ArgumentParser(description='Run DataFrame Service command')
parser.add_argument('--port', type=int, help='specify alternate port (default: 8000)', default=8000)
parser.add_argument('--host', type=str, help='specify alternate host address (default: 127.0.0.1)', default="127.0.0.1")
parser.add_argument('--max_connections', type=int, help='specify alternate source data directory (default: 8)', default=8)
parser.add_argument('--dir', type=str, help='specify alternate files directory (default: current dir)', default=os.getcwd())

args = parser.parse_args()

def load_dataframe(file_path):
    with open(file_path, "rb") as f:
        return pd.read_pickle(f.read())


def send_file(pool, file_path, *args):
    with pool as c:
        c.insert_data(load_dataframe(file_path), *args)


with DataFrameConnectionPool(args.host, args.port, max_connections=args.max_connections) as pool:
    arr = [f for f in os.listdir(args.dir) if os.path.isfile(f) and os.path.splitext(f)[1] == ".pkl"]
    with mp.Pool() as p:
        p.starmap(send_file, zip(repeat(pool), arr))
