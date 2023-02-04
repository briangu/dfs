import gzip
import logging
import struct
import threading
import time
from functools import wraps
from io import BytesIO

import pandas as pd
import simdjson as json


def send_msg(conn, msg):
    conn.sendall(struct.pack('>I', len(msg)) + msg)


def recv_msg(conn):
    raw_msglen = recvall(conn, 4)
    if not raw_msglen:
        # raise RuntimeError("server error")
        return None
    msglen = struct.unpack('>I', raw_msglen)[0]
    return recvall(conn, msglen)


def recvall(conn, n):
    data = bytearray()
    while len(data) < n:
        packet = conn.recv(n - len(data))
        if not packet:
            return None
        data.extend(packet)
    return data


def recv_df(conn):
    data = recv_msg(conn)
    if data is None or len(data) == 0:
        raise ValueError("no data")
    bio = BytesIO(data)
    with gzip.open(bio, 'rb') as f:
        return pd.read_pickle(f)


def send_df(conn, df):
    bio = BytesIO()
    with gzip.open(bio, 'wb', compresslevel=1) as f:
        df.to_pickle(f)
    send_msg(conn, bio.getvalue())


def send_json(conn, **kwargs):
    send_msg(conn, json.dumps({str(k):v for k,v in kwargs.items()}).encode())


def recv_json(conn):
    return json.loads(recv_msg(conn).decode())


def save_df(file_path, df):
    with open(file_path, "wb") as f:
        with gzip.open(f, "wb", compresslevel=9) as gzf:
            df.to_pickle(gzf)


def read_df(file_path):
    with open(file_path, "rb") as f:
        with gzip.open(f, "rb") as gzf:
            return pd.read_pickle(gzf)


def send_status(conn, err=None):
    if err is None:
        send_json(conn, success=True)
    else:
        send_json(conn, success=False, err=str(err))


def send_success(conn):
    send_status(conn)


def recv_status(conn):
    status = recv_json(conn)
    if status['success']:
        return True
    raise(RuntimeError(status['err']))


def send_cmd(conn, name, **kwargs):
    send_json(conn, name=name, **kwargs)


def df_memory_usage(df):
    return df.memory_usage(deep=True).sum()


def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter_ns()
        try:
            result = func(*args, **kwargs)
        finally:
            end_time = time.perf_counter_ns()
        total_time = (end_time - start_time)
        print(f'Function {func.__name__} Took {(total_time/1000000):.4f} ms')
        return result
    return timeit_wrapper


def tinfo(msg):
    logging.info(f"tid: {threading.current_thread().ident}: " + msg)


def serialize_df(df):
    bio = BytesIO()
    with gzip.open(bio, 'wb') as f:
        df.to_pickle(f)
    return bio.getvalue()


def deserialize_df(data):
    bio = BytesIO(data)
    with gzip.open(bio, "rb") as gzf:
        return pd.read_pickle(gzf)
