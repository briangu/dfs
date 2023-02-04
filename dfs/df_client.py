import multiprocessing as mp
import os
import socket
import threading
from queue import Queue

import simdjson as json

from .helpers import *
import logging


class DataFrameClient:
    def __init__(self, pool, conn):
        self.pool = pool
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.release_connection(self.conn)

    def get_data(self, *args, range_start=None, range_end=None, range_type="timestamp"):
        send_cmd(self.conn, 'get', file_path=args, range_start=range_start, range_end=range_end, range_type=range_type)
        return recv_df(self.conn)

    def insert_data(self, df, *args):
        send_cmd(self.conn, 'insert', file_path=args)
        send_df(self.conn, df)
        recv_status(self.conn)

    def unload(self, *args):
        send_cmd(self.conn, 'unload', file_path=args)
        recv_status(self.conn)

    def load(self, *args):
        send_cmd(self.conn, 'load', file_path=args)
        return recv_json(self.conn)

    def get_stats(self, level=None):
        send_msg(self.conn, json.dumps({'type': 'stats', 'level': level}).encode())
        return recv_json(self.conn)


class DataFrameConnectionFactory:
    def __init__(self, host, port):
        self.host = host
        self.port = port

    def create_socket(self):
        tinfo(f"Creating connection to: {(self.host, self.port)}")
        return socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def connect(self, sock):
        sock.connect((self.host, self.port))

    def is_connected(self, sock):
        return self.get_status(sock) == 0

    def get_status(self, sock):
        return sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)

    def close(self, sock):
        sock.close()


class DataFrameConnectionPool:
    def __init__(self, host, port, max_connections=int(mp.cpu_count()*0.8), max_retries=3, client_class=DataFrameClient):
        logging.info(f"Creating connection pool with {max_connections} connections")
        self.factory = DataFrameConnectionFactory(host, port)
        self.max_connections = max_connections
        self.connections = Queue()
        self.semaphore = threading.Semaphore(max_connections)
        self.max_retries = max_retries
        self.client_class = client_class

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._shutdown()

    def __del__(self):
        self._shutdown()

    def _shutdown(self):
        while not self.connections.empty():
            conn = None
            try:
                conn = self.connections.get()
                send_msg(conn, json.dumps({'type': 'close'}).encode())
                recv_msg(conn)
            except Exception:
                pass
            finally:
                if conn is not None:
                    self.factory.close(conn)

    def get_connection(self):
        self.semaphore.acquire()
        if self.connections.empty():
            conn = None
        else:
            conn = self.connections.get()
            if not self.factory.is_connected(conn):
                tinfo(f"Releasing closed connection")
                conn = None
        if conn is None:
            tinfo(f"Creating socket")
            conn = self.factory.create_socket()
            attempts = 1
            while attempts <= self.max_retries:
                try:
                    self.factory.connect(conn)
                    break
                except socket.error:
                    tinfo(f"Connection Failed, Retrying...{attempts}")
                    time.sleep(2**attempts)
                    attempts += 1
            if self.factory.is_connected(conn):
                tinfo(f"Connection created after {attempts} attempts")
            else:
                tinfo(f"Connection failed after {attempts} attempts")
                self.semaphore.release()
                raise ConnectionError(f"Connection failed after {attempts} attempts")
        return self.client_class(self, conn)

    def release_connection(self, conn):
        self.connections.put(conn)
        self.semaphore.release()


