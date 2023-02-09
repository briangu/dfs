import logging
import multiprocessing as mp
import socket
import threading
from queue import Queue

from .helpers import *


class CommandClient():
    def __init__(self, pool, conn):
        self.pool = pool
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.release_connection(self.conn)

    def unload(self, *args):
        send_cmd(self.conn, 'unload', key_path=args)
        recv_status(self.conn)

    def load(self, *args):
        send_cmd(self.conn, 'load', key_path=args)
        return recv_json(self.conn)

    def get_stats(self, level=None):
        send_cmd(self.conn, 'stats', level=level)
        return recv_json(self.conn)


class DataFrameClient(CommandClient):
    # TODO: add 'del' operation

    def __init__(self, pool, conn):
        self.pool = pool
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.release_connection(self.conn)

    def filter(self, *args, range_start=None, range_end=None, range_type="timestamp"):
        send_cmd(self.conn, 'df:filter', key_path=args, range_start=range_start, range_end=range_end, range_type=range_type)
        return recv_df(self.conn)

    def update(self, df, *args):
        send_cmd(self.conn, 'df:update', key_path=args)
        send_df(self.conn, df)
        recv_status(self.conn)



class FileClient(CommandClient):
    # TODO: add 'del' operation

    def __init__(self, pool, conn):
        self.pool = pool
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.release_connection(self.conn)

    def get(self, *args):
        send_cmd(self.conn, 'get', key_path=args)
        return recv_msg(self.conn)

    def set(self, contents, *args):
        send_cmd(self.conn, 'set', key_path=args)
        send_msg(self.conn, contents)
        recv_status(self.conn)


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
        self.default_client_class = client_class

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
                send_cmd(conn, 'close')
                recv_msg(conn)
            except Exception:
                pass
            finally:
                if conn is not None:
                    self.factory.close(conn)

    def get_connection(self, client_class=None):
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
        return (client_class or self.default_client_class)(self, conn)

    def release_connection(self, conn):
        self.connections.put(conn)
        self.semaphore.release()


