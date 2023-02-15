import logging
import os
import socketserver

import simdjson as json

from .helpers import *


def to_key_path(file_path):
    return file_path.split(os.sep)


class ClientCloseException(Exception):
    pass


class SystemCommandProcessor:

    @staticmethod
    def _to_file_path(*args):
        return os.path.join(*args)

    def process(self, server, conn, command):
        handled = True
        name = command['name']
        if name == 'unload':
            file_path = self._to_file_path(*command['key_path'])
            server.cache.unload_file(file_path)
            send_success(conn)
        elif name == 'load':
            file_path = self._to_file_path(*command['key_path'])
            data = server.cache.get_file(file_path)
            send_json(conn, length=len(data))
        elif name == 'stats':
            stats = self.get_stats(server, level=command.get('level'))
            send_msg(conn, json.dumps(stats).encode())
        elif name == 'close':
            send_success(conn)
            raise ClientCloseException()
        else:
            handled = False
        return handled

    def get_all_key_paths(self, root_path):
        key_paths = []
        for path, _, files in os.walk(root_path):
            key_paths.extend([to_key_path(os.path.join(path[len(root_path)+1:], f)) for f in files])
        return key_paths

    def get_stats(self, server, level=None):
        level = 0 if level is None else level
        stats = {
                'memory': {
                    'used': str(server.cache.current_memory_usage),
                    'free': str(server.cache.max_memory - server.cache.current_memory_usage),
                    'max': str(server.cache.max_memory)
                },
                'config': {
                    'root_path': server.cache.root_path,
                    'max_memory': str(server.cache.max_memory),
                },
            }
        if level >= 1:
            stats['loaded_keys'] = [[to_key_path(k),str(v[1])] for k,v in server.cache.file_futures.items()]
        if level >= 2:
            stats['all_keys'] = self.get_all_key_paths(server.cache.root_path)
        return stats


class FileCommandProcessor(SystemCommandProcessor):

    @staticmethod
    def _to_file_path(*args):
        return os.path.join(*args)

    def process(self, server, conn, command):
        handled = True
        name = command['name']
        if name == 'set':
            data = recv_msg(conn)
            server.cache.update_file(self._to_file_path(*command['key_path']), data)
            send_success(conn)
        elif name == 'get':
            file_path = self._to_file_path(*command['key_path'])
            data = server.cache.get_file(file_path)
            send_msg(conn, data)
        else:
            handled = super().process(server, conn, command)
        return handled


class DataFrameCommandProcessor(SystemCommandProcessor):

    @staticmethod
    def _to_file_path(*args):
        return os.path.join(*args)

    def process(self, server, conn, command):
        handled = True
        name = command['name']
        if name == 'df:update':
            df = recv_df(conn)
            server.cache.update(self._to_file_path(*command['key_path']), df)
            send_success(conn)
        elif name == 'df:filter':
            file_path = self._to_file_path(*command['key_path'])
            df = server.cache.get_dataframe(file_path, command.get('range_start'), command.get('range_end'), command.get('range_type'))
            if df is None:
                send_msg(conn, bytes([]))
            else:
                send_df(conn, df)
        else:
            handled = super().process(server, conn, command)
        return handled


class CommandHandler(socketserver.BaseRequestHandler):

    def setup(self) -> None:
        addr = self.client_address[0]
        logging.info(f'Connection created by {addr}')

    def handle(self):
        with self.request as conn:
            while True:
                try:
                    data = recv_msg(conn)
                except ConnectionResetError as e:
                    data = None
                if data is None:
                    addr = self.client_address[0]
                    logging.info(f'Connection dropped by {addr}')
                    break
                command = json.loads(data.decode())
                try:
                    handled = self.server.processor.process(self.server, conn, command)
                    if not handled:
                        logging.warm(f"command not handled: {command}")
                except ClientCloseException as e:
                    addr = self.client_address[0]
                    logging.info(f'Connection closed by {addr}')
                    break
                except MemoryError as e:
                    logging.warn(f"memory error: {command}")
                except Exception as e:
                    logging.error(f"exception: {command} {e}")
                    import traceback
                    traceback.print_exc(e)
                    break

    def finish(self):
        addr = self.client_address[0]
        logging.info(f'Connection finished by {addr}')


class DataFrameServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, cache, address, *args, **kwargs):
        super().__init__(address, CommandHandler, *args, **kwargs)
        self.cache = cache
        self.processor = DataFrameCommandProcessor()



class FileServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, cache, address, *args, **kwargs):
        super().__init__(address, CommandHandler, *args, **kwargs)
        self.cache = cache
        self.processor = FileCommandProcessor()
