import logging
import os
import socketserver

import simdjson as json

from .helpers import *


class CommandHandler(socketserver.BaseRequestHandler):

    def setup(self) -> None:
        addr = self.client_address[0]
        logging.info(f'Connection created by {addr}')

    def process_command(self, conn, command):
        should_continue = True
        if command['type'] == 'insert':
            df = recv_df(conn)
            self.server.cache.append(command['file_path'], df)
            send_success(conn)
        elif command['type'] == 'get':
            file_path = command['file_path']
            df = self.server.cache.get_dataframe(file_path, command.get('range_start'), command.get('range_end'), command.get('range_type'))
            if df is None:
                send_msg(conn, bytes([]))
            else:
                send_df(conn, df)
        elif command['type'] == 'stats':
            send_msg(conn, json.dumps(self.get_stats(level=command.get('level'))).encode())
        elif command['type'] == 'unload':
            file_path = command['file_path']
            self.server.cache.unload_file(file_path)
            send_success(conn)
        elif command['type'] == 'load':
            file_path = command['file_path']
            data = self.server.cache.get_file(file_path)
            send_json(conn, length=len(data))
        elif command['type'] == 'close':
            send_success(conn)
            addr = self.client_address[0]
            logging.info(f'Connection closed by {addr}')
            should_continue = False
        return should_continue

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
                    should_continue = self.process_command(conn, command)
                    if not should_continue:
                        break
                except Exception as e:
                    import traceback
                    traceback.print_exc(e)
                    break

    def finish(self):
        addr = self.client_address[0]
        logging.info(f'Connection finished by {addr}')

    def get_stored_file_paths(self):
        all_files = []
        for path, _, files in os.walk(self.server.cache.root_path):
            all_files.extend([os.path.join(path[len(self.server.cache.root_path)+1:], f) for f in files])
        return all_files

    def get_stats(self, level=None):
        level = 0 if level is None else level
        stats = {
                'memory': {
                    'used': str(self.server.cache.current_memory_usage),
                    'free': str(self.server.cache.max_memory - self.server.cache.current_memory_usage),
                    'max': str(self.server.cache.max_memory)
                }
            }
        if level >= 1:
            stats['loaded_files'] = {k:str(v) for k,v in self.server.cache.file_sizes.items()}
        if level >= 2:
            stats['all_files'] = self.get_stored_file_paths()
        return stats


class DataFrameServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, cache, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cache = cache
