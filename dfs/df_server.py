import logging
import os
import socketserver

import simdjson as json

from .helpers import *


def get_file_path_from_key_path(*args):
    return os.path.join(*args[:-1], f"{args[-1]}.pkl")


def get_key_path_from_file_path(file_path):
    return os.path.splitext(file_path)[0].split(os.sep)


class CommandProcessor:
    def process_command(self, server, conn, command):
        pass


class DataFrameCommandProcessor(CommandProcessor):

    def process_command(self, server, conn, command):
        handled = True
        if command['name'] == 'update':
            df = recv_df(conn)
            server.cache.update(get_file_path_from_key_path(*command['key_path']), df)
            send_success(conn)
        elif command['name'] == 'filter':
            file_path = get_file_path_from_key_path(*command['key_path'])
            df = server.cache.get_dataframe(file_path, command.get('range_start'), command.get('range_end'), command.get('range_type'))
            if df is None:
                send_msg(conn, bytes([]))
            else:
                send_df(conn, df)
        elif command['name'] == 'unload':
            file_path = get_file_path_from_key_path(*command['key_path'])
            server.cache.unload_file(file_path)
            send_success(conn)
        elif command['name'] == 'load':
            file_path = get_file_path_from_key_path(*command['key_path'])
            data = server.cache.get_file(file_path)
            send_json(conn, length=len(data))
        else:
            handled = False
        return handled, True


class CommandProcessRouter:

    def __init__(self, handler_map, namespace_map):
        self.handler_map = handler_map
        self.namespace_map = namespace_map

    def get_handler_names(self):
        return list(self.handler_map.keys())

    def get_namespace_map(self):
        return dict(**self.namespace_map)

    def handle(self, conn, command, default_processor):
        key_path = command.get('key_path')
        if key_path is None or len(key_path) == 0:
            raise RuntimeError("expected keypath")
        namespace = key_path[0]
        namespace_handler_name = self.server.namespace_map.get(namespace)
        if namespace_handler_name is None:
            raise RuntimeError(f"namespace not defined: {namespace}")
        handler = self.server.handler_map.get(namespace_handler_name)
        if handler is None:
            raise RuntimeError(f"handler not defined: {namespace_handler_name}")
        handled = False if handler is None else handler.process_command(self.server, conn, command)
        should_continue = True if handled else default_processor(conn, command)
        return should_continue


class CommandHandler(socketserver.BaseRequestHandler):

    def setup(self) -> None:
        addr = self.client_address[0]
        logging.info(f'Connection created by {addr}')

    def process_command(self, conn, command):
        tinfo(json.dumps(command))
        should_continue = True
        if command['name'] == 'stats':
            stats = self.get_stats(level=command.get('level'))
            send_msg(conn, json.dumps(stats).encode())
        # elif command['name'] == 'unload':
        #     file_path = get_file_path_from_key_path(*command['key_path'])
        #     self.server.cache.unload_file(file_path)
        #     send_success(conn)
        # elif command['name'] == 'load':
        #     file_path = get_file_path_from_key_path(*command['key_path'])
        #     data = self.server.cache.get_file(file_path)
        #     send_json(conn, length=len(data))
        elif command['name'] == 'close':
            send_success(conn)
            addr = self.client_address[0]
            logging.info(f'Connection closed by {addr}')
            should_continue = False
        return should_continue

    def get_stored_file_paths(self):
        all_files = []
        for path, _, files in os.walk(self.server.cache.root_path):
            all_files.extend([get_key_path_from_file_path(os.path.join(path[len(self.server.cache.root_path)+1:], f)) for f in files])
        return all_files

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
                    'max_memory': str(server.cache.max_memory)
                }
            }
        if level >= 1:
            stats['loaded_files'] = [[get_key_path_from_file_path(k),str(v)] for k,v in server.cache.file_sizes.items()]
        if level >= 2:
            stats['all_files'] = self.get_stored_file_paths()
        return stats

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
                    should_continue = self.server.router.handle(conn, command, self.process_command)
                    if not should_continue:
                        break
                except Exception as e:
                    import traceback
                    traceback.print_exc(e)
                    break

    def finish(self):
        addr = self.client_address[0]
        logging.info(f'Connection finished by {addr}')


class DataFrameServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, cache, router, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cache = cache
        self.router = router
