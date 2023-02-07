import logging
import os
import socketserver

import simdjson as json

from .helpers import *


def get_key_path_from_file_path(file_path):
    return os.path.splitext(file_path)[0].split(os.sep)


class ClientCloseException(Exception):
    pass


class CommandProcessor:

    def __init__(self, ext):
        self.ext = ext

    def get_file_path_from_key_path(self, *args):
        return os.path.join(*args[:-1], f"{args[-1]}.{self.ext}")

    def process_command(self, server, conn, command):
        handled = True
        if command['name'] == 'unload':
            file_path = self.get_file_path_from_key_path(*command['key_path'])
            server.cache.unload_file(file_path)
            send_success(conn)
        elif command['name'] == 'load':
            file_path = self.get_file_path_from_key_path(*command['key_path'])
            data = server.cache.get_file(file_path)
            send_json(conn, length=len(data))
        else:
            handled = False
        return handled

class DataFrameCommandProcessor(CommandProcessor):

    def __init__(self):
        super().__init__("pkl")

    def process_command(self, server, conn, command):
        handled = True
        if command['name'] == 'update':
            df = recv_df(conn)
            server.cache.update(self.get_file_path_from_key_path(*command['key_path']), df)
            send_success(conn)
        elif command['name'] == 'filter':
            file_path = self.get_file_path_from_key_path(*command['key_path'])
            df = server.cache.get_dataframe(file_path, command.get('range_start'), command.get('range_end'), command.get('range_type'))
            if df is None:
                send_msg(conn, bytes([]))
            else:
                send_df(conn, df)
        else:
            handled = super().process_command(server, conn, command)
        return handled, True


class JsonFileCommandProcessor(CommandProcessor):

    def __init__(self):
        super().__init__("json")

    def process_command(self, server, conn, command):
        handled = True
        if command['name'] == 'set':
            data = recv_msg(conn)
            server.cache.update_file(self.get_file_path_from_key_path(*command['key_path']), data)
            send_success(conn)
        elif command['name'] == 'get':
            file_path = self.get_file_path_from_key_path(*command['key_path'])
            data = server.cache.get_file(file_path)
            send_msg(conn, data)
        else:
            handled = super().process_command(server, conn, command)
        return handled, True


class BytesCommandProcessor(CommandProcessor):

    def __init__(self):
        super().__init__("b")

    def process_command(self, server, conn, command):
        handled = True
        if command['name'] == 'set':
            data = recv_msg(conn)
            server.cache.update_file(self.get_file_path_from_key_path(*command['key_path']), data)
            send_success(conn)
        elif command['name'] == 'get':
            file_path = self.get_file_path_from_key_path(*command['key_path'])
            data = server.cache.get_file(file_path)
            send_msg(conn, data)
        else:
            handled = super().process_command(server, conn, command)
        return handled, True


class DefaultCommandHandler(CommandProcessor):

    def __init__(self):
        super().__init__(None)

    def process_command(self, server, conn, command):
        tinfo(json.dumps(command))
        handled = True
        if command['name'] == 'stats':
            stats = self.get_stats(server, level=command.get('level'))
            send_msg(conn, json.dumps(stats).encode())
        elif command['name'] == 'close':
            send_success(conn)
            raise ClientCloseException()
        else:
            handled = False
        return handled

    def get_stored_file_paths(self, root_path):
        all_files = []
        for path, _, files in os.walk(root_path):
            all_files.extend([get_key_path_from_file_path(os.path.join(path[len(root_path)+1:], f)) for f in files])
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
                    'max_memory': str(server.cache.max_memory),
                    'handlers': server.router.get_handler_names(),
                },
                'namespaces': server.router.get_namespace_map(),
            }
        if level >= 1:
            stats['loaded_files'] = [[get_key_path_from_file_path(k),str(v)] for k,v in server.cache.file_sizes.items()]
        if level >= 2:
            stats['all_files'] = self.get_stored_file_paths(server.cache.root_path)
        return stats


class CommandProcessRouter:

    def __init__(self, handler_map, namespace_map, default_handler=None):
        self.handler_map = handler_map
        self.namespace_map = namespace_map
        self.default_handler = default_handler or DefaultCommandHandler()

    def get_handler_names(self):
        return list(self.handler_map.keys())

    def get_namespace_map(self):
        return dict(**self.namespace_map)

    def handle(self, server, conn, command):
        key_path = command.get('key_path')
        if key_path is None:
            handled = self.default_handler.process_command(server, conn, command)
        else:
            if len(key_path) == 0:
                raise RuntimeError("expected keypath")
            namespace = key_path[0]
            namespace_handler_name = server.namespace_map.get(namespace)
            if namespace_handler_name is None:
                raise RuntimeError(f"namespace not defined: {namespace}")
            handler = server.handler_map.get(namespace_handler_name)
            if handler is None:
                raise RuntimeError(f"handler not defined: {namespace_handler_name}")
            handled = False if handler is None else handler.process_command(server, conn, command)
            handled = True if handled else self.default_handler.process_command(conn, command)
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
                    handled = self.server.router.handle(self.server, conn, command)
                    if not handled:
                        logging.warm(f"commadn not handled: {command}")
                except ClientCloseException as e:
                    addr = self.client_address[0]
                    logging.info(f'Connection closed by {addr}')
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
