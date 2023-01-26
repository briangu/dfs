import random
import numpy as np
import psutil

class TCPServer:
    def __init__(self, host, port, buffer_size):
        self.host = host
        self.port = port
        self.buffer_size = buffer_size
        self.recv_loops = 0
        self.messages = 0
        self.active_clients = 0

    def start(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, self.port))
            s.listen()
            while True:
                conn, addr = s.accept()
                self.active_clients += 1
                self.handle_client(conn, addr)

    def handle_client(self, conn, addr):
        self.recv_loops = 0
        self.messages = 0
        with conn:
            print('Connected by',addr)
            while True:
                buffer_size = rl_agent.get_action(get_state())
                chunk = conn.recv(buffer_size)
                self.recv_loops += 1
                if not chunk:
                    break
                self.messages += 1
                data += chunk
                if self.messages % 10 == 0:
                    rl_agent.update_reward(get_reward())
            self.active_clients -= 1

    def get_state(self):
        network_congestion = psutil.net_io_counters()
        return (self.buffer_size, self.active_clients, network_congestion)

    def get_reward(self):
        #calculate the reward based on the relevant factors
        return -(self.recv_loops / self.messages) #


