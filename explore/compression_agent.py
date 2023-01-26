import numpy as np
import math

def get_closest_power_of_2(size):
    return int(math.pow(2, math.ceil(math.log(size, 2))))


class CompressionAgent:
    def __init__(self, state_size, action_size, learning_rate=0.01, discount_factor=0.95, max_compression_rate=9):
        self.state_size = state_size
        self.action_size = action_size
        self.learning_rate = learning_rate
        self.discount_factor = discount_factor
        self.q_table = np.zeros((state_size, action_size))
        self.max_compression_rate = max_compression_rate
        self.current_compression_rate = 6

    def get_compression_rate(self, state):
        state[0] = get_closest_power_of_2(state[0])
        rate = np.argmax(self.q_table[state])
        if rate > self.max_compression_rate:
            rate = self.max_compression_rate
        return rate

    def update_reward(self, state, action, reward, next_state):
        self.current_compression_rate = action
        q_next_state = np.max(self.q_table[next_state])
        q_target = reward + self.discount_factor * q_next_state
        q_current = self.q_table[state, action]
        self.q_table[state, action] = q_current + self.learning_rate * (q_target - q_current)
