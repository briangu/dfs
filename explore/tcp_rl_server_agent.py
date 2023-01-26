import numpy as np
class QLearningAgent:
    def __init__(self, state_size, action_size, learning_rate=0.01, discount_factor=0.95):
        self.state_size = state_size
        self.action_size = action_size
        self.learning_rate = learning_rate
        self.discount_factor = discount_factor
        self.q_table = np.zeros((state_size, action_size))

    def get_action(self, state):
        return np.argmax(self.q_table[state])

    def update_reward(self, state, action, reward, next_state):
        q_next_state = np.max(self.q_table[next_state])
        q_target = reward + self.discount_factor * q_next_state
        q_current = self.q_table[state, action]
        self.q_table[state, action] = q_current + self.learning_rate * (q_target - q_current)


class QLearningAgent:
    def __init__(self, state_size, action_size, learning_rate=0.01, discount_factor=0.95, max_buffer_size=4096):
        self.state_size = state_size
        self.action_size = action_size
        self.learning_rate = learning_rate
        self.discount_factor = discount_factor
        self.q_table = np.zeros((state_size, action_size))
        self.max_buffer_size = max_buffer_size
        self.current_buffer_size = 4096

    def get_action(self, state):
        action =  np.argmax(self.q_table[state])
        if action > self.max_buffer_size:
            action = self.max_buffer_size
        if action < self.current_buffer_size:
            self.q_table[state,self.current_buffer_size] = -1000000
        return action

    def update_reward(self, state, action, reward, next_state):
        self.current_buffer_size = action
        q_next_state = np.max(self.q_table[next_state])
        q_target = reward + self.discount_factor * q_next_state
        q_current = self.q_table[state, action]
        self.q_table[state, action] = q_current + self.learning_rate * (q_target - q_current)


class QLearningAgent:
    def __init__(self, state_size, action_size, learning_rate=0.01, discount_factor=0.95, max_buffer_size=4096):
        self.state_size = state_size
        self.action_size = action_size
        self.learning_rate = learning_rate
        self.discount_factor = discount_factor
        self.q_table = np.zeros((state_size, action_size))
        self.max_buffer_size = max_buffer_size
        self.current_buffer_size = 4096

    def get_action(self, state):
        action =  np.argmax(self.q_table[state])
        if action > self.max_buffer_size:
            action = self.max_buffer_size
        return action

    def update_reward(self, state, action, reward, next_state):
        self.current_buffer_size = action
        q_next_state = np.max(self.q_table[next_state])
        if action > self.current_buffer_size:
            reward -= (action-self.current_buffer_size)/self.max_buffer_size # this will decrease the reward if the buffer size gets bigger
        q_target = reward + self.discount_factor * q_next_state
        q_current = self.q_table[state, action]
        self.q_table[state, action] = q_current + self.learning_rate * (q_target - q_current)
