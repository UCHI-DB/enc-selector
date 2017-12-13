import numpy as np


class DataSet:
    def __init__(self, data, label):
        self.data = data
        self.label = label
        self.size = self.data.shape[0]
        self.start = 0

    def next_batch(self, batch_size):
        if self.start == self.size:
            perm = np.random.permutation(self.size)
            self.data = self.data[perm]
            self.label = self.label[perm]
            self.start = 0
        start = self.start
        end = min(start + batch_size, self.size)
        self.start = end
        return [self.data[start:end], self.label[start:end]]
