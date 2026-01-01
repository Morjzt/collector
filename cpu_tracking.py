import os
import psutil

class Node:
    def __init__(self, value):
        self.value = value
        self.next = None
        self.prev = None

class InvariantDeque:
    def __init__(self, maxlen: int):
        self.maxlen = maxlen
        self.head = None
        self.tail = None
        self.size = 0
        self.running_total = 0.0 

    def append(self, value):
        new_node = Node(value)
        if not self.head:
            self.head = self.tail = new_node
        else:
            new_node.prev = self.tail
            self.tail.next = new_node
            self.tail = new_node
        
        self.running_total += value
        self.size += 1

        if self.size > self.maxlen:
            self._pop_left()

    def _pop_left(self):
        if not self.head:
            return
        self.running_total -= self.head.value
        self.head = self.head.next
        if self.head:
            self.head.prev = None
        else:
            self.tail = None
        self.size -= 1

    def get_average(self):
        if self.size == 0:
            return 0.0
        return round(self.running_total / self.size, 2)


class CPUMonitor:
    def __init__(self, window_size: int = 15):
        self.history = InvariantDeque(maxlen=window_size)
        self._process = psutil.Process(os.getpid())
        self._process.cpu_percent(interval=None)

    def update(self) -> float:
        current_usage = self._process.cpu_percent(interval=None)
        self.history.append(current_usage)
        return self.history.get_average()