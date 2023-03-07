from .exceptions import *
class RoundRobin:
    def __init__(self):
        self.counter = 0
    
    def generate(self, array: list):
        if len(array) == 0:
            raise EmptyList("Input list is empty")
        if self.counter >= len(array):
            self.counter = 0
        x = array[self.counter]
        self.counter += 1
        return x