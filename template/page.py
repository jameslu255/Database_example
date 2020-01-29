#from template.config import *


class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray()

    def has_capacity(self):
        return (4096 - 8 * self.num_records) > 0

    def write(self, value):
        if self.has_capacity():
            self.data += value.to_bytes(8, "big")
            self.num_records += 1
            return 0
        else:
            return -1
