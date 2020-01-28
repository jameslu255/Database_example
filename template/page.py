#from template.config import *


class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        return (4096 - self.num_records) > 0

    def write(self, value):
        if self.has_capacity():
            # data will be written at idx 4 since 
            # the first 4 columns are reserved
            self.data[self.num_records] = value
            self.num_records += 1
            return 0
        else:
            return -1
