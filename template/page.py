from template.config import *


class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        # pass
        empty_record_count = 0
        for record in self.data:
            if record == 0:
                empty_record_count += 1
        return empty_record_count

    def write(self, value):
        self.num_records += 1
        pass

