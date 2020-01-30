#from template.config import *


class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray()

    def has_capacity(self):
        return (4096 - 8 * self.num_records) > 0

    def get_record_bytes(self, record_num):
        """
        Returns the given record number as bytes
        """
        # If out of bounds, return 0 bytes
        if (record_num >= self.num_records or record_num < 0):
            return bytes()
        return self.data[record_num * 8: record_num * 8 + 8]

    def get_record_int(self, record_num):
        """
        Returns the given record number as int 
        """
        byteval = self.get_record_bytes(record_num)
        if (byteval == bytes()):
            return -1
        return int.from_bytes(byteval, "big")

    def set_record(self, record_num, value):
        # If out of bounds, return 0 bytes
        if (record_num >= self.num_records or record_num < 0):
            return
        if self.data[record_num * 8: record_num * 8 + 8] != bytes(8):
            print("Cannot set record")
            return
        self.data[record_num * 8: record_num * 8 + 8] = value.to_bytes(8, "big")

    def write(self, value):
        if self.has_capacity():
            self.data += value.to_bytes(8, "big")
            self.num_records += 1
            return 0
        else:
            return -1
