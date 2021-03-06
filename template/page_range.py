from template.config import *
from template.counter import *


class PageRange:
    def __init__(self, id_num, num_cols):
        self.id_num = id_num        # unique ID of this page range
        self.num_base_pages = AtomicCounter()     # current base in the pageRange
        self.num_tail_pages = AtomicCounter()
        self.base_pages = []        # tracking base and tail pages within this page range
        self.tail_pages = []
        self.max_capacity = (5 + num_cols)  # want to hold num_cols and plus 4 default columns
        self.free_base_pages = []
        self.free_tail_pages = []

        self.update_count = AtomicCounter()   # Track number of updates a page range has had (for merge purposes)

    # check if our page range still has space to add more pages
    def page_range_has_capacity(self):
        # use 16 for now number changes depending on how many max pages we want to store
        # print("checking capacity, cur at ", self.num_base_pages)
        return (self.max_capacity - self.num_base_pages.value) >= 0

    def make_count_serializable(self):
        # if counter is an AtomicCounter, convert to int for serialization
        if isinstance(self.update_count, AtomicCounter):
            self.update_count = self.update_count.value
        if isinstance(self.num_base_pages, AtomicCounter):
            self.num_base_pages = self.num_base_pages.value
        if isinstance(self.num_tail_pages, AtomicCounter):
            self.num_tail_pages = self.num_tail_pages.value
        if isinstance(self.id_num, AtomicCounter):
            self.id_num = self.id_num.value


    def reset_counter(self):
        # if counter is an int, convert to AtomicCounter during deserialization
        if isinstance(self.update_count, int):
            self.update_count = AtomicCounter(self.update_count)
        if isinstance(self.num_base_pages, int):
            self.num_base_pages = AtomicCounter(self.num_base_pages)
        if isinstance(self.num_tail_pages, int):
            self.num_tail_pages = AtomicCounter(self.num_tail_pages)
        if isinstance(self.id_num, int):
            self.id_num = AtomicCounter(self.id_num)

