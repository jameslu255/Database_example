from template.config import *


class PageRange:
    def __init__(self, id_num, num_cols):
        self.id_num = id_num        # unique ID of this page range
        self.num_base_pages = 0     # current base in the pageRange
        self.num_tail_pages = 0
        self.base_pages = []        # tracking base and tail pages within this page range
        self.tail_pages = []
        self.start_rid_base = 0     # first starting RID in this page range (0 for null value since rid start at 1)
        self.end_rid_base = 0
        self.start_rid_tail = 0     # first starting RID of tail in this page range (0 for null value since rid start at 1)
        self.end_rid_tail = 0
        self.max_capacity = (4 + num_cols)  # want to hold num_cols and plus 4 default columns
        self.free_base_pages = []
        self.free_tail_pages = []

        self.update_count = 0       # Track number of updates a page range has had (for merge purposes)

    # check if our page range still has space to add more pages
    def page_range_has_capacity(self):
        # use 16 for now number changes depending on how many max pages we want to store
        # print("checking capacity, cur at ", self.num_base_pages)
        return (self.max_capacity - self.num_base_pages) >= 0
