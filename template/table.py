from template.page import *
from template.page_range import *
from template.manager import * 
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

    def __str__(self):
        return f"Record(RID: {self.rid}, Columns: {self.columns})"

class Table:
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        # SID -> RID
        self.keys = {}
        # RID -> Page
        self.base_page_directory = {}
        # RID -> Page_tail
        self.tail_page_directory = {}
        
        # number of records a table has
        self.base_rid = 0
        # tail page id
        self.tail_rid = 0

        self.capacity = (num_columns + 4) * 4
        # bufferpool size
        self.size = 0

        # BufferPoolManager
        self.base_page_manager = BufferPoolManager(self.num_columns + 4, 
                                                    "base_pages.bin")
        self.tail_page_manager = BufferPoolManager(self.num_columns + 4, 
                                                    "tail_pages.bin")

        # will increment each time we create a new page range (acts as unique ID used to differentiate PR's)
        # also will tell us index of current pr in the pr array
        self.cur_page_range_id = 0
        # page range list which stores all the page ranges
        self.page_ranges = []

        # create our first page range
        self.page_ranges.append(PageRange(self.cur_page_range_id, self.num_columns))

        # create pages for the indirection, rid, timestamp, schema encoding column
        self.create_base_page("indirection")  # index 0
        self.create_base_page("rid")  # index 1
        self.create_base_page("timestamp")  # index 2
        self.create_base_page("schema")  # index 3

        
        # create pages for the key and the data columns
        for x in range(num_columns):
            self.create_base_page(x)

    def __merge(self):
        pass

    def has_capacity(self):
        return self.size <= self.capacity

    def create_tail_page(self, col, base_rid):
        # get the base_page that's getting updated rid (passed in as base_rid)
        # find the page range that base_page is in
        # add the tail page to that page range
        pr_id = (base_rid // (512 + 1))  # given the base_rid we can find the page range we want
        cur_pr = self.page_ranges[pr_id]

        # Check if we have room for the new page
        if not self.has_capacity():
            self.tail_page_manager.evict(cur_pr.tail_pages)
            self.size -= 1

        # create the page and push to array holding pages
        new_page = Page()
        # Check if we have room for the new page
        
        # add this tail page to the page range's tail list
        cur_pr.tail_pages.append(new_page)
        self.size +=1

        # keep track of index of page relative to array index
        if (len(cur_pr.free_tail_pages) < self.num_columns + 4):  # when initializing
            cur_pr.free_tail_pages.append(len(cur_pr.tail_pages) - 1)
        else:  # when creating new page and need to update the index
            cur_pr.free_tail_pages[col] = len(cur_pr.tail_pages) - 1

        # Add page to lru
        self.tail_page_manager.update_page_usage(cur_pr.id_num, len(cur_pr.tail_pages) - 1)
        # Not dirty because we didn't write to it

        cur_pr.num_tail_pages += 1

    def update_tail_page(self, col, value, base_rid):
        pr_id = base_rid // (512 + 1)  # given the base_rid we can find the page range we want
        # update the page linked to the col
        # print(str(col) + " writing val: " + str(value) + " of type " + str(type(value)))
        cur_pr = self.page_ranges[pr_id]
        index_relative = cur_pr.free_tail_pages[col]
        cur_page = cur_pr.tail_pages[index_relative]
        # If page isn't in bufferpool
        if cur_page == None:
            # If we don't have enough space to bring in another page
            if not self.has_capacity():
                self.tail_page_manager.evict(cur_pr.tail_pages)
                self.size -= 1
            # Fetch the page from disk
            fetched_page = self.tail_page_manager.fetch(cur_pr.id_num, index_relative)
            cur_pr.tail_pages[index_relative] = fetched_page
            cur_page = fetched_page
            self.size += 1
         
        error = cur_page.write(value)
        if error == -1:  # maximum size reached in page
            # Check if we have room for the new page
            if not self.has_capacity():
                self.tail_page_manager.evict(cur_pr.tail_pages)
                self.size -= 1

            # create new page
            page = Page()
            self.size += 1

            # write to new page
            page.write(value)
            # append the new page
            cur_pr.tail_pages.append(page)
            # pagerangenum * num_cols + page
            # add this tail page to the page range's tail list
            new_tail_page_num = len(cur_pr.tail_pages) - 1
            # update free page index to point to new blank page
            cur_pr.free_tail_pages[col] = new_tail_page_num
            # Update LRU
            self.tail_page_manager.update_page_usage(cur_pr.id_num, new_tail_page_num)
            # Page is dirty
            self.tail_page_manager.set_page_dirty(cur_pr.id_num, new_tail_page_num)
        else:
            # update lru
            self.tail_page_manager.update_page_usage(cur_pr.id_num, index_relative)
            # Page is dirty
            self.tail_page_manager.set_page_dirty(cur_pr.id_num, index_relative)

        if cur_pr.end_rid_tail == 0:
            cur_pr.start_rid_tail = self.tail_rid

        cur_pr.end_rid_tail = self.tail_rid

    def update_tail_rid(self, column_index, rid, value, base_rid):
        pr_id = base_rid // (512 + 1)
        cur_pr = self.page_ranges[pr_id]
        cur_page = cur_pr.tail_pages[column_index]

        # If page isn't in bufferpool
        if cur_page == None:
            # If we don't have enough space to bring in another page
            if not self.has_capacity():
                self.tail_page_manager.evict(cur_pr.tail_pages)
                self.size -= 1
            fetched_page = self.tail_page_manager.fetch(cur_pr.id_num, column_index)
            cur_pr.tail_pages[column_index] = fetched_page
            cur_page = fetched_page
            self.size += 1
            
        # Update LRU
        self.tail_page_manager.update_page_usage(cur_pr.id_num, column_index)
        # Page is dirty
        self.tail_page_manager.set_page_dirty(cur_pr.id_num, column_index)
        # Update the page
        cur_page.set_record(rid, value)

    def update_base_rid(self, column_index, rid, value):
        pr_id = rid // (512 + 1)
        cur_pr = self.page_ranges[pr_id]
        # Get the index of the base page
        base_page_index = cur_pr.free_base_pages[column_index]
        cur_page = cur_pr.base_pages[base_page_index]
        # If page isn't in bufferpool
        if cur_page == None:
            # If we don't have enough space to bring in another page
            if not self.has_capacity():
                self.base_page_manager.evict(cur_pr.base_pages)
                self.size -= 1
            fetched_page = self.tail_page_manager.fetch(cur_pr.id_num, base_page_index)
            cur_pr.base_pages[base_page_index] = fetched_page
            cur_page = fetched_page
         
        # Update LRU
        self.base_page_manager.update_page_usage(cur_pr.id_num, base_page_index)
        # Page is dirty
        self.base_page_manager.set_page_dirty(cur_pr.id_num, base_page_index)
        # Get the record's offset
        base_offset = rid - (512 * pr_id)
        # Set the record's value
        cur_page.set_record(base_offset, value)

    def create_base_page(self, col_name):
        # Get the current page range
        cur_pr = self.page_ranges[self.cur_page_range_id]

       # Check if we have room for the new page
        if not self.has_capacity():
            self.tail_page_manager.evict(cur_pr.base_pages)
            self.size -= 1

        # check current PR can hold more
        self.create_new_pr_if_necessary()

        # print("creating new page for " + str(col_name))
        # create the page and push to array holding pages
        new_page = Page()
        self.size += 1
        # also add page to the list of base pages in pr
        cur_pr = self.page_ranges[self.cur_page_range_id]
        cur_pr.base_pages.append(new_page)
        cur_pr.free_base_pages.append(len(cur_pr.base_pages) - 1)
        # Update LRU
        self.base_page_manager.update_page_usage(self.cur_page_range_id, 
                                            len(cur_pr.base_pages) - 1)
        # Not dirty because new page was not written to


    def update_base_page(self, index, value, rid):
        # print("updating col", index, "with", value, "for rid", rid)
        # update the page linked to the index
        pr_id = rid // (512 + 1)
        # index_relative = self.free_base_pages[index]
        # print("pr_id", pr_id)
        
        if pr_id >= len(self.page_ranges): #no new page range
            # make new page range
            # print("making new pange range")
            self.cur_page_range_id += 1  # this pr is full - update the pr id
            new_pr = PageRange(self.cur_page_range_id, self.num_columns)
            self.page_ranges.append(new_pr)  # add this new pr with new id to the PR list
            # initiaThe Maze lize base pages on new pange range creation
            for x in range(self.num_columns + 4):
                self.create_base_page(x)
            
        pr = self.page_ranges[pr_id]
        index_relative = pr.free_base_pages[index]
        cur_page = pr.base_pages[index_relative]
        # If page isn't in bufferpool
        if cur_page == None:
            # If we don't have enough space to bring in another page
            if not self.has_capacity():
                self.base_page_manager.evict(pr.base_pages)
                self.size -= 1

            # Fetch the page
            fetched_page = self.base_page_manager.fetch(pr.id_num, index_relative)
            cur_pr.tail_pages[column_index] = fetched_page
            cur_page = fetched_page
         
        error = cur_page.write(value)
        if error == -1:  # maximum size reached in page
            # Check if we have room for the new page
            if not self.has_capacity():
                self.base_page_manager.evict(pr.base_pages)
                self.size -= 1

            # similar to above check if we have space in page range/create if necessary/update
            # create new page
            page = Page()

            # self.append_base_page_to_pr(page)
            # also add page to the list of base pages in pr
            page.write(value)
            pr.base_pages.append(page)
            pr.free_base_pages.append(len(pr.base_pages) - 1)
            
            # Update LRU
            self.base_page_manager.update_page_usage(pr.id_num, len(pr.base_pages) - 1)
            self.base_page_manager.set_page_dirty(pr.id_num, len(pr.base_pages) - 1)
            # increment the num pages count in either case (full or not full since we are adding a new page)
            pr.num_base_pages += 1
            self.size += 1
        else:
            # Update LRU
            self.base_page_manager.update_page_usage(pr.id_num, index_relative)
            # Page is dirty
            self.base_page_manager.set_page_dirty(pr.id_num, index_relative)

        # print("current page range: " + str(cur_pr_id_num))
        if pr.start_rid_base == 0:
            pr.start_rid_base = self.base_rid

        pr.end_rid_base = self.base_rid

    # creates a new page range if the current one gets filled up/does housekeeping stuff (update vals)
    def create_new_pr_if_necessary(self):
        # get most recent page range from pr array
        cur_pr = self.page_ranges[self.cur_page_range_id]
        # check that this page range can still hold more page's
        # print("current num pages in pr: " + str(cur_pr.num_base_pages))
        # print("current page before cap check range id: " + str(self.cur_page_range_id))
        if not cur_pr.page_range_has_capacity():
            # self.actual_page_directory.append(cur_pr.end_rid_base)      # store the max rid into the array
            self.cur_page_range_id += 1  # this pr is full - update the pr id
            self.page_ranges.append(
                PageRange(self.cur_page_range_id, self.num_columns))  # add this new pr with new id to the PR list

        # need to reassign cur pr in case we created a new PR (should not ref to old one)
        cur_pr = self.page_ranges[self.cur_page_range_id]
        # increment the num pages count in either case (full or not full since we are adding a new page)
        cur_pr.num_base_pages += 1

