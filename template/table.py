from template.page import *
from template.page_range import *

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

    def create_tail_page(self, col, base_rid):
        # get the base_page that's getting updated rid (passed in as base_rid)
        # find the page range that base_page is in
        # add the tail page to that page range
        pr_id = (base_rid // 512)  # given the base_rid we can find the page range we want
        cur_pr = self.page_ranges[pr_id]

        # create the page and push to array holding pages
        new_page = Page()

        # add this tail page to the page range's tail list
        cur_pr.tail_pages.append(new_page)

        # keep track of index of page relative to array index
        if (len(cur_pr.free_tail_pages) < self.num_columns + 4):  # when initializing
            cur_pr.free_tail_pages.append(len(cur_pr.tail_pages) - 1)
            # self.free_tail_pages.append(len(self.tail_pages) - 1)
        else:  # when creating new page and need to update the index
            cur_pr.free_tail_pages[col] = len(cur_pr.tail_pages) - 1

        cur_pr.num_tail_pages += 1

    def update_tail_page(self, col, value, base_rid):
        pr_id = base_rid // 512  # given the base_rid we can find the page range we want
        # update the page linked to the col
        print(str(col) + " writing val: " + str(value) + " of type " + str(type(value)))
        cur_pr = self.page_ranges[pr_id]
        index_relative = cur_pr.free_tail_pages[col]
        # index_relative = self.free_tail_pages[col]
        pg = cur_pr.tail_pages[index_relative]
        error = pg.write(value)
        if error == -1:  # maximum size reached in page
            print("col:" + str(col) + " in page " + str(index_relative) + " is full, making a new one")

            # create new page
            page = Page()

            # write to new page
            page.write(value)
            # append the new page
            cur_pr.tail_pages.append(page)

            # add this tail page to the page range's tail list

            # update free page index to point to new blank page
            cur_pr.free_tail_pages[col] = len(cur_pr.tail_pages) - 1
            # self.free_pages[col].append(len(pages) - 1)
        if cur_pr.end_rid_tail == 0:
            cur_pr.start_rid_tail = self.tail_rid

        cur_pr.end_rid_tail = self.tail_rid

    def update_tail_rid(self, column_index, rid, value, base_rid):
        pr_id = base_rid // 512
        cur_pr = self.page_ranges[pr_id]
        if (column_index < 0 or column_index > self.num_columns):
            print("updating a rid in base " + str(column_index) + " out of bounds")
        print("updating tail rid " + str(rid) + " @ col " + str(column_index) + " with value: " + str(value))
        cur_pr.tail_pages[column_index].set_record(rid, value)

    def update_base_rid(self, column_index, rid, value):
        pr_id = rid // 512
        cur_pr = self.page_ranges[pr_id]
        if (column_index < 0 or column_index > self.num_columns):
            print("updating a rid in base " + str(column_index) + " out of bounds")
        print("updating rid " + str(rid) + " @ col " + str(column_index) + " with value: " + str(value))
        base_page_index = cur_pr.free_base_pages[column_index]
        cur_pr.base_pages[base_page_index].set_record(rid, value)

    def create_base_page(self, col_name):
        # check current PR can hold more
        self.create_new_pr_if_necessary()

        print("creating new page for " + str(col_name))
        # create the page and push to array holding pages
        new_page = Page()
        # also add page to the list of base pages in pr
        cur_pr = self.page_ranges[self.cur_page_range_id]
        cur_pr.base_pages.append(new_page)
        cur_pr.free_base_pages.append(len(cur_pr.base_pages) - 1)

    def update_base_page(self, index, value, rid):
        # update the page linked to the index
        pr_id = rid // 512
        # index_relative = self.free_base_pages[index]
        print("pr_id", pr_id)
        pr = self.page_ranges[pr_id]
        print("free pafge", pr.free_base_pages)
        index_relative = pr.free_base_pages[index]
        print("index_relative", index_relative)
        error = pr.base_pages[index_relative].write(value)
        # error = self.base_pages[index_relative].write(value)
        if error == -1:  # maximum size reached in page
            print("col:" + str(index) + " in page " + str(index_relative) + " is full, making a new one")
            # similar to above check if we have space in page range/create if necessary/update
            # self.create_new_pr_if_necessary()

            # create new page
            page = Page()

            # add new page to pr's base page list
            self.append_base_page_to_pr(page)

            # also add page to the list of base pages in pr
            page.write(value)
            pr.base_pages.append(page)
            pr.free_base_pages.append(len(self.base_pages) - 1)

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

