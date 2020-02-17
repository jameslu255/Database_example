from template.page import *
from template.page_range import *

from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

TPS_COLUMN = 4
BASE_RID_COLUMN = 4

KEY_COLUMN = 5


PAGE_RANGE_MAX_RECORDS = 512





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
        self.create_base_page("tps")  # index 4

        # create pages for the key and the data columns
        for x in range(num_columns):
            self.create_base_page(x)


    def get_page_range(self, base_rid):
        pr_id = (base_rid // (PAGE_RANGE_MAX_RECORDS + 1))  # given the base_rid we can find the page range we want
        return self.page_ranges[pr_id]



    def __merge(self, page_range):

        # GAME PLAN:
        # Make a copy of the base pages
        # Go through every row (every RID) in the base pages
        # For each row:
        #     Check indirection (if indirection > TPS: merge for this row)
        #     Go to tail RID (from indirection)
        #     Check BASE_RID (to not waste time)
        #     Backtrack until you get all most recent updates for all columns for that row OR until you hit last merge (check TPS for last merge)

        # Copy base pages
        base_pages_copy = page_range.base_pages.copy()

        # Get first RID in this page range (just to access base page indices)
        rid = page_range.id_num * PAGE_RANGE_MAX_RECORDS

        # Find physical pages' indices for RID from page_directory [RID:[x x x x x]]
        base_page_indices = self.table.base_page_directory[rid]
        # print(f"Found base pages: {base_page_indices}")

        # ----- Get a bunch of columns that we need to read info from to perform merge -----
        # Get RIDs
        rid_page_index = base_page_indices[RID_COLUMN]
        rid_page = page_range.base_pages[rid_page_index]
        # Get Indirection
        indirection_page_index = base_page_indices[INDIRECTION_COLUMN]
        indirection_page = page_range.base_pages[indirection_page_index]
        # Get TPS
        tps_page_index = base_page_indices[TPS_COLUMN]
        tps_page = page_range.base_pages[tps_page_index]
        # Get keys
        key_page_index = base_page_indices[KEY_COLUMN]
        key_page = page_range.base_pages[key_page_index]

        # Get the number of rows in this page range
        num_rows = key_page.num_records

        # Go through every row (every RID)
        for i in range(rid, num_rows + 1):
            rid_data = rid_page.get_record_int(i)
            if rid_data != 0:
                indirection = indirection_page.get_record_int(i)
                tps = tps_page.get_record_int(i)
                if indirection > tps:
                    # MERGE

        pass



    def create_tail_page(self, col, base_rid):
        # get the base_page that's getting updated rid (passed in as base_rid)
        # find the page range that base_page is in
        # add the tail page to that page range
        cur_pr = self.get_page_range(base_rid)

        # create the page and push to array holding pages
        new_page = Page()

        # add this tail page to the page range's tail list
        cur_pr.tail_pages.append(new_page)

        # keep track of index of page relative to array index
        if (len(cur_pr.free_tail_pages) < self.num_columns + 5):  # when initializing
            cur_pr.free_tail_pages.append(len(cur_pr.tail_pages) - 1)
            # self.free_tail_pages.append(len(self.tail_pages) - 1)
        else:  # when creating new page and need to update the index
            cur_pr.free_tail_pages[col] = len(cur_pr.tail_pages) - 1

        cur_pr.num_tail_pages += 1



    def update_tail_page(self, col, value, base_rid):
        # update the page linked to the col
        # print(str(col) + " writing val: " + str(value) + " of type " + str(type(value)))
        cur_pr = self.get_page_range(base_rid)
        index_relative = cur_pr.free_tail_pages[col]
        # index_relative = self.free_tail_pages[col]
        pg = cur_pr.tail_pages[index_relative]
        error = pg.write(value)
        if error == -1:  # maximum size reached in page
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



    def update_tail_rid(self, column_index, rid, value, base_rid):
        pr_id = base_rid // (PAGE_RANGE_MAX_RECORDS + 1)
        cur_pr = self.page_ranges[pr_id]
        # if (column_index < 0):
            # print("updating a rid in tail " + str(column_index) + " out of bounds")
        # print("updating tail rid " + str(rid) + " @ col " + str(column_index) + " with value: " + str(value))
        cur_pr.tail_pages[column_index].set_record(rid, value)

    def update_base_rid(self, column_index, rid, value):
        pr_id = rid // (PAGE_RANGE_MAX_RECORDS + 1)
        cur_pr = self.page_ranges[pr_id]
        # if (column_index < 0 or column_index > self.num_columns):
            # print("updating a rid in base " + str(column_index) + " out of bounds")
        # print("updating rid " + str(rid) + " @ col " + str(column_index) + " with value: " + str(value))
        base_page_index = cur_pr.free_base_pages[column_index]
        base_offset = rid - (PAGE_RANGE_MAX_RECORDS * pr_id)
        cur_pr.base_pages[base_page_index].set_record(base_offset, value)


    def create_base_page(self, col_name):
        # check current PR can hold more
        self.create_new_pr_if_necessary()

        # print("creating new page for " + str(col_name))
        # create the page and push to array holding pages
        new_page = Page()
        # also add page to the list of base pages in pr
        cur_pr = self.page_ranges[self.cur_page_range_id]
        cur_pr.base_pages.append(new_page)
        cur_pr.free_base_pages.append(len(cur_pr.base_pages) - 1)



    def update_base_page(self, index, value, rid):
        # print("updating col", index, "with", value, "for rid", rid)
        # update the page linked to the index
        pr_id = rid // (PAGE_RANGE_MAX_RECORDS + 1)
        # index_relative = self.free_base_pages[index]
        # print("pr_id", pr_id)
        
        if pr_id >= len(self.page_ranges): #no new page range
            # make new page range
            # print("making new pange range")
            self.cur_page_range_id += 1  # this pr is full - update the pr id
            new_pr = PageRange(self.cur_page_range_id, self.num_columns)
            self.page_ranges.append(new_pr)  # add this new pr with new id to the PR list
            # initialize base pages on new pange range creation
            for x in range(self.num_columns + 5):
                self.create_base_page(x)
            
        pr = self.page_ranges[pr_id]
        index_relative = pr.free_base_pages[index]
        error = pr.base_pages[index_relative].write(value)
        # error = self.base_pages[index_relative].write(value)
        if error == -1:  # maximum size reached in page
            # similar to above check if we have space in page range/create if necessary/update
            # create new page
            page = Page()

            # self.append_base_page_to_pr(page)
            # also add page to the list of base pages in pr
            page.write(value)
            pr.base_pages.append(page)
            pr.free_base_pages.append(len(pr.base_pages) - 1)
            
            # increment the num pages count in either case (full or not full since we are adding a new page)
            pr.num_base_pages += 1



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

