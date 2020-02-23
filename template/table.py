from template.page import *
from template.page_range import *

from time import time

# Column Indices Constants
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
TPS_COLUMN = 4
BASE_RID_COLUMN = 4

# Number of constant columns
NUM_CONSTANT_COLUMNS = 5
# Key column is always 6th column (first column before data columns)
KEY_COLUMN = 5
# Maximum number of records a page range can hold
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
        self.create_base_page("indirection")    # index 0
        self.create_base_page("rid")            # index 1
        self.create_base_page("timestamp")      # index 2
        self.create_base_page("schema")         # index 3
        self.create_base_page("tps")            # index 4

        # create pages for the key and the data columns
        for x in range(num_columns):
            self.create_base_page(x)

    def get_page_range(self, base_rid):
        pr_id = (base_rid // (PAGE_RANGE_MAX_RECORDS + 1))  # given the base_rid we can find the page range we want
        return self.page_ranges[pr_id]

    def merge(self, page_range):

        # GAME PLAN:

        # 1. Identify committed tail records in tail pages
        #    What:  Select a set of consecutive fully committed tail records (or pages) since the last merge within each update range.
        #    How: Create a queue to hold committed tail records (or pages) then use while loop to merge while queue is not empty.
        # 2. Load the corresponding outdated base pages
        #    What:
        #    How:
        # 3. Consolidate the base and tail pages (MERGE)
        # 4. Update the page directory
        # 5. De-allocate the outdated base pages

        # NOTES:

        # 1. Writers append new uncommitted tail records to tail pages,
        # but as stated before uncommitted records do not participate in the merge.

        # 2. Writers also perform in-place update of the Indirection column within base
        # records to point to the latest version of the updated records in tail pages,
        # but the Indirection column is not modified by the merge process

        # GAME PLAN:
        # Make a copy of the base pages
        # Go through every row (every RID) in the base pages
        # For each row:
        #     Check indirection (if indirection > TPS: merge for this row)
        #     Merge:
        #         Call select to obtain most recent updates
        #         Modify base pages copy

        # Copy base pages
        # [ 0    1     2      3     4    5   6   7 ]
        # [IND  RID  TIME  SCHEMA  TPS  KEY  G1  G2]
        base_pages_copy = page_range.base_pages.copy()

        # ----- Get a bunch of columns that we need to read info from to perform merge -----
        rid_page = base_pages_copy[RID_COLUMN]  # Get RIDs
        indirection_page = base_pages_copy[INDIRECTION_COLUMN]  # Get Indirection
        tps_page = base_pages_copy[TPS_COLUMN]  # Get TPS
        key_page = base_pages_copy[KEY_COLUMN]  # Get keys

        # First RID in this page range
        start_rid = page_range.id_num * PAGE_RANGE_MAX_RECORDS
        # Get the number of rows in this page range
        num_rows = rid_page.num_records
        # Last RID in this page range
        end_rid = start_rid + num_rows

        # # Find physical pages' indices for RID from page_directory [RID:[x x x x x]]
        # base_page_indices = self.table.base_page_directory[start_rid]

        # Go through every row (every RID) --> i = RID
        for i in range(start_rid, end_rid + 1):
            rid_data = rid_page.get_record_int(i)
            if rid_data != 0:
                indirection = indirection_page.get_record_int(i)
                tps = tps_page.get_record_int(i)

                # MERGE
                if indirection > tps: # if indirection !> tps --> no need to merge this record (hasn't had new updates)

                    # ----- Set Up to call select -----
                    key = key_page.get_record_int(i)
                    query_columns = []
                    for j in range(self.num_columns):
                        query_columns.append(1)

                    # record = [TPS, Record(rid, key, columns)] --> always 2 items in select return array
                    select_return = self.select_two(page_range, i, query_columns, 0, tps, base_pages_copy)
                    new_tps = select_return[0]
                    columns = select_return[1]

                    # Update TPS
                    self.replace(i, base_pages_copy, TPS_COLUMN, new_tps)

                    # Put new values into base pages copy
                    # columns in record object contains the data we want
                    # column_index is the index of the column that we are merging into
                    # (may need to change to +2 if select doesnt include key in return)
                    column_index = NUM_CONSTANT_COLUMNS + 1
                    for value in columns:
                        self.replace(i, base_pages_copy, column_index, value)
                        column_index += 1

        # Update real base pages
        # Free tail pages
        # Update tail directory
        pass

    # call example: self.replace(i, base_pages_copy, TPS_COLUMN, new_tps)
    def replace(self, rid, base_pages_copy, column_index, value):
        # use rid to find the offset of the item within the page to replace
        pr_id = self.get_page_range(rid).id_num
        offset = rid - (PAGE_RANGE_MAX_RECORDS * pr_id)
        # base_pages_copy[column][offset] = value
        self.update_base_page(self, column_index, value, rid)

    # Change so that don't start at very bottom, but rather start at merge point
    def select_two(self, page_range, rid, query_columns, start_TID, stop_TID, base_pages):
        # get relative rid to new page range since it starts at 0
        offset = rid - (PAGE_RANGE_MAX_RECORDS * page_range.id_num)

        # Get and check indirection
        indirection_page = base_pages[INDIRECTION_COLUMN]
        indirection_data = indirection_page.get_record_int(offset)
        if indirection_data != 0:
            tail_page_indices = self.tail_page_directory[indirection_data]

        # Get schema
        schema_page = base_pages[SCHEMA_ENCODING_COLUMN]
        schema_data_int = schema_page.get_record_int(offset)

        # Get desired columns' page indices
        data = []
        tps = 0
        columns = []
        for i in range(len(query_columns)):
            column_index = i + NUM_CONSTANT_COLUMNS
            # Check schema (base page or tail page? --> Has been updated before?)
            has_prev_tail_pages = self.bit_is_set(column_index, schema_data_int)

            # If base page
            if query_columns[i] == 1 and not has_prev_tail_pages:
                base_page = base_pages[column_index]
                base_data = base_page.get_record_int(offset)
                # print("index",i,"appending base data", base_data)
                columns.append(base_data)
                # print(f"Column {i+5} -> Base Page Index: {base_page_index} -> Data: {base_data}")

            # If tail page
            elif query_columns[i] == 1 and has_prev_tail_pages:
                # get tail page value of this column
                # grab index and offset of this tail page
                tail_page_index_offset_tuple = tail_page_indices[column_index]
                # print(f"tail_page (page index, offset): {tail_page_index_offset_tuple}")
                tail_page_index = tail_page_index_offset_tuple[0]
                tail_page_offset = tail_page_index_offset_tuple[1]
                tail_page = page_range.tail_pages[tail_page_index]
                # print("tail_page size", tail_page.num_records, "offset", tail_page_offset)
                tail_data = tail_page.get_record_int(tail_page_offset)

                # Get TPS
                tps_tail_page_index_offset_tuple = tail_page_indices[TPS_COLUMN]
                tps_tail_page_index = tps_tail_page_index_offset_tuple[0]
                tps_tail_page_offset = tps_tail_page_index_offset_tuple[1]
                tps_tail_page = page_range.tail_pages[tps_tail_page_index]
                tps_tail_data = tps_tail_page.get_record_int(tps_tail_page_offset)

                if (tail_page_offset == 0):
                    # we are in the right column, but the wrong tail page associated with it (spanning new tail pages every time)
                    offset_exists = tail_page_offset
                    indirection_value = indirection_data
                    while (offset_exists == 0):  # while the current tail page doesn't have a value
                        tp_dir = self.tail_page_directory[indirection_value]
                        indirection_index = tp_dir[INDIRECTION_COLUMN][0]
                        indirection_offset = tp_dir[INDIRECTION_COLUMN][1]
                        indirection_page = page_range.tail_pages[indirection_index]
                        indirection_value = indirection_page.get_record_int(indirection_offset)

                        # Break if we reached last merge
                        if indirection_value == stop_TID:
                            break
                        column_tuple = self.tail_page_directory[indirection_value][column_index]
                        offset_exists = column_tuple[1]

                    if (offset_exists != 0):  # there exists something in this page
                        correct_tail_page = self.tail_page_directory[indirection_value][column_index]
                        tail_page = page_range.tail_pages[correct_tail_page[0]]
                        tail_data = tail_page.get_record_int(correct_tail_page[1])
                        # print("correct tail page data is in index",correct_tail_page[0],correct_tail_page[1])

                        # Get TPS from same TPS page at same offset that tail_data is coming from
                        correct_tps_tail_page = self.tail_page_directory[indirection_value][TPS_COLUMN]
                        tps_tail_page = page_range.tail_pages[correct_tps_tail_page[0]]
                        tps_tail_data = tps_tail_page.get_record_int(correct_tps_tail_page[1])

                # Append found most recent data to columns
                columns.append(tail_data)
                # Find most recent update TPS
                current_tps = tps_tail_data
                if current_tps > tps:
                    tps = current_tps
        # [TPS_value, [data_value1 data_value2 data_value3 ...]]
        data.append(tps)
        data.append(columns)
        return data


    def bit_is_set(self, column, schema_enc):
        mask = 1 << (NUM_CONSTANT_COLUMNS + self.num_columns - column - 1)
        return schema_enc & mask > 0

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

        if pr_id >= len(self.page_ranges):  # no new page range
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
