from template.page import *
from template.page_range import *
from template.index import *
from template.bp_manager import *
from template.counter import *
from template.lock_manager import *
import copy

from time import time


# ============== FOR PRINTING ==============
# Header Constants
PAGE_RANGE = "PAGE RANGE "
BASE_PAGES = "Base Pages"
TAIL_PAGE = "Tail Page "
INDIRECTION = "indirection"
RID = "RID"
TIME = "time"
SCHEMA = "schema"
TPS = "TPS"
BASE_RID = "Base RID"
KEY = "key"
G1 = "G1"
G2 = "G2"

def print_header_line(count):
    for j in range(count):
        print("_", end='')
    print()

#  ============== ============== ============== ==============

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
        self.index = Index(num_columns)
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

        # Aim for 20 * num records or else things will be too slow
        self.capacity = 20000
        # bufferpool size
        self.size = AtomicCounter()

        # BufferPoolManager
        self.base_page_manager = BufferPoolManager(self.num_columns + 5,
                                                    "base_pages.bin")
        self.tail_page_manager = BufferPoolManager(self.num_columns + 5,
                                                    "tail_pages.bin")

        # LockManager
        self.lock_manager = LockManager()

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

    def counters_to_int(self):
        self.size = self.size.value
        for pr in self.page_ranges:
            pr.make_count_serializable()

    def reset_counters(self):
        if isinstance(self.size, int):
            self.size = AtomicCounter(self.size)
        for pr in self.page_ranges:
            pr.reset_counter()



    def get_page_range(self, base_rid):
        pr_id = (base_rid // (PAGE_RANGE_MAX_RECORDS + 1))  # given the base_rid we can find the page range we want
        return self.page_ranges[pr_id]

    def merge(self, page_range):
        # print("MERGE!!")
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
        
        base_pages_copy = copy.deepcopy(page_range.base_pages)
        tps_page = base_pages_copy[TPS_COLUMN]  # Get TPS
        if tps_page == None:
            # Fetch the page from disk
            tps_page = self.base_page_manager.fetch(page_range.id_num, TPS_COLUMN)
            page_range.base_pages[TPS_COLUMN] = tps_page

        indirection_page = base_pages_copy[INDIRECTION_COLUMN]  # Get Indirection
        if indirection_page == None:
            # Fetch the page from disk
            indirection_page = self.base_page_manager.fetch(page_range.id_num, INDIRECTION_COLUMN)
            page_range.base_pages[INDIRECTION_COLUMN] = indirection_page

        # Get pages of columns that we need to read info from to perform merge
        rid_page = base_pages_copy[RID_COLUMN]  # Get RIDs
        if rid_page == None:
            # Fetch the page from disk
            rid_page = self.base_page_manager.fetch(page_range.id_num, RID_COLUMN)
            page_range.base_pages[RID_COLUMN] = rid_page

        # First RID in this page range
        start_rid = (page_range.id_num * PAGE_RANGE_MAX_RECORDS) + 1
        # Last RID in this page range
        end_rid = start_rid + rid_page.num_records - 1

        # Go through every row (every RID) --> i = RID
        for i in range(start_rid, end_rid + 1):
            offset = i - (PAGE_RANGE_MAX_RECORDS * page_range.id_num)
            rid_data = rid_page.get_record_int(offset)
            # print("looking into rid: " + str(rid_data))
            if rid_data != 0: # check if rid was not deleted
                indirection = indirection_page.get_record_int(offset)
                tps = tps_page.get_record_int(offset)

                # ------------------------- MERGE -------------------------
                # Only merge records that have had updates since last merge
                if indirection > tps:
                    # print("conidtion indir greater than tps")
                    # Get most recent values for this record
                    query_columns = []
                    for j in range(self.num_columns):
                        query_columns.append(1)
                    select_return = self.select(page_range, i, query_columns, 0, tps, base_pages_copy)
                    # Select's return format: [TPS#, columns[]]
                    new_tps = select_return[0]
                    columns = select_return[1]
                    # print("!!!!!!!!!!!!! columns " + str(columns))
                    # print(f"Data from select for RID {i}: {columns}")

                    # Update TPS and New Values
                    self.replace(offset, base_pages_copy, TPS_COLUMN, new_tps)
                    column_index = KEY_COLUMN     # index of the column that we are merging
                    for value in columns:
                        self.replace(offset, base_pages_copy, column_index, value)
                        column_index += 1

        # Update real base pages
        # [no   no    no     no    yes  yes yes yes]
        # [ 0    1     2      3     4    5   6   7 ]
        # [IND  RID  TIME  SCHEMA  TPS  KEY  G1  G2]
        start_col = NUM_CONSTANT_COLUMNS - 1        # 5-1 = 4
        end_col = start_col + self.num_columns      # 4+3 = 7
        for i in range(start_col, end_col + 1):
            for rid in range(start_rid, end_rid + 1):
                offset = rid - (PAGE_RANGE_MAX_RECORDS * page_range.id_num)
                if base_pages_copy[i] == None:
                    # Fetch the page from disk
                    base_pages_copy[i] = self.base_page_manager.fetch(page_range.id_num, i)

                value = base_pages_copy[i].get_record_int(offset)
                # Lock
                self.replace(offset, page_range.base_pages, i, value)
                # Unlock
        # deallocate base page copy
        base_pages_copy = None




    def has_capacity(self):
        return self.size.value <= self.capacity

    def evict_tail_page(self):
        """
        Uses bufferpool manager to evict the least recently used tail page
        """
        # Find a tail page to evict
        evictPair = self.tail_page_manager.find_evict()
        # 0 -> Disk Page Number
        # 1 -> Page Range ID
        if evictPair != None:
            # Write the tail page to disk if possible
            self.tail_page_manager.write_back(self.page_ranges[evictPair[1]].tail_pages,
            evictPair[0], evictPair[1])
            # Convert from disk page number to bufferpool page number
            page_num = evictPair[0] - (evictPair[1] * self.tail_page_manager.num_columns)
            # Remove the page from the bufferpool
            self.page_ranges[evictPair[1]].tail_pages[page_num] = None
            # Decrement number of pages in the bufferpool
            self.size.add(-1)
            return True
        return False

    def evict_base_page(self):
        """
        Uses bufferpool manager to evict the least recently used base page
        """
        # Find a base page to evict
        evictPair = self.base_page_manager.find_evict()
        # 0 -> Disk Page Number
        # 1 -> Page Range ID
        if evictPair != None:
            # Write the tail page to disk if possible
            self.base_page_manager.write_back(self.page_ranges[evictPair[1]].base_pages,
            evictPair[0], evictPair[1])
            # Convert from disk page number to bufferpool page number
            page_num = evictPair[0] - (evictPair[1] * self.base_page_manager.num_columns)
            # Remove the page from the bufferpool
            self.page_ranges[evictPair[1]].base_pages[page_num] = None
            # Decrement number of pages in the bufferpool
            self.size.add(-1)
            return True
        return False

    def check_need_evict(self):
        if self.has_capacity():
            return
        a = self.evict_base_page()
        b = self.evict_tail_page()
        assert(a or b), "Cannot evict anything"
    # call example: self.replace(i, base_pages_copy, TPS_COLUMN, new_tps)
    def replace(self, rid, base_pages, column_index, value):
        base_page = base_pages[column_index]
        base_page.set_record(rid, value)


    # Change so that don't start at very bottom, but rather start at merge point
    def select(self, page_range, rid, query_columns, start_TID, stop_TID, base_pages):
        # print(f"----------------------------------- select -----------------------------------")
        # get relative rid to new page range since it starts at 0
        offset = rid - (PAGE_RANGE_MAX_RECORDS * page_range.id_num)

        # Get and check indirection
        indirection_page = base_pages[INDIRECTION_COLUMN]
        if indirection_page == None:
            # Fetch the page from disk
            indirection_page = self.base_page_manager.fetch(page_range.id_num, INDIRECTION_COLUMN)
            page_range.base_pages[INDIRECTION_COLUMN] = indirection_page


        indirection_data = indirection_page.get_record_int(offset)
        if indirection_data != 0:
            tail_page_indices = self.tail_page_directory[indirection_data]

        # Get schema
        schema_page = base_pages[SCHEMA_ENCODING_COLUMN]
        if schema_page == None:
            # Fetch the page from disk
            schema_page = self.base_page_manager.fetch(page_range.id_num, SCHEMA_ENCODING_COLUMN)
            page_range.base_pages[SCHEMA_ENCODING_COLUMN] = schema_page



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
                if base_page == None:
                    # Fetch the page from disk
                    base_page = self.base_page_manager.fetch(page_range.id_num, column_index)
                    page_range.base_pages[column_index] = base_page


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
                if tail_page == None:
                    # Fetch the page from disk
                    tail_page = self.tail_page_manager.fetch(page_range.id_num, tail_page_index)
                    page_range.tail_pages[tail_page_index] = tail_page

                # print("tail_page size", tail_page.num_records, "offset", tail_page_offset)
                tail_data = tail_page.get_record_int(tail_page_offset)

                # Get TPS
                tps_tail_page_index_offset_tuple = tail_page_indices[TPS_COLUMN]
                tps_tail_page_index = tps_tail_page_index_offset_tuple[0]
                tps_tail_page_offset = tps_tail_page_index_offset_tuple[1]
                tps_tail_page = page_range.tail_pages[tps_tail_page_index]
                if tps_tail_page == None:
                    # Fetch the page from disk
                    tps_tail_page = self.tail_page_manager.fetch(page_range.id_num, tps_tail_page_index)
                    page_range.tail_pages[tps_tail_page_index] = tps_tail_page

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
                        if indirection_page == None:
                            # Fetch the page from disk
                            indirection_page = self.tail_page_manager.fetch(page_range.id_num, indirection_index)
                            page_range.tail_pages[indirection_index] = indirection_page

                        indirection_value = indirection_page.get_record_int(indirection_offset)

                        # Break if we reached last merge
                        if indirection_value == stop_TID:
                            break
                        column_tuple = self.tail_page_directory[indirection_value][column_index]
                        offset_exists = column_tuple[1]

                    if (offset_exists != 0):  # there exists something in this page
                        correct_tail_page = self.tail_page_directory[indirection_value][column_index]
                        tail_page = page_range.tail_pages[correct_tail_page[0]]
                        if tail_page == None:
                            # Fetch the page from disk
                            tail_page = self.tail_page_manager.fetch(page_range.id_num, correct_tail_page[0])
                            page_range.tail_pages[correct_tail_page[0]] = tail_page


                        tail_data = tail_page.get_record_int(correct_tail_page[1])
                        # print("correct tail page data is in index",correct_tail_page[0],correct_tail_page[1])

                        # Get TPS from same TPS page at same offset that tail_data is coming from
                        correct_tps_tail_page = self.tail_page_directory[indirection_value][TPS_COLUMN]
                        tps_tail_page = page_range.tail_pages[correct_tps_tail_page[0]]
                        if tps_tail_page == None:
                            # Fetch the page from disk
                            tps_tail_page = self.tail_page_manager.fetch(page_range.id_num, correct_tps_tail_page[0])
                            page_range.tail_pages[correct_tps_tail_page[0]] = tps_tail_page

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

        # Check if we have room for the new page
        self.check_need_evict()

        # create the page and push to array holding pages
        new_page = Page()
        # Check if we have room for the new page

        # add this tail page to the page range's tail list
        cur_pr.tail_pages.append(new_page)
        self.size.add(1)

        # keep track of index of page relative to array index
        if (len(cur_pr.free_tail_pages) < self.num_columns + 5):  # when initializing
            cur_pr.free_tail_pages.append(len(cur_pr.tail_pages) - 1)
            # self.free_tail_pages.append(len(self.tail_pages) - 1)
        else:  # when creating new page and need to update the index
            cur_pr.free_tail_pages[col] = len(cur_pr.tail_pages) - 1

        # Add page to lru
        self.tail_page_manager.update_page_usage(cur_pr.id_num, len(cur_pr.tail_pages) - 1)
        self.tail_page_manager.set_page_dirty(cur_pr.id_num, len(cur_pr.tail_pages) - 1)

        cur_pr.num_tail_pages += 1

    def append_tail_page_record(self, col, value, base_rid):
        # update the page linked to the col
        # print(str(col) + " writing val: " + str(value) + " of type " + str(type(value)))
        cur_pr = self.get_page_range(base_rid)
        index_relative = cur_pr.free_tail_pages[col]
        # index_relative = self.free_tail_pages[col]
        cur_page = cur_pr.tail_pages[index_relative]
        # If page isn't in bufferpool
        if cur_page == None:
            # If we don't have enough space to bring in another page
            self.check_need_evict()

            # Fetch the page from disk
            cur_page = self.tail_page_manager.fetch(cur_pr.id_num, index_relative)
            self.page_ranges[pr_id].tail_pages[index_relative] = cur_page
            self.size.add(1)

        # Pin the current Page
        self.tail_page_manager.pin(cur_pr.id_num, index_relative)
        error = cur_page.write(value)
        if error == -1:  # maximum size reached in page
            # Check if we have room for the new page
            self.check_need_evict()
            # create new page
            page = Page()
            self.size.add(1)

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

        # Page is dirty
        self.tail_page_manager.set_page_dirty(cur_pr.id_num, index_relative)

        # update lru
        self.tail_page_manager.update_page_usage(cur_pr.id_num, index_relative)

        # Unpin the current Page
        self.tail_page_manager.unpin(cur_pr.id_num, index_relative)

    def update_tail_rid(self, column_index, rid, value, base_rid):
        pr_id = base_rid // (PAGE_RANGE_MAX_RECORDS + 1)
        cur_pr = self.page_ranges[pr_id]
        cur_page = cur_pr.tail_pages[column_index]

        # If page isn't in bufferpool
        if cur_page == None:
            # If we don't have enough space to bring in another page
            self.check_need_evict()

            cur_page = self.tail_page_manager.fetch(cur_pr.id_num,
                                                    column_index)
            self.page_ranges[pr_id].tail_pages[column_index] = cur_page
            self.size.add(1)

        # Pin the current Page
        self.tail_page_manager.pin(cur_pr.id_num, column_index)
        # Update LRU
        self.tail_page_manager.update_page_usage(cur_pr.id_num, column_index)
        # Page is dirty
        self.tail_page_manager.set_page_dirty(cur_pr.id_num, column_index)
        # Update the page
        cur_page.set_record(rid, value)
        # Unpin the current Page
        self.tail_page_manager.unpin(cur_pr.id_num, column_index)

    def update_base_rid(self, column_index, rid, value):
        pr_id = rid // (PAGE_RANGE_MAX_RECORDS + 1)
        cur_pr = self.page_ranges[pr_id]
        # if (column_index < 0 or column_index > self.num_columns):
        # print("updating a rid in base " + str(column_index) + " out of bounds")
        # print("updating rid " + str(rid) + " @ col " + str(column_index) + " with value: " + str(value))
        base_page_index = cur_pr.free_base_pages[column_index]
        cur_page = cur_pr.base_pages[base_page_index]
        # If page isn't in bufferpool
        if cur_page == None:
            # If we don't have enough space to bring in another page
            self.check_need_evict()

            cur_page = self.base_page_manager.fetch(cur_pr.id_num, base_page_index)
            self.page_ranges[pr_id].base_pages[base_page_index] = cur_page
            self.size.add(1)

        # Pin the page
        self.base_page_manager.pin(cur_pr.id_num, base_page_index)
        # Update LRU
        self.base_page_manager.update_page_usage(cur_pr.id_num, base_page_index)
        # Page is dirty
        self.base_page_manager.set_page_dirty(cur_pr.id_num, base_page_index)
        # Unpin the page
        self.base_page_manager.unpin(cur_pr.id_num, base_page_index)
        # Get the record's offset
        base_offset = rid - (PAGE_RANGE_MAX_RECORDS * pr_id)
        # Set the record's value
        cur_page.set_record(base_offset, value)

    def create_base_page(self, col_name):
        # Get the current page range
        cur_pr = self.page_ranges[self.cur_page_range_id]

       # Check if we have room for the new page
        self.check_need_evict()

        # check current PR can hold more
        self.create_new_pr_if_necessary()

        # print("creating new page for " + str(col_name))
        # create the page and push to array holding pages
        new_page = Page()
        self.size.add(1)
        # also add page to the list of base pages in pr
        cur_pr = self.page_ranges[self.cur_page_range_id]
        cur_pr.base_pages.append(new_page)
        cur_pr.free_base_pages.append(len(cur_pr.base_pages) - 1)
        # Update LRU
        self.base_page_manager.update_page_usage(self.cur_page_range_id,
                                            len(cur_pr.base_pages) - 1)
        self.base_page_manager.set_page_dirty(self.cur_page_range_id,
                                            len(cur_pr.base_pages) - 1)

    def append_base_page_record(self, index, value, rid):
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
        cur_page = pr.base_pages[index_relative]
        # If page isn't in bufferpool
        if cur_page == None:
            # If we don't have enough space to bring in another page
            self.check_need_evict()

            # Fetch the page
            cur_page = self.base_page_manager.fetch(pr.id_num, index_relative)
            self.page_ranges[pr_id].base_pages[index_relative] = cur_page
            self.size.add(1)

        # Pin the page
        self.base_page_manager.pin(pr.id_num, index_relative)
        error = cur_page.write(value)
        if error == -1:  # maximum size reached in page
            # Check if we have room for the new page
            self.check_need_evict()
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
            self.size.add(1)

        # Page is dirty
        self.base_page_manager.set_page_dirty(pr.id_num, index_relative)

        # Update LRU
        self.base_page_manager.update_page_usage(pr.id_num, index_relative)

        # Unpin the page
        self.base_page_manager.unpin(pr.id_num, index_relative)
        # print("current page range: " + str(cur_pr_id_num))

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

