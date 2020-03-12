from template.table import *
from template.lock_manager import *
from template.index import Index
from template.logger import Logger
import time
import threading
import re


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    """
    abort_sem = threading.Semaphore()

    def __init__(self, table):
        self.table = table
        self.logger = Logger("log")

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon successful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, key, txn_id=0, abort=False):
        if abort:
            Query.abort_sem.acquire()

        # grab the old value for recovery purposes
        old_val = self.select(key, 0, [1] * (self.table.num_columns - 1))
        if old_val == False:
            return False
        else:
            old_val = old_val[0].columns

        # get base rid
        # base_rid = self.table.keys[key]
        rids = self.table.index.get_value(0, key)

        record = self.select(key, 0, [1, 1, 1, 1, 1])
        if record == False:
            old_val = ["None"]
            return False
        else:
            record = record[0]
        # print("record columns are : " + str(record.columns))

        for base_rid in rids:
            didAcquireLock = self.table.lock_manager.acquire(base_rid, 'W')
            if not didAcquireLock and abort == False:
                self.abort(txn_id)
                return False

            # grab current page range 
            # pr_id = rid_base // (max_page_size / 8)
            pr_id = base_rid // (PAGE_RANGE_MAX_RECORDS + 1)
            with self.table.page_range_lock:
                cur_pr = self.table.page_ranges[pr_id]

            # get relative rid to new page range since it starts at 0
            offset = base_rid - (PAGE_RANGE_MAX_RECORDS * pr_id)


            with self.table.page_range_lock:
                # set rid to invalid value - 0
                self.table.update_base_rid(RID_COLUMN, base_rid, 0)
                # check if there are tail pages to invalidate as well
                # get indirection value of the base page
                indirection_index = self.table.base_page_directory[base_rid][INDIRECTION_COLUMN]
                indirection_page = cur_pr.base_pages[indirection_index]
                # Pin the current page
                self.table.base_page_manager.pin(cur_pr.id_num.value, indirection_index)
                # If page is not in bufferpool, read from disk
                if (indirection_page == None):
                    # if no space for new page
                    self.table.check_need_evict()

                    # Fetch page from disk
                    indirection_page = self.table.base_page_manager.fetch(cur_pr.id_num.value, indirection_index)
                    cur_pr.base_pages[indirection_index] = indirection_page
                    self.table.size.add(1)

            indirection_value = indirection_page.get_record_int(offset)

            if indirection_value == 0:
                return old_val  # return the old value for recovery purposes

            # Unpin the current page
            self.table.base_page_manager.unpin(cur_pr.id_num.value, indirection_index)
            self.table.base_page_manager.update_page_usage(cur_pr.id_num.value, indirection_index)
            with self.table.page_range_lock:
                # index arithmetic to find the latest tail page and its predecessors
                rid_index = self.table.tail_page_directory[indirection_value][RID_COLUMN][0]  # index part of tuple

                while (indirection_value != 0):  # there are update(s) to this rid
                    # set tail rid to invalid 0 of the tail record
                    rid_offset = self.table.tail_page_directory[indirection_value][RID_COLUMN][1]  # offset
                    self.table.update_tail_rid(rid_index, rid_offset, 0, base_rid)

                    # get the tail record indirection value
                    indirection_index = self.table.tail_page_directory[indirection_value][INDIRECTION_COLUMN][0]  # index
                    indirection_offset = self.table.tail_page_directory[indirection_value][INDIRECTION_COLUMN][1]  # offset
                    # indirection_page = self.table.tail_pages[indirection_index]
                    indirection_page = cur_pr.tail_pages[indirection_index]
                    self.table.tail_page_manager.pin(cur_pr.id_num.value, indirection_index)
                    # If page is not in bufferpool, read from disk
                    if (indirection_page == None):
                        # if no space for new page
                        self.table.check_need_evict()

                        # Fetch page from disk
                        indirection_page = self.table.tail_page_manager.fetch(cur_pr.id_num.value, indirection_index)
                        cur_pr.tail_pages[indirection_index] = indirection_page
                        self.table.size.add(1)

                    indirection_value = indirection_page.get_record_int(indirection_offset)
                    self.table.tail_page_manager.unpin(cur_pr.id_num.value, indirection_index)
                    self.table.tail_page_manager.update_page_usage(cur_pr.id_num.value, indirection_index)
                    # update index to find the previous page for this column
                    rid_index -= (NUM_CONSTANT_COLUMNS + self.table.num_columns)

        self.table.index.remove_values(0, key, list(rids)[0])
        for i in range(len(record.columns)):
            if i != 0:
                self.table.index.remove_values(i, record.columns[i], list(rids)[0])

        self.table.lock_manager.release(base_rid, 'W')
        if txn_id > 0:
            self.logger.write(txn_id, "delete", old_val.columns, None, key)

        if abort:
            Query.abort_sem.release()
        return True

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns, txn_id=0, abort=False):
        if abort:
            Query.abort_sem.acquire()

        self.table.base_rid.add(1)
        rid = self.table.base_rid.value
        didAcquireLock = self.table.lock_manager.acquire(rid, 'W')
        if not didAcquireLock and abort == False:
            self.abort(txn_id)
            return False

        page_directory_indexes = []
        # record = Record(self.table.base_rid, columns[0], columns)
        key = columns[0]
        schema_encoding = 0  # '0' * self.table.num_columns
        timestamp = int(time.time())
        indirection = 0  # None

        # Write to the page
        with self.table.page_range_lock:
            self.table.append_base_page_record(INDIRECTION_COLUMN, indirection, rid)
            self.table.append_base_page_record(RID_COLUMN, rid, rid)
            self.table.append_base_page_record(TIMESTAMP_COLUMN, timestamp, rid)
            self.table.append_base_page_record(SCHEMA_ENCODING_COLUMN, schema_encoding, rid)
            self.table.append_base_page_record(TPS_COLUMN, 0, rid)

        # add each column's value to the respective page
        for x in range(len(columns)):
            with self.table.page_range_lock:
                self.table.append_base_page_record(x + NUM_CONSTANT_COLUMNS, columns[x], rid)
            if x != 0:
                # subtract 1 from x because we want to start with assignment 1
                self.table.index.add_values(x, columns[x], rid)
            else:
                self.table.index.create_dictionary(x, columns[x], rid)

        # grab current page range 
        # pr_id = rid_base // (max_page_size / 8)
        pr_id = rid // (PAGE_RANGE_MAX_RECORDS + 1)
        # print("pr_id", pr_id)
        with self.table.page_range_lock:
            cur_pr = self.table.page_ranges[pr_id]

            # SID -> RID
            self.table.keys[key] = rid
            # RID -> page_index
            for x in range(len(columns) + NUM_CONSTANT_COLUMNS):
                # page_directory_indexes.append(self.table.free_base_pages[x])
                page_directory_indexes.append(cur_pr.free_base_pages[x])
            self.table.base_page_directory[rid] = page_directory_indexes

        self.table.lock_manager.release(rid, 'W')
        if (txn_id > 0):
            self.logger.write(txn_id, "insert", [0] * (self.table.num_columns - 1), columns[1:], columns[0])

        if abort:
            Query.abort_sem.release()

        return True

    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    # add for loop to run it again multiple times with key being score and given a column number.
    def select(self, key, column, query_columns, txn_id=0, abort=False):
        print(f"----------------------------------- select -----------------------------------")
        if abort:
            Query.abort_sem.acquire()
        # if key not in self.table.keys:
        #    return []

        # Find RID from key, keys = {SID: RID}
        # rid = self.table.keys[key]
        # start for loop here.'
        record = []
        rids = self.table.index.locate(key, column)
        for rid in rids:
            if (rid == "F"):
                return []
            didAcquireLock = self.table.lock_manager.acquire(rid, 'R')
            if not didAcquireLock and abort == False:
                self.abort(txn_id)
                return False
            # Find Page Range ID
            pr_id = rid // (PAGE_RANGE_MAX_RECORDS + 1)

            with self.table.page_range_lock:
                page_range = self.table.page_ranges[pr_id]

                # get relative rid to new page range since it starts at 0
                offset = rid - (PAGE_RANGE_MAX_RECORDS * pr_id)

                # Find physical pages' indices for RID from page_directory [RID:[x x x x x]]
                base_page_indices = self.table.base_page_directory[rid]
                # print(f"Found base pages: {base_page_indices}")

                # Get and check indirection
                indirection_page_index = base_page_indices[INDIRECTION_COLUMN]
                indirection_page = page_range.base_pages[indirection_page_index]
                self.table.base_page_manager.pin(pr_id, indirection_page_index)
                if (indirection_page == None):
                    # if no space for new page
                    self.table.check_need_evict()
                    # Fetch page from disk
                    indirection_page = self.table.base_page_manager.fetch(pr_id, indirection_page_index)
                    self.table.page_ranges[pr_id].base_pages[indirection_page_index] = indirection_page
                    self.table.size.add(1)

            # Pin the page
            self.table.base_page_manager.pin(pr_id, indirection_page_index)
            indirection_data = indirection_page.get_record_int(offset)
            # Unpin the page
            self.table.base_page_manager.unpin(pr_id, indirection_page_index)
            self.table.base_page_manager.update_page_usage(pr_id, indirection_page_index)

            with self.table.page_range_lock:
                if indirection_data != 0:
                    tail_page_indices = self.table.tail_page_directory[indirection_data]
                # Get schema
                schema_page_index = base_page_indices[SCHEMA_ENCODING_COLUMN]
                schema_page = page_range.base_pages[schema_page_index]
                # Pin the page
                self.table.base_page_manager.pin(pr_id, schema_page_index)
                if (schema_page == None):
                    # if no space for new page
                    self.table.check_need_evict()
                    # Fetch page from disk
                    schema_page = self.table.base_page_manager.fetch(pr_id, schema_page_index)
                    self.table.page_ranges[pr_id].base_pages[schema_page_index] = schema_page
                    self.table.size.add(1)

            schema_data_int = schema_page.get_record_int(offset)

            # Unpin the page
            self.table.base_page_manager.unpin(pr_id, schema_page_index)
            self.table.base_page_manager.update_page_usage(pr_id, schema_page_index)


            with self.table.page_range_lock:
                # Get TPS column
                tps_page_index = base_page_indices[TPS_COLUMN]
                tps_page = page_range.base_pages[tps_page_index]
                # Pin the page
                self.table.base_page_manager.pin(pr_id, tps_page_index)
                if (tps_page == None):
                    # if no space for new page
                    self.table.check_need_evict()
                    # Fetch page from disk
                    tps_page = self.table.base_page_manager.fetch(pr_id, tps_page_index)
                    self.table.page_ranges[pr_id].base_pages[tps_page_index] = tps_page
                    self.table.size.add(1)

            tps_data = tps_page.get_record_int(offset)

            # Unpin the page
            self.table.base_page_manager.unpin(pr_id, tps_page_index)
            self.table.base_page_manager.update_page_usage(pr_id, tps_page_index)

            # Get desired columns' page indices
            columns = []
            for i in range(len(query_columns)):
                # Check schema (base page or tail page?)
                # If base page
                has_prev_tail_pages = self.bit_is_set(i + NUM_CONSTANT_COLUMNS, schema_data_int)
                if indirection_data <= tps_data or (query_columns[i] == 1 and not has_prev_tail_pages):

                    with self.table.page_range_lock:
                        base_page_index = base_page_indices[i + NUM_CONSTANT_COLUMNS]
                        base_page = page_range.base_pages[base_page_index]
                        # Pin the page
                        self.table.base_page_manager.pin(pr_id, base_page_index)
                        # If page is not in bufferpool, read from disk
                        if (base_page == None):
                            # if no space for new page
                            self.table.check_need_evict()
                            # Fetch page from disk
                            base_page = self.table.base_page_manager.fetch(pr_id,
                                                                           base_page_index)
                            self.table.page_ranges[pr_id].base_pages[base_page_index] = base_page
                            self.table.size.add(1)


                    base_data = base_page.get_record_int(offset)
                    # print("index",i,"appending base data", base_data)
                    columns.append(base_data)
                    # print(f"Column {i+4} -> Base Page Index: {base_page_index} -> Data: {base_data}")

                    # Unpin the page
                    self.table.base_page_manager.unpin(pr_id, base_page_index)
                    self.table.base_page_manager.update_page_usage(pr_id, base_page_index)
                # If tail page
                elif query_columns[i] == 1 and has_prev_tail_pages:  # query this column, but it's been updated before
                    # get tail page value of this column
                    # grab index and offset of this tail page
                    column_index = i + NUM_CONSTANT_COLUMNS
                    with self.table.page_range_lock:
                        tail_page_index_offset_tuple = tail_page_indices[i + NUM_CONSTANT_COLUMNS]
                        # print(f"tail_page (page index, offset): {tail_page_index_offset_tuple}")
                        tail_page_index = tail_page_index_offset_tuple[0]
                        tail_page_offset = tail_page_index_offset_tuple[1]
                        tail_page = page_range.tail_pages[tail_page_index]

                        # Pin the page
                        self.table.tail_page_manager.pin(pr_id, tail_page_index)
                        # If page is not in bufferpool, read from disk
                        if (tail_page == None):
                            # if no space for new page
                            self.table.check_need_evict()
                            # Fetch page from disk
                            tail_page = self.table.tail_page_manager.fetch(pr_id,
                                                                           tail_page_index)
                            self.table.page_ranges[pr_id].tail_pages[tail_page_index] = tail_page
                            self.table.size.add(1)

                    # print("tail_page size", tail_page.num_records, "offset", tail_page_offset)
                    tail_data = tail_page.get_record_int(tail_page_offset)
                    # Unpin the page
                    self.table.tail_page_manager.unpin(pr_id, tail_page_index)
                    self.table.tail_page_manager.update_page_usage(pr_id, tail_page_index)
                    if (tail_page_offset == 0):  # there's supposed to be somethinghere but its the wrong tail page
                        # we are in the right column, but the wrong tail page associated with it
                        # this is probably because of the indirection value was not dealt with before
                        # there could be a latest value for a column in a previous tail record
                        offset_exists = tail_page_offset
                        indirection_value = indirection_data
                        while (offset_exists == 0):  # while the current tail page doesn't have a value
                            # get the right number in the right tail record
                            # update tail directory with the indirection value of this current tail page
                            # make sure to find the right indirection value
                            with self.table.page_range_lock:
                                tp_dir = self.table.tail_page_directory[indirection_value]
                                indirection_index = tp_dir[INDIRECTION_COLUMN][0]
                                indirection_offset = tp_dir[INDIRECTION_COLUMN][1]
                                indirection_page = page_range.tail_pages[indirection_index]
                                # Pin the page
                                self.table.tail_page_manager.pin(pr_id, indirection_index)
                                if (indirection_page == None):
                                    # if no space for new page
                                    self.table.check_need_evict()

                                    # Fetch page from disk
                                    indirection_page = self.table.tail_page_manager.fetch(pr_id,
                                                                                          indirection_index)
                                    self.table.page_ranges[pr_id].tail_pages[indirection_index] = indirection_page
                                    self.table.size.add(1)

                            indirection_value = indirection_page.get_record_int(indirection_offset)
                            # Unpin the page
                            self.table.tail_page_manager.unpin(pr_id, indirection_index)
                            self.table.tail_page_manager.update_page_usage(pr_id, indirection_index)
                            if indirection_value == 0:
                                break
                            column_tuple = self.table.tail_page_directory[indirection_value][column_index]
                            offset_exists = column_tuple[1]

                        if (offset_exists != 0):  # there exists something in this page
                            with self.table.page_range_lock:
                                correct_tail_page = self.table.tail_page_directory[indirection_value][column_index]
                                tail_page = page_range.tail_pages[correct_tail_page[0]]
                                self.table.tail_page_manager.pin(pr_id,
                                                                 correct_tail_page[0])

                                if tail_page == None:
                                    self.table.check_need_evict()
                                    # Fetch page from disk
                                    tail_page = self.table.tail_page_manager.fetch(pr_id,
                                                                                   correct_tail_page[0])

                                    self.table.page_ranges[pr_id].tail_pages[correct_tail_page[0]] = tail_page
                                    self.table.size.add(1)

                            tail_data = tail_page.get_record_int(correct_tail_page[1])
                            self.table.tail_page_manager.unpin(pr_id,
                                                               correct_tail_page[0])
                            self.table.tail_page_manager.update_page_usage(pr_id,
                                                                           correct_tail_page[0])

                    columns.append(tail_data)
            print(f"rid: {rid}")
            print(f"key: {key}")
            print(f"columns: {columns}")
            record.append(Record(rid, key, columns))
            self.table.lock_manager.release(rid, 'R')

        if abort:
            Query.abort_sem.release()

        return record

    @staticmethod
    def int_to_binary(decimal, bit_size):
        temp = []
        binary = []
        for i in range(bit_size):
            if decimal > 0:
                temp.append(decimal % 2)
                decimal = decimal // 2
            else:
                temp.append(0)
        for i in range(bit_size - 1, -1, -1):
            binary.append(temp[i])
        return binary

    def bit_is_set(self, column, schema_enc):
        mask = 1 << (NUM_CONSTANT_COLUMNS + self.table.num_columns - column - 1)
        return schema_enc & mask > 0

    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, key, *columns, txn_id=0, abort=False):
        if abort:
            Query.abort_sem.acquire()
        # test abort - scenario: lock fails
        # self.abort(txn_id)
        # return False

        rid_base = self.table.keys[key]  # rid of base page with key
        # grab the old value for recovery purposes
        old_val = self.select(key, 0, [1] * self.table.num_columns)
        if old_val == False:
            old_val = ["None"]
            return False
        else:
            old_val = old_val[0].columns

        didAcquireLock = self.table.lock_manager.acquire(rid_base, 'W')
        if not didAcquireLock and abort == False:
            self.abort(txn_id)
            return False

        self.table.tail_rid.add(1)



        # Tail record default values
        indirection = 0
        rid = self.table.tail_rid.value  # rid of current tail page
        timestamp = int(time.time())
        schema_encoding = ''

        # Get current page range
        pr_id = rid_base // (PAGE_RANGE_MAX_RECORDS + 1)
        with self.table.page_range_lock:
            cur_pr = self.table.page_ranges[pr_id]

        # Get relative rid to new page range since it starts at 0
        rid_offset = rid_base - (PAGE_RANGE_MAX_RECORDS * pr_id)

        # Create new tail pages if there are none (i.e. first update performed)
        if len(cur_pr.tail_pages) == 0:
            tail_page_directory = []
            with self.table.page_range_lock:
                self.table.create_tail_page("indirection_t", rid_base)  # index 0
                self.table.create_tail_page("rid_t", rid_base)  # index 1
                self.table.create_tail_page("timestamp_t", rid_base)  # index 2
                self.table.create_tail_page("schema_t", rid_base)  # index 3
                self.table.create_tail_page("base_rid", rid_base)  # index 4
            for x in range(self.table.num_columns):
                with self.table.page_range_lock:
                    self.table.create_tail_page(x, rid_base)
            # Add the indices to the tail page directory
            for x in range(len(columns) + NUM_CONSTANT_COLUMNS):
                with self.table.page_range_lock:
                    page_index = cur_pr.free_tail_pages[x]
                    page = cur_pr.tail_pages[page_index]
                    # If page is not in bufferpool, read from disk
                    # Pin the page
                    self.table.tail_page_manager.pin(cur_pr.id_num.value, page_index)
                    if (page == None):
                        # if no space for new page
                        self.table.check_need_evict()
                        # Fetch page from disk
                        page = self.table.tail_page_manager.fetch(cur_pr.id_num.value, page_index)
                        self.table.page_ranges[pr_id].tail_pages[page_index] = page
                        self.table.size.add(1)
                    tail_page_directory.append((page_index, page.num_records))
                # Unpin the page
                self.table.tail_page_manager.unpin(cur_pr.id_num.value, page_index)
                self.table.tail_page_manager.update_page_usage(cur_pr.id_num.value, page_index)
                # tail_page_directory.append(self.table.free_tail_pages[x])
                # update tail page directory
                self.table.tail_page_directory[rid] = tail_page_directory

            cur_pr.update_count.add(5 + self.table.num_columns)
        # Already initialized tail pages
        else:
            # check if a tail record was created for this key in this page 
            # check indirection pointer of the rid in the base page

            with self.table.page_range_lock:
                # get indirection value in base page
                indirection_base_index = self.table.base_page_directory[rid_base][INDIRECTION_COLUMN]

                # indirection_base_page = self.table.base_pages[indirection_base_index]
                indirection_base_page = cur_pr.base_pages[indirection_base_index]
                # If page is not in bufferpool, read from disk
                if (indirection_base_page == None):
                    # if no space for new page
                    self.table.check_need_evict()

                    # Fetch page from disk
                    indirection_base_page = self.table.base_page_manager.fetch(cur_pr.id_num.value,
                                                                               indirection_base_index)
                    self.table.page_ranges[pr_id].base_pages[indirection_base_index] = indirection_base_page
                    self.table.size.add(1)
                    # Pin the page
                    self.table.base_page_manager.pin(cur_pr.id_num.value, indirection_base_index)

            indirection_value = indirection_base_page.get_record_int(rid_offset)
            indirection = indirection_value
            # Unpin the page
            self.table.base_page_manager.unpin(cur_pr.id_num.value, indirection_base_index)
            self.table.base_page_manager.update_page_usage(cur_pr.id_num.value, indirection_base_index)

            # Value has been updated before
            if (indirection_value != 0):
                with self.table.page_range_lock:
                    # check schema encoding to see if there's a previous tail page
                    # get the latest tail pages
                    matching_tail_pages = self.table.tail_page_directory[indirection_value]

                    # Get the schema encoding page of the matching tail page
                    schema_tail_page_index = matching_tail_pages[SCHEMA_ENCODING_COLUMN][0]  # 0 for index | 1 for offset
                    offset = matching_tail_pages[SCHEMA_ENCODING_COLUMN][1]
                    # schema_tail_page = self.table.tail_pages[schema_tail_page_index]
                    schema_tail_page = cur_pr.tail_pages[schema_tail_page_index]
                    # If page is not in bufferpool, read from disk
                    # Pin the page
                    self.table.tail_page_manager.pin(cur_pr.id_num.value, schema_tail_page_index)
                    if (schema_tail_page == None):
                        # if no space for new page
                        self.table.check_need_evict()
                        # Fetch page from disk
                        schema_tail_page = self.table.tail_page_manager.fetch(cur_pr.id_num.value,
                                                                              schema_tail_page_index)

                        self.table.page_ranges[pr_id].tail_pages[schema_tail_page_index] = schema_tail_page
                        self.table.size.add(1)


                # print("indirection value: ", indirection_value)
                # Get the schema encoding of the latest tail page
                latest_schema = schema_tail_page.get_record_int(offset)
                # Unpin the page
                self.table.tail_page_manager.unpin(cur_pr.id_num.value, schema_tail_page_index)
                self.table.tail_page_manager.update_page_usage(cur_pr.id_num.value, schema_tail_page_index)
                # print("latest_schema: ", latest_schema)
                if latest_schema > 0:  # there is at least one column that's updated
                    # create tail pages for everyone
                    tail_page_directory = []
                    with self.table.page_range_lock:
                        self.table.create_tail_page(INDIRECTION_COLUMN, rid_base)
                        self.table.create_tail_page(RID_COLUMN, rid_base)
                        self.table.create_tail_page(TIMESTAMP_COLUMN, rid_base)
                        self.table.create_tail_page(SCHEMA_ENCODING_COLUMN, rid_base)
                        self.table.create_tail_page(BASE_RID_COLUMN, rid_base)
                    for x in range(self.table.num_columns):
                        with self.table.page_range_lock:
                            self.table.create_tail_page(x + NUM_CONSTANT_COLUMNS, rid_base)
                    # Add the indices to the tail page directory
                    for x in range(len(columns) + NUM_CONSTANT_COLUMNS):
                        with self.table.page_range_lock:
                            page_index = cur_pr.free_tail_pages[x]
                            page = cur_pr.tail_pages[page_index]
                            # Pin the page
                            self.table.tail_page_manager.pin(cur_pr.id_num.value, page_index)
                            # If page is not in bufferpool, read from disk
                            if (page == None):
                                # if no space for new page
                                self.table.check_need_evict()
                                # Fetch page from disk
                                page = self.table.tail_page_manager.fetch(cur_pr.id_num.value, page_index)
                                self.table.page_ranges[pr_id].tail_pages[page_index] = page
                                self.table.size.add(1)

                            tail_page_directory.append((page_index, page.num_records))
                        # Unpin the page
                        self.table.tail_page_manager.unpin(cur_pr.id_num.value, page_index)
                        self.table.tail_page_manager.update_page_usage(cur_pr.id_num.value, page_index)
                        # tail_page_directory.append(self.table.free_tail_pages[x])
                    # update tail page directory
                    # set map of RID -> tail page indexes
                    with self.table.page_range_lock:
                        self.table.tail_page_directory[rid] = tail_page_directory
                    # Update number of updates for current page range
                    cur_pr.update_count.add(5 + self.table.num_columns)

        # find schema encoding of the new tail record
        # by comparing value of all the columns of this new tail record
        # with the record in the base page
        for x in range(NUM_CONSTANT_COLUMNS + len(columns)):
            with self.table.page_range_lock:
                # get base page val @ rid_base
                base_page_index = self.table.base_page_directory[rid_base][x]
                # base_page = self.table.base_pages[base_page_index]
                base_page = cur_pr.base_pages[base_page_index]
                # pin the page
                self.table.base_page_manager.pin(cur_pr.id_num.value, base_page_index)
                if (base_page == None):
                    # if no space for new page
                    self.table.check_need_evict()
                    # Fetch page from disk
                    base_page = self.table.base_page_manager.fetch(cur_pr.id_num.value, base_page_index)
                    self.table.page_ranges[pr_id].base_pages[base_page_index] = base_page
                    self.table.size.add(1)

            base_value = base_page.get_record_int(rid_offset)
            # Unpin the page
            self.table.base_page_manager.unpin(cur_pr.id_num.value, base_page_index)
            self.table.base_page_manager.update_page_usage(cur_pr.id_num.value, base_page_index)
            if x == INDIRECTION_COLUMN:
                schema_encoding += str(int(indirection == base_value))
            elif x == RID_COLUMN:
                schema_encoding += str(int(rid == base_value))
            elif x == TIMESTAMP_COLUMN:
                schema_encoding += str(int(timestamp == base_value))
            elif x == SCHEMA_ENCODING_COLUMN:
                schema_encoding += str(int(0 == base_value))
            else:
                schema_encoding += str(int(columns[x - NUM_CONSTANT_COLUMNS] != None))
        schema_encoding = int(schema_encoding, 2)

        # write to the tail pages
        tail_page_directory = []
        with self.table.page_range_lock:
            self.table.append_tail_page_record(INDIRECTION_COLUMN, indirection, rid_base)
            self.table.append_tail_page_record(RID_COLUMN, rid, rid_base)
            self.table.append_tail_page_record(TIMESTAMP_COLUMN, timestamp, rid_base)
            self.table.append_tail_page_record(SCHEMA_ENCODING_COLUMN, schema_encoding, rid_base)
            self.table.append_tail_page_record(BASE_RID_COLUMN, rid_base, rid_base)

        ### -------- Possibly change this so that it just puts None into record instead of 0s -------- ###
        for x in range(len(columns)):
            if columns[x] != None:
                # print(f"Appending value {columns[x]} into tail page at index {x + NUM_CONSTANT_COLUMNS}")
                with self.table.page_range_lock:
                    self.table.append_tail_page_record(x + NUM_CONSTANT_COLUMNS, columns[x], rid_base)
                    base_page_num = self.table.base_page_directory[rid_base][x + 5]

                    page = cur_pr.base_pages[base_page_num]
                    if page == None:
                        # if no space for new page
                        self.table.check_need_evict()
                        # Fetch page from disk
                        page = self.table.base_page_manager.fetch(cur_pr.id_num.value, page_index)
                        self.table.page_ranges[pr_id].base_pages[page_index] = page
                        self.table.size.add(1)

                base_record_val = page.get_record_int(rid_offset)
                if (x != 0):
                    self.table.index.update_btree(x, base_record_val, rid_base, columns[x])  # james added this
        ### ------------------------------------------------------------------------------------------ ###

        # Add the indices to the tail page directory
        for x in range(len(columns) + NUM_CONSTANT_COLUMNS):
            # pin the page
            with self.table.page_range_lock:
                # page_index = self.table.free_tail_pages[x]
                page_index = cur_pr.free_tail_pages[x]
                # page = self.table.tail_pages[page_index]
                page = cur_pr.tail_pages[page_index]
                # If page is not in bufferpool, read from disk
                self.table.tail_page_manager.pin(cur_pr.id_num.value, page_index)
                if (page == None):
                    # if no space for new page
                    self.table.check_need_evict()
                    # Fetch page from disk
                    page = self.table.tail_page_manager.fetch(cur_pr.id_num.value, page_index)
                    self.table.page_ranges[pr_id].tail_pages[page_index] = page
                    self.table.size.add(1)

                tail_page_directory.append((page_index, page.num_records))
            # Unin the page
            self.table.tail_page_manager.unpin(cur_pr.id_num.value, page_index)
            self.table.tail_page_manager.update_page_usage(cur_pr.id_num.value, page_index)
        # update tail page directory
        # set map of RID -> tail page indexes
        with self.table.page_range_lock:
            self.table.tail_page_directory[rid] = tail_page_directory

            # update base page indirection and schema encoding
            schema_enc_base_page_idx = self.table.base_page_directory[rid_base][SCHEMA_ENCODING_COLUMN]
            schema_base_page = cur_pr.base_pages[schema_enc_base_page_idx]
            # If page is not in bufferpool, read from disk
            self.table.base_page_manager.pin(cur_pr.id_num.value, schema_enc_base_page_idx)
            if (schema_base_page == None):
                # if no space for new page
                self.table.check_need_evict()

                # Fetch page from disk
                schema_base_page = self.table.base_page_manager.fetch(cur_pr.id_num.value, schema_enc_base_page_idx)
                self.table.page_ranges[pr_id].base_pages[schema_enc_base_page_idx] = schema_base_page
                self.table.size.add(1)

        last_base_schema_enc = schema_base_page.get_record_int(rid_offset)
        self.table.base_page_manager.unpin(cur_pr.id_num.value, schema_enc_base_page_idx)
        self.table.base_page_manager.update_page_usage(cur_pr.id_num.value, schema_enc_base_page_idx)

        new_base_schema_enc = last_base_schema_enc | schema_encoding
        with self.table.page_range_lock:
            self.table.update_base_rid(INDIRECTION_COLUMN, rid_base, rid)  # indirection
            self.table.update_base_rid(SCHEMA_ENCODING_COLUMN, rid_base, new_base_schema_enc)

        tail_page_sets = 2  # Merge every two sets of tail pages
        num_columns = 5 + self.table.num_columns  # number of columns in this table
        num_total_tail_pages = tail_page_sets * num_columns  # gives us what to mod by

        # if (cur_pr.update_count.value > 0) and (cur_pr.num_tail_pages.value % num_total_tail_pages == 0):
        #     with self.table.page_range_lock:
        #         self.table.merge(cur_pr)

        self.table.lock_manager.release(rid_base, 'W')
        # print("!!!!!!!BEFRORE WRITE IN UPDATE!!!!!!!!!!!! ", txn_id)

        if (txn_id > 0):
            self.logger.write(txn_id, "update", old_val, columns[1:], key)

        if abort:
            Query.abort_sem.release()
        return True

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        print(f"----------------------------------- sum -----------------------------------")
        # print(f"start_range = {start_range}, end_range = {end_range}, aggregate_column_index = {aggregate_column_index}")
        # print(start_range, end_range)
        query_columns = []
        for i in range(self.table.num_columns):
            query_columns.append(0)
        query_columns[aggregate_column_index] = 1
        print(f"query_columns: {query_columns}")
        count = 0
        for i in range(start_range, end_range + 1):
            print(f"\n\nCalling select with i = {i} and query_columns: {query_columns}")
            record = self.select(i, 0, query_columns)
            if len(record) == 0: continue
            data = record[0].columns
            # print(f"data: {data}")
            count += data[0]
            # print(f"count: {count}\n")
        return count

    """
    increments one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """

    def increment(self, key, column, txn_id=0):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r.columns[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False

    def abort(self, txn_id):
        # tid_abort = self.logger.last_abort
        # print("last abort", tid_abort)
        log_lines = self.logger.read_tid(txn_id)
        # print("loglines", log_lines)
        # log_lines = ["tid query old values new values key", ...]
        # ex: log_lines = ["1 insert 0,0,0,0, 2,3,4,5, 1"]
        for line in log_lines:
            parsed_line = line.split()  # ["1", "update", "0,0,0", "10,20,30", "key"]
            tid = int(parsed_line[0])
            query_str = parsed_line[1]
            old_values = self.parse_string_array(parsed_line[2])
            new_values = self.parse_string_tuple(parsed_line[2])
            key = int(parsed_line[4])

            if query_str == "update":
                # function call: update(self, key, *columns, txn_id=0, abort=False)
                self.update(key, *old_values, txn_id, True)  # To undo update: update w/ old values
            elif query_str == "insert":
                # function call: delete(self, key, txn_id=0, abort=False):
                self.delete(key, txn_id, True)  # To undo insert: delete
            elif query_str == "delete":
                # function call: insert(self, *columns, txn_id=0, abort=False):
                self.insert(*old_values, txn_id, True)  # To undo delete: insert

    @staticmethod
    def parse_string_array(string):
        string_with_no_ending_comma = string[:-1]
        parsed_string = string_with_no_ending_comma.split(',')

        values = []
        for i in range(len(parsed_string)):
            value = int(parsed_string[i])
            values.append(value)
        return values

    @staticmethod
    def parse_string_tuple(string):
        string_with_no_ending_comma = string[:-1]
        parsed_string = string_with_no_ending_comma.split(',')

        values = []
        for i in range(len(parsed_string)):
            value = int(parsed_string[i])
            values.append(value)
        return values

