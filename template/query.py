from template.table import *
from template.index import Index
import time
import threading


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    """

    def __init__(self, table):
        self.table = table
        pass

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon successful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, key):
        # grab the old value for recovery purposes
        old_val = self.select(key, 0, [1] * (self.table.num_columns - 1))

        # get base rid
        # base_rid = self.table.keys[key]
        rids = self.table.index.get_value(0, key)
        record = self.select(key, 0, [1, 1, 1, 1, 1])[0]
        # print("record columns are : " + str(record.columns))
        self.table.index.remove_values(0, key, list(rids)[0])
        for i in range(len(record.columns)):
            if i != 0:
                self.table.index.remove_values(i, record.columns[i], list(rids)[0])

        for base_rid in rids:
            # grab current page range 
            # pr_id = rid_base // (max_page_size / 8)
            pr_id = base_rid // (PAGE_RANGE_MAX_RECORDS + 1)
            cur_pr = self.table.page_ranges[pr_id]

            # get relative rid to new page range since it starts at 0
            offset = base_rid - (PAGE_RANGE_MAX_RECORDS * pr_id)

            # set rid to invalid value - 0
            self.table.update_base_rid(RID_COLUMN, base_rid, 0)

            # check if there are tail pages to invalidate as well
            # get indirection value of the base page 
            indirection_index = self.table.base_page_directory[base_rid][INDIRECTION_COLUMN]
            indirection_page = cur_pr.base_pages[indirection_index]
            # If page is not in bufferpool, read from disk
            if (indirection_page == None):
                # if no space for new page
                self.table.check_need_evict()

                # Fetch page from disk
                indirection_page = self.table.base_page_manager.fetch(cur_pr.id_num, indirection_index)
                cur_pr.base_pages[indirection_index] = indirection_page
                self.table.size += 1

            # Pin the current page
            self.table.base_page_manager.pin(cur_pr.id_num, indirection_index)
            indirection_value = indirection_page.get_record_int(offset)

            if indirection_value == 0:
                return old_val  # return the old value for recovery purposes

            # Unpin the current page
            self.table.base_page_manager.unpin(cur_pr.id_num, indirection_index)
            self.table.base_page_manager.update_page_usage(cur_pr.id_num, indirection_index)
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
                self.table.tail_page_manager.pin(cur_pr.id_num, indirection_index)
                # If page is not in bufferpool, read from disk
                if (indirection_page == None):
                    # if no space for new page
                    self.table.check_need_evict()

                    # Fetch page from disk
                    indirection_page = self.table.tail_page_manager.fetch(cur_pr.id_num, indirection_index)
                    cur_pr.tail_pages[indirection_index] = indirection_page
                    self.table.size += 1

                indirection_value = indirection_page.get_record_int(indirection_offset)
                self.table.tail_page_manager.unpin(cur_pr.id_num, indirection_index)
                self.table.tail_page_manager.update_page_usage(cur_pr.id_num, indirection_index)
                # update index to find the previous page for this column
                rid_index -= (NUM_CONSTANT_COLUMNS + self.table.num_columns)
        return old_val  # return old values for recovery purposes

    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        self.table.base_rid += 1

        page_directory_indexes = []
        # record = Record(self.table.base_rid, columns[0], columns)
        key = columns[0]
        schema_encoding = 0  # '0' * self.table.num_columns
        timestamp = int(time.time())
        rid = self.table.base_rid
        indirection = 0  # None

        # Write to the page
        self.table.append_base_page_record(INDIRECTION_COLUMN, indirection, rid)
        self.table.append_base_page_record(RID_COLUMN, rid, rid)
        self.table.append_base_page_record(TIMESTAMP_COLUMN, timestamp, rid)
        self.table.append_base_page_record(SCHEMA_ENCODING_COLUMN, schema_encoding, rid)
        self.table.append_base_page_record(TPS_COLUMN, 0, rid)

        # add each column's value to the respective page
        for x in range(len(columns)):
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
        cur_pr = self.table.page_ranges[pr_id]

        # SID -> RID
        self.table.keys[key] = self.table.base_rid
        # RID -> page_index
        for x in range(len(columns) + NUM_CONSTANT_COLUMNS):
            # page_directory_indexes.append(self.table.free_base_pages[x])
            page_directory_indexes.append(cur_pr.free_base_pages[x])
        self.table.base_page_directory[self.table.base_rid] = page_directory_indexes

        # for recovery purposes
        return [0] * (self.table.num_columns - 1)


    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    # add for loop to run it again multiple times with key being score and given a column number.
    def select(self, key, column, query_columns):
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
            # Find Page Range ID
            pr_id = rid // (PAGE_RANGE_MAX_RECORDS + 1)
            page_range = self.table.page_ranges[pr_id]

            # get relative rid to new page range since it starts at 0
            offset = rid - (PAGE_RANGE_MAX_RECORDS * pr_id)

            # Find physical pages' indices for RID from page_directory [RID:[x x x x x]]
            base_page_indices = self.table.base_page_directory[rid]
            # print(f"Found base pages: {base_page_indices}")

            # Get and check indirection
            indirection_page_index = base_page_indices[INDIRECTION_COLUMN]
            indirection_page = page_range.base_pages[indirection_page_index]
            if (indirection_page == None):
                # if no space for new page
                self.table.check_need_evict()
                # Fetch page from disk
                indirection_page = self.table.base_page_manager.fetch(pr_id, indirection_page_index)
                self.table.page_ranges[pr_id].base_pages[indirection_page_index] = indirection_page
                self.table.size += 1

            # Pin the page
            self.table.base_page_manager.pin(pr_id, indirection_page_index)
            indirection_data = indirection_page.get_record_int(offset)
            # Unpin the page
            self.table.base_page_manager.unpin(pr_id, indirection_page_index)
            self.table.base_page_manager.update_page_usage(pr_id, indirection_page_index)
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
                self.table.size += 1

            schema_data_int = schema_page.get_record_int(offset)

            # Unpin the page
            self.table.base_page_manager.unpin(pr_id, schema_page_index)
            self.table.base_page_manager.update_page_usage(pr_id, schema_page_index)

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
                self.table.size += 1

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
                    base_page_index = base_page_indices[i + NUM_CONSTANT_COLUMNS]
                    base_page = page_range.base_pages[base_page_index]
                    # If page is not in bufferpool, read from disk
                    if (base_page == None):
                        # if no space for new page
                        self.table.check_need_evict()
                        # Fetch page from disk
                        base_page = self.table.base_page_manager.fetch(pr_id,
                                                                       base_page_index)
                        self.table.page_ranges[pr_id].base_pages[base_page_index] = base_page
                        self.table.size += 1

                    # Pin the page
                    self.table.base_page_manager.pin(pr_id, base_page_index)

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
                    tail_page_index_offset_tuple = tail_page_indices[i + NUM_CONSTANT_COLUMNS]
                    # print(f"tail_page (page index, offset): {tail_page_index_offset_tuple}")
                    tail_page_index = tail_page_index_offset_tuple[0]
                    tail_page_offset = tail_page_index_offset_tuple[1]
                    tail_page = page_range.tail_pages[tail_page_index]

                    # If page is not in bufferpool, read from disk
                    if (tail_page == None):
                        # if no space for new page
                        self.table.check_need_evict()
                        # Fetch page from disk
                        tail_page = self.table.tail_page_manager.fetch(pr_id,
                                                                       tail_page_index)
                        self.table.page_ranges[pr_id].tail_pages[tail_page_index] = tail_page
                        self.table.size += 1

                    # print("tail_page size", tail_page.num_records, "offset", tail_page_offset)
                    # Pin the page
                    self.table.tail_page_manager.pin(pr_id, tail_page_index)
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
                            tp_dir = self.table.tail_page_directory[indirection_value]
                            indirection_index = tp_dir[INDIRECTION_COLUMN][0]
                            indirection_offset = tp_dir[INDIRECTION_COLUMN][1]
                            indirection_page = page_range.tail_pages[indirection_index]
                            if (indirection_page == None):
                                # if no space for new page
                                self.table.check_need_evict()

                                # Fetch page from disk
                                indirection_page = self.table.tail_page_manager.fetch(pr_id,
                                                                                      indirection_index)
                                self.table.page_ranges[pr_id].tail_pages[indirection_index] = indirection_page
                                self.table.size += 1

                            # Pin the page
                            self.table.tail_page_manager.pin(pr_id, indirection_index)
                            indirection_value = indirection_page.get_record_int(indirection_offset)
                            # Unpin the page
                            self.table.tail_page_manager.unpin(pr_id, indirection_index)
                            self.table.tail_page_manager.update_page_usage(pr_id, indirection_index)
                            if indirection_value == 0:
                                break
                            column_tuple = self.table.tail_page_directory[indirection_value][column_index]
                            offset_exists = column_tuple[1]

                        if (offset_exists != 0):  # there exists something in this page
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
                                self.table.size += 1

                            tail_data = tail_page.get_record_int(correct_tail_page[1])
                            self.table.tail_page_manager.unpin(pr_id,
                                                               correct_tail_page[0])
                            self.table.tail_page_manager.update_page_usage(pr_id,
                                                                           correct_tail_page[0])

                    columns.append(tail_data)

            record.append(Record(rid, key, columns))
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

    def update(self, key, *columns):
        self.table.tail_rid += 1

        # grab the old value for recovery purposes
        old_val = self.select(key, 0, [1] * self.table.num_columns)

        # Tail record default values
        indirection = 0
        rid = self.table.tail_rid  # rid of current tail page
        timestamp = int(time.time())
        schema_encoding = ''
        rid_base = self.table.keys[key]  # rid of base page with key

        # Get current page range
        pr_id = rid_base // (PAGE_RANGE_MAX_RECORDS + 1)
        cur_pr = self.table.page_ranges[pr_id]

        # Get relative rid to new page range since it starts at 0
        rid_offset = rid_base - (PAGE_RANGE_MAX_RECORDS * pr_id)

        # Create new tail pages if there are none (i.e. first update performed)
        if len(cur_pr.tail_pages) == 0:
            tail_page_directory = []
            self.table.create_tail_page("indirection_t", rid_base)  # index 0
            self.table.create_tail_page("rid_t", rid_base)  # index 1
            self.table.create_tail_page("timestamp_t", rid_base)  # index 2
            self.table.create_tail_page("schema_t", rid_base)  # index 3
            self.table.create_tail_page("base_rid", rid_base)  # index 4
            for x in range(self.table.num_columns):
                self.table.create_tail_page(x, rid_base)
            # Add the indices to the tail page directory
            for x in range(len(columns) + NUM_CONSTANT_COLUMNS):
                page_index = cur_pr.free_tail_pages[x]
                page = cur_pr.tail_pages[page_index]
                # If page is not in bufferpool, read from disk
                # Pin the page
                self.table.tail_page_manager.pin(cur_pr.id_num, page_index)
                if (page == None):
                    # if no space for new page
                    self.table.check_need_evict()
                    # Fetch page from disk
                    page = self.table.tail_page_manager.fetch(cur_pr.id_num, page_index)
                    self.table.page_ranges[pr_id].tail_pages[page_index] = page
                    self.table.size += 1
                tail_page_directory.append((page_index, page.num_records))
                # Unpin the page
                self.table.tail_page_manager.unpin(cur_pr.id_num, page_index)
                self.table.tail_page_manager.update_page_usage(cur_pr.id_num, page_index)
                # tail_page_directory.append(self.table.free_tail_pages[x])
            # update tail page directory
            self.table.tail_page_directory[rid] = tail_page_directory

            cur_pr.update_count += (5 + self.table.num_columns)
        # Already initialized tail pages
        else:
            # check if a tail record was created for this key in this page 
            # check indirection pointer of the rid in the base page

            # get indirection value in base page
            indirection_base_index = self.table.base_page_directory[rid_base][INDIRECTION_COLUMN]

            # indirection_base_page = self.table.base_pages[indirection_base_index]
            indirection_base_page = cur_pr.base_pages[indirection_base_index]
            # If page is not in bufferpool, read from disk
            if (indirection_base_page == None):
                # if no space for new page
                self.table.check_need_evict()

                # Fetch page from disk
                indirection_base_page = self.table.base_page_manager.fetch(cur_pr.id_num,
                                                                           indirection_base_index)
                self.table.page_ranges[pr_id].base_pages[indirection_base_index] = indirection_base_page
                self.table.size += 1

            # Pin the page
            self.table.base_page_manager.pin(cur_pr.id_num, indirection_base_index)
            indirection_value = indirection_base_page.get_record_int(rid_offset)
            indirection = indirection_value
            # Unpin the page
            self.table.base_page_manager.unpin(cur_pr.id_num, indirection_base_index)
            self.table.base_page_manager.update_page_usage(cur_pr.id_num, indirection_base_index)

            # Value has been updated before
            if (indirection_value != 0):
                # check schema encoding to see if there's a previous tail page 
                # get the latest tail pages
                matching_tail_pages = self.table.tail_page_directory[indirection_value]

                # Get the schema encoding page of the matching tail page
                schema_tail_page_index = matching_tail_pages[SCHEMA_ENCODING_COLUMN][0]  # 0 for index | 1 for offset
                offset = matching_tail_pages[SCHEMA_ENCODING_COLUMN][1]
                # schema_tail_page = self.table.tail_pages[schema_tail_page_index]
                schema_tail_page = cur_pr.tail_pages[schema_tail_page_index]
                # If page is not in bufferpool, read from disk
                if (schema_tail_page == None):
                    # if no space for new page
                    self.table.check_need_evict()
                    # Fetch page from disk
                    schema_tail_page = self.table.tail_page_manager.fetch(cur_pr.id_num,
                                                                          schema_tail_page_index)

                    self.table.page_ranges[pr_id].tail_pages[schema_tail_page_index] = schema_tail_page
                    self.table.size += 1

                # Pin the page
                self.table.tail_page_manager.pin(cur_pr.id_num, schema_tail_page_index)

                # print("indirection value: ", indirection_value)
                # Get the schema encoding of the latest tail page
                latest_schema = schema_tail_page.get_record_int(offset)
                # Unpin the page
                self.table.tail_page_manager.unpin(cur_pr.id_num, schema_tail_page_index)
                self.table.tail_page_manager.update_page_usage(cur_pr.id_num, schema_tail_page_index)
                # print("latest_schema: ", latest_schema)
                if latest_schema > 0:  # there is at least one column that's updated
                    # create tail pages for everyone
                    tail_page_directory = []
                    self.table.create_tail_page(INDIRECTION_COLUMN, rid_base)
                    self.table.create_tail_page(RID_COLUMN, rid_base)
                    self.table.create_tail_page(TIMESTAMP_COLUMN, rid_base)
                    self.table.create_tail_page(SCHEMA_ENCODING_COLUMN, rid_base)
                    self.table.create_tail_page(BASE_RID_COLUMN, rid_base)
                    for x in range(self.table.num_columns):
                        self.table.create_tail_page(x + NUM_CONSTANT_COLUMNS, rid_base)
                    # Add the indices to the tail page directory
                    for x in range(len(columns) + NUM_CONSTANT_COLUMNS):
                        page_index = cur_pr.free_tail_pages[x]
                        page = cur_pr.tail_pages[page_index]
                        # If page is not in bufferpool, read from disk
                        if (page == None):
                            # if no space for new page
                            self.table.check_need_evict()
                            # Fetch page from disk
                            page = self.table.tail_page_manager.fetch(cur_pr.id_num, page_index)
                            self.table.page_ranges[pr_id].tail_pages[page_index] = page
                            self.table.size += 1

                        # Pin the page
                        self.table.tail_page_manager.pin(cur_pr.id_num, page_index)
                        tail_page_directory.append((page_index, page.num_records))
                        # Unpin the page
                        self.table.tail_page_manager.unpin(cur_pr.id_num, page_index)
                        self.table.tail_page_manager.update_page_usage(cur_pr.id_num, page_index)
                        # tail_page_directory.append(self.table.free_tail_pages[x])
                    # update tail page directory
                    # set map of RID -> tail page indexes
                    self.table.tail_page_directory[rid] = tail_page_directory
                    # Update number of updates for current page range
                    cur_pr.update_count += (5 + self.table.num_columns)

        # find schema encoding of the new tail record
        # by comparing value of all the columns of this new tail record
        # with the record in the base page
        for x in range(NUM_CONSTANT_COLUMNS + len(columns)):
            # get base page val @ rid_base
            base_page_index = self.table.base_page_directory[rid_base][x]
            # base_page = self.table.base_pages[base_page_index]
            base_page = cur_pr.base_pages[base_page_index]
            if (base_page == None):
                # if no space for new page
                self.table.check_need_evict()
                # Fetch page from disk
                base_page = self.table.base_page_manager.fetch(cur_pr.id_num, base_page_index)
                self.table.page_ranges[pr_id].base_pages[base_page_index] = base_page
                self.table.size += 1

            # pin the page
            self.table.base_page_manager.pin(cur_pr.id_num, base_page_index)
            base_value = base_page.get_record_int(rid_offset)
            # Unpin the page
            self.table.base_page_manager.unpin(cur_pr.id_num, base_page_index)
            self.table.base_page_manager.update_page_usage(cur_pr.id_num, base_page_index)
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
        self.table.append_tail_page_record(INDIRECTION_COLUMN, indirection, rid_base)
        self.table.append_tail_page_record(RID_COLUMN, rid, rid_base)
        self.table.append_tail_page_record(TIMESTAMP_COLUMN, timestamp, rid_base)
        self.table.append_tail_page_record(SCHEMA_ENCODING_COLUMN, schema_encoding, rid_base)
        self.table.append_tail_page_record(BASE_RID_COLUMN, rid_base, rid_base)

        ### -------- Possibly change this so that it just puts None into record instead of 0s -------- ###
        for x in range(len(columns)):
            if columns[x] != None:
                # print(f"Appending value {columns[x]} into tail page at index {x + NUM_CONSTANT_COLUMNS}")
                self.table.append_tail_page_record(x + NUM_CONSTANT_COLUMNS, columns[x], rid_base)
                base_page_num = self.table.base_page_directory[rid_base][x + 5]
                base_record_val = cur_pr.base_pages[base_page_num].get_record_int(rid_offset)
                self.table.index.update_btree(x, base_record_val, rid_base, columns[x])  # james added this
        ### ------------------------------------------------------------------------------------------ ###

        # Add the indices to the tail page directory
        for x in range(len(columns) + NUM_CONSTANT_COLUMNS):
            # page_index = self.table.free_tail_pages[x]
            page_index = cur_pr.free_tail_pages[x]
            # page = self.table.tail_pages[page_index]
            page = cur_pr.tail_pages[page_index]
            # If page is not in bufferpool, read from disk
            if (page == None):
                # if no space for new page
                self.table.check_need_evict()
                # Fetch page from disk
                page = self.table.tail_page_manager.fetch(cur_pr.id_num, page_index)
                self.table.page_ranges[pr_id].tail_pages[page_index] = page
                self.table.size += 1

            # pin the page
            self.table.tail_page_manager.pin(cur_pr.id_num, page_index)
            tail_page_directory.append((page_index, page.num_records))
            # Unin the page
            self.table.tail_page_manager.unpin(cur_pr.id_num, page_index)
            self.table.tail_page_manager.update_page_usage(cur_pr.id_num, page_index)
        # update tail page directory
        # set map of RID -> tail page indexes
        self.table.tail_page_directory[rid] = tail_page_directory

        # update base page indirection and schema encoding 
        schema_enc_base_page_idx = self.table.base_page_directory[rid_base][SCHEMA_ENCODING_COLUMN]
        schema_base_page = cur_pr.base_pages[schema_enc_base_page_idx]
        # If page is not in bufferpool, read from disk
        if (schema_base_page == None):
            # if no space for new page
            self.table.check_need_evict()

            # Fetch page from disk
            schema_base_page = self.table.base_page_manager.fetch(cur_pr.id_num, schema_enc_base_page_idx)
            self.table.page_ranges[pr_id].base_pages[schema_enc_base_page_idx] = schema_base_page
            self.table.size += 1

        self.table.base_page_manager.pin(cur_pr.id_num, schema_enc_base_page_idx)
        last_base_schema_enc = schema_base_page.get_record_int(rid_offset)
        self.table.base_page_manager.unpin(cur_pr.id_num, schema_enc_base_page_idx)
        self.table.base_page_manager.update_page_usage(cur_pr.id_num, schema_enc_base_page_idx)

        new_base_schema_enc = last_base_schema_enc | schema_encoding
        self.table.update_base_rid(INDIRECTION_COLUMN, rid_base, rid)  # indirection
        self.table.update_base_rid(SCHEMA_ENCODING_COLUMN, rid_base, new_base_schema_enc)

        tail_page_sets = 2  # Merge every two sets of tail pages
        num_columns = 5 + self.table.num_columns  # number of columns in this table
        num_total_tail_pages = tail_page_sets * num_columns  # gives us what to mod by

        if (cur_pr.update_count > 0) and (cur_pr.num_tail_pages % num_total_tail_pages == 0):
            self.table.merge(cur_pr)

        # return old val for transaction
        return old_val

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        # print(f"----------------------------------- sum -----------------------------------")
        # print(f"start_range = {start_range}, end_range = {end_range}, aggregate_column_index = {aggregate_column_index}")
        # print(start_range, end_range)
        query_columns = []
        for i in range(self.table.num_columns):
            query_columns.append(0)
        query_columns[aggregate_column_index] = 1
        # print(f"query_columns: {query_columns}")
        count = 0
        for i in range(start_range, end_range + 1):
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

    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False

    def abort(self, line):
        # line = "tid query old_val_1,...,old_val_x new_val_1,...,new_val_x key"
        # ex: line = "1 update 0,0,0 10,20,30 key"
        parsed_line = line.split()  # ["1", "update", "0,0,0", "10,20,30", "key"]
        tid = int(parsed_line[0])
        query_str = parsed_line[1]
        old_values = self.parse_string_array(parsed_line[2])
        new_values = self.parse_string_array(parsed_line[2])
        key = int(parsed_line[4])

        if query_str == "update":
            self.query.update(key, *old_values)     # To undo update: update w/ old values
        elif query_str == "insert":
            self.query.delete(*key)                 # To undo insert: delete
        elif query_str == "delete":
            self.query.insert(*old_values)          # To undo delete: insert

    @staticmethod
    def parse_string_array(string):
        values = []
        parsed_string = string.split(',')
        for i in range(len(parsed_string)):
            value = int(parsed_string[i])
            values.append(value)
        return values

