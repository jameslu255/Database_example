from template.table import *
from template.index import Index
import time

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
    """
    def delete(self, key):
        # get base rid
        base_rid = self.table.keys[key] 
        
        # grab current page range 
        # pr_id = rid_base // (max_page_size / 8)
        pr_id = base_rid // (512 + 1)
        cur_pr = self.table.page_ranges[pr_id]
        
        # get relative rid to new page range since it starts at 0
        offset = base_rid - (512 * pr_id)
        
        # set rid to invalid value - 0
        self.table.update_base_rid(RID_COLUMN, base_rid, 0)

        # check if there are tail pages to invalidate as well
        # get indirection value of the base page 
        indirection_index = self.table.base_page_directory[base_rid][INDIRECTION_COLUMN]
        indirection_page = cur_pr.base_pages[indirection_index]
        indirection_value = indirection_page.get_record_int(offset)
        
        if indirection_value == 0:
            return
        
        # index arithmetic to find the latest tail page and its predecessors
        rid_index = self.table.tail_page_directory[indirection_value][RID_COLUMN][0] # index part of tuple
        
        while(indirection_value != 0): # there are update(s) to this rid 
            # set tail rid to invalid 0 of the tail record
            rid_offset = self.table.tail_page_directory[indirection_value][RID_COLUMN][1] # offset
            self.table.update_tail_rid(rid_index, rid_offset, 0, base_rid)
            
            # get the tail record indirection value 
            indirection_index = self.table.tail_page_directory[indirection_value][INDIRECTION_COLUMN][0] # index
            indirection_offset = self.table.tail_page_directory[indirection_value][INDIRECTION_COLUMN][1] # offset
            # indirection_page = self.table.tail_pages[indirection_index]
            indirection_page = cur_pr.tail_pages[indirection_index]
            indirection_value = indirection_page.get_record_int(indirection_offset)
            
            # update index to find the previous page for this column
            rid_index -= (4 + self.table.num_columns)
        pass



    """
    # Insert a record with specified columns
    """
    def insert(self, *columns):  
        self.table.base_rid += 1

        page_directory_indexes = []
        # record = Record(self.table.base_rid, columns[0], columns)
        key = columns[0]
        schema_encoding = 0 # '0' * self.table.num_columns
        timestamp = int(time.time())
        rid = self.table.base_rid
        indirection = 0 # None
        
        # Write to the page
        self.table.update_base_page(INDIRECTION_COLUMN, indirection, rid)
        self.table.update_base_page(RID_COLUMN, rid, rid)
        self.table.update_base_page(TIMESTAMP_COLUMN, timestamp, rid)
        self.table.update_base_page(SCHEMA_ENCODING_COLUMN, schema_encoding, rid)
        
        # add each column's value to the respective page
        for x in range(len(columns)):
            self.table.update_base_page(x + 4, columns[x], rid)
            if x != 0:
                # subtract 1 from x because we want to start with assignment 1
                Index.add_values(self, x, columns[x], rid)
            else:
                Index.create_dictionary(self, x, columns[x], rid)
        
        # grab current page range 
        # pr_id = rid_base // (max_page_size / 8)
        pr_id = rid // (512 + 1)
        # print("pr_id", pr_id)
        cur_pr = self.table.page_ranges[pr_id]
        
        # SID -> RID
        self.table.keys[key] = self.table.base_rid
        # RID -> page_index
        for x in range(len(columns) + 4):
            # page_directory_indexes.append(self.table.free_base_pages[x])
            page_directory_indexes.append(cur_pr.free_base_pages[x])
        self.table.base_page_directory[self.table.base_rid] = page_directory_indexes
        pass



    """
    # Read a record with specified key
    """
    def select(self, key, query_columns):
        if key not in self.table.keys:
            return []

        # Find RID from key, keys = {SID: RID}
        rid = self.table.keys[key]

        # Find Page Range ID
        pr_id = rid//513
        page_range = self.table.page_ranges[pr_id]
        
        # get relative rid to new page range since it starts at 0
        offset = rid - (512 * pr_id)

        # Find physical pages' indices for RID from page_directory [RID:[x x x x x]]
        base_page_indices = self.table.base_page_directory[rid]
        # print(f"Found base pages: {base_page_indices}")

        # Get and check indirection
        indirection_page_index = base_page_indices[INDIRECTION_COLUMN]
        indirection_page = page_range.base_pages[indirection_page_index]
        indirection_data = indirection_page.get_record_int(offset)
        if indirection_data != 0:
            tail_page_indices = self.table.tail_page_directory[indirection_data]

        # Get schema
        schema_page_index = base_page_indices[SCHEMA_ENCODING_COLUMN]
        schema_page = page_range.base_pages[schema_page_index]
        schema_data_int = schema_page.get_record_int(offset)

        # Get desired columns' page indices
        columns = []
        for i in range(len(query_columns)):
            # Check schema (base page or tail page?)
            # If base page
            has_prev_tail_pages = self.bit_is_set(i+4, schema_data_int)
            if query_columns[i] == 1 and not has_prev_tail_pages:
                base_page_index = base_page_indices[i+4]
                base_page = page_range.base_pages[base_page_index]
                base_data = base_page.get_record_int(offset)
                # print("index",i,"appending base data", base_data)
                columns.append(base_data)
                # print(f"Column {i+4} -> Base Page Index: {base_page_index} -> Data: {base_data}")
            # If tail page
            elif query_columns[i] == 1 and has_prev_tail_pages:# query this column, but it's been updated before
                # get tail page value of this column 
                # grab index and offset of this tail page
                column_index = i + 4
                tail_page_index_offset_tuple = tail_page_indices[i+4]
                # print(f"tail_page (page index, offset): {tail_page_index_offset_tuple}")
                tail_page_index = tail_page_index_offset_tuple[0]
                tail_page_offset = tail_page_index_offset_tuple[1]
                tail_page = page_range.tail_pages[tail_page_index]
                # print("tail_page size", tail_page.num_records, "offset", tail_page_offset)
                tail_data = tail_page.get_record_int(tail_page_offset)
                if(tail_page_offset == 0): #there's supposed to be somethinghere but its the wrong tail page
                    # we are in the right column, but the wrong tail page associated with it
                    # this is probably because of the indirection value was not dealt with before 
                    # there could be a latest value for a column in a previous tail record
                    offset_exists = tail_page_offset
                    indirection_value = indirection_data
                    while(offset_exists == 0): #while the current tail page doesn't have a value
                        # get the right number in the right tail record 
                        # update tail directory with the indirection value of this current tail page
                        # make sure to find the right indirection value
                        tp_dir = self.table.tail_page_directory[indirection_value]
                        indirection_index = tp_dir[INDIRECTION_COLUMN][0]
                        indirection_offset = tp_dir[INDIRECTION_COLUMN][1]
                        indirection_page = page_range.tail_pages[indirection_index]
                        indirection_value  = indirection_page.get_record_int(indirection_offset)
                        if indirection_value == 0:
                            break
                        column_tuple = self.table.tail_page_directory[indirection_value][column_index]
                        offset_exists = column_tuple[1]
                        
                    if(offset_exists != 0): #there exists something in this page
                        correct_tail_page = self.table.tail_page_directory[indirection_value][column_index]
                        tail_page = page_range.tail_pages[correct_tail_page[0]]
                        tail_data = tail_page.get_record_int(correct_tail_page[1])
                        # print("correct tail page data is in index",correct_tail_page[0],correct_tail_page[1])

                columns.append(tail_data)
                
        record = [Record(rid, key, columns)]
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
        for i in range(bit_size-1,-1,-1):
            binary.append(temp[i])
        return binary

    def bit_is_set(self, column, schema_enc):
        mask = 1 << (4 + self.table.num_columns - column - 1)
        return schema_enc & mask > 0



    """
    # Update a record with specified key and columns
    """
    def update(self, key, *columns):
        self.table.tail_rid += 1
        
        # default values for the tail record
        schema_encoding = '' #
        timestamp = int(time.time())
        rid = self.table.tail_rid # rid of current tail page
        rid_base = self.table.keys[key] # rid of base page with key
        indirection = 0
        
        # grab current page range 
        # pr_id = rid_base // (max_page_size / 8)
        pr_id = rid_base // (512 + 1)
        cur_pr = self.table.page_ranges[pr_id]

        # get relative rid to new page range since it starts at 0
        rid_offset = rid_base - (512 * pr_id)

        # If there are no tail pages (i.e. first update performed)
        # initiate new tail pages if tail page array empty
        # if len(self.table.tail_pages) == 0: #tail page list empty
        if len(cur_pr.tail_pages) == 0: #tail page list empty
            tail_page_directory = []
            self.table.create_tail_page("indirection_t", rid_base) #index 0
            self.table.create_tail_page("rid_t", rid_base) #index 1
            self.table.create_tail_page("timestamp_t", rid_base)#index 2
            self.table.create_tail_page("schema_t", rid_base)#index 3
            for x in range(self.table.num_columns):
                self.table.create_tail_page(x, rid_base)
            # Add the indices to the tail page directory
            for x in range(len(columns) + 4):
                page_index = cur_pr.free_tail_pages[x]
                page = cur_pr.tail_pages[page_index]
                tail_page_directory.append((page_index, page.num_records))
                # tail_page_directory.append(self.table.free_tail_pages[x])
            # update tail page directory
            self.table.tail_page_directory[rid] = tail_page_directory
            
        else: #already initialized tail pages
            # check if a tail record was created for this key in this page 
            # check indirection pointer of the rid in the base page
            
            # get indirection value in base page
            indirection_base_index = self.table.base_page_directory[rid_base][INDIRECTION_COLUMN]
            
            # indirection_base_page = self.table.base_pages[indirection_base_index]
            indirection_base_page = cur_pr.base_pages[indirection_base_index]
            indirection_value = indirection_base_page.get_record_int(rid_offset)
            indirection = indirection_value

            if(indirection_value != 0): #not a 0 => values has been updated before
                # check schema encoding to see if there's a previous tail page 
                # get the latest tail pages
                matching_tail_pages = self.table.tail_page_directory[indirection_value]

                # Get the schema encoding page of the matching tail page
                schema_tail_page_index = matching_tail_pages[SCHEMA_ENCODING_COLUMN][0] # 0 for index | 1 for offset
                offset = matching_tail_pages[SCHEMA_ENCODING_COLUMN][1]
                # schema_tail_page = self.table.tail_pages[schema_tail_page_index]
                schema_tail_page = cur_pr.tail_pages[schema_tail_page_index]
                # print("indirection value: ", indirection_value)
                # Get the schema encoding of the latest tail page
                latest_schema = schema_tail_page.get_record_int(offset)
                # print("latest_schema: ", latest_schema)
                if latest_schema > 0: #there is at least one column that's updated
                    # create tail pages for everyone
                    tail_page_directory = []
                    self.table.create_tail_page(INDIRECTION_COLUMN, rid_base)
                    self.table.create_tail_page(RID_COLUMN, rid_base) 
                    self.table.create_tail_page(TIMESTAMP_COLUMN, rid_base)
                    self.table.create_tail_page(SCHEMA_ENCODING_COLUMN, rid_base)
                    for x in range(self.table.num_columns):
                        self.table.create_tail_page(x + 4, rid_base)
                    # Add the indices to the tail page directory
                    for x in range(len(columns) + 4):
                        # page_index = self.table.free_tail_pages[x]
                        page_index = cur_pr.free_tail_pages[x]
                        # page = self.table.tail_pages[page_index]
                        page = cur_pr.tail_pages[page_index]
                        tail_page_directory.append((page_index, page.num_records))
                        # tail_page_directory.append(self.table.free_tail_pages[x])
                    # update tail page directory
                    # set map of RID -> tail page indexes
                    self.table.tail_page_directory[rid] = tail_page_directory
                    
            
        # find schema encoding of the new tail record
        # by comparing value of all the columns of this new tail record
        # with the record in the base page
        for x in range(4 + len(columns)):
            # get base page val @ rid_base
            base_page_index = self.table.base_page_directory[rid_base][x]
            # base_page = self.table.base_pages[base_page_index]
            base_page = cur_pr.base_pages[base_page_index]
            base_value= base_page.get_record_int(rid_offset)
            if x == INDIRECTION_COLUMN:
                schema_encoding += str(int(indirection == base_value))
            elif x == RID_COLUMN:
                schema_encoding += str(int(rid == base_value))
            elif x == TIMESTAMP_COLUMN:
                schema_encoding += str(int(timestamp == base_value))
            elif x == SCHEMA_ENCODING_COLUMN:
                schema_encoding += str(int(0 == base_value))
            else:
                schema_encoding += str(int(columns[x - 4] != None))
        schema_encoding = int(schema_encoding, 2)

        # write to the tail pages
        tail_page_directory = []
        self.table.update_tail_page(INDIRECTION_COLUMN, indirection, rid_base)
        self.table.update_tail_page(RID_COLUMN, rid, rid_base)
        self.table.update_tail_page(TIMESTAMP_COLUMN, timestamp, rid_base)
        self.table.update_tail_page(SCHEMA_ENCODING_COLUMN, schema_encoding, rid_base)
        for x in range(len(columns)):
            if columns[x] != None:
                self.table.update_tail_page(x + 4, columns[x], rid_base)
                base_page_num = self.table.base_page_directory[rid_base][x + 4]
                base_record_val = cur_pr.base_pages[base_page_num].get_record_int(rid_offset)
                if x != 0:
                    self.table.index.update_btree(x, base_record_val, rid_base, columns[x])  # james added this
        # Add the indices to the tail page directory
        for x in range(len(columns) + 4):
            # page_index = self.table.free_tail_pages[x]
            page_index = cur_pr.free_tail_pages[x]
            # page = self.table.tail_pages[page_index]
            page = cur_pr.tail_pages[page_index]
            tail_page_directory.append((page_index, page.num_records))
        # update tail page directory
        # set map of RID -> tail page indexes
        self.table.tail_page_directory[rid] = tail_page_directory
                    
        # update base page indirection and schema encoding 
        schema_enc_base_page_idx = self.table.base_page_directory[rid_base][SCHEMA_ENCODING_COLUMN]
        last_base_schema_enc = cur_pr.base_pages[schema_enc_base_page_idx].get_record_int(rid_offset)

        new_base_schema_enc = last_base_schema_enc | schema_encoding
        self.table.update_base_rid(INDIRECTION_COLUMN, rid_base, rid) #indirection 
        self.table.update_base_rid(SCHEMA_ENCODING_COLUMN, rid_base, new_base_schema_enc)



    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
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
            record = self.select(i, query_columns)
            if len(record) == 0: continue
            data = record[0].columns
            # print(f"data: {data}")
            count += data[0]
            # print(f"count: {count}\n")
        return count
        pass
