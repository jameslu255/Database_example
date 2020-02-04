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
        print("removing ", key)
        # get base rid
        base_rid = self.table.keys[key] 
        
        # grab current page range 
        # pr_id = rid_base // (max_page_size / 8)
        pr_id = base_rid // (512 + 1)
        cur_pr = self.table.page_ranges[pr_id]
        
        # set rid to invalid value - 0
        self.table.update_base_rid(RID_COLUMN, base_rid, 0)
        
        # check if there are tail pages to invalidate as well
        # get indirection value of the base page 
        indirection_index = self.table.base_page_directory[base_rid][INDIRECTION_COLUMN]
        indirection_page = cur_pr.base_pages[indirection_index]
        indirection_value = indirection_page.get_record_int(base_rid)
        
        # index arithmetic to find the latest tail page and its predecessors
        rid_index = self.table.tail_page_directory[indirection_value][RID_COLUMN][0] # index part of tuple
        
        while(indirection_value != 0): # there are update(s) to this rid 
            # set tail rid to invalid 0 of the tail record
            rid_offset = self.table.tail_page_directory[indirection_value][RID_COLUMN][1] # offset
            self.table.update_tail_rid(rid_index, rid_offset, 0, base_rid)
            
            # print(indirection_column_index)
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
        record = Record(self.table.base_rid, columns[0], columns)
        schema_encoding = 0 # '0' * self.table.num_columns
        timestamp = int(time.time())
        rid = record.rid
        indirection = 0 # None
        
        # Write to the page
        self.table.update_base_page(INDIRECTION_COLUMN, indirection, rid)
        self.table.update_base_page(RID_COLUMN, rid, rid)
        self.table.update_base_page(TIMESTAMP_COLUMN, timestamp, rid)
        self.table.update_base_page(SCHEMA_ENCODING_COLUMN, schema_encoding, rid)
        
        # add each column's value to the respective page
        for x in range(len(record.columns)):
            self.table.update_base_page(x + 4, columns[x], rid)
        
        # grab current page range 
        # pr_id = rid_base // (max_page_size / 8)
        pr_id = rid // (512 + 1)
        # print("pr_id", pr_id)
        cur_pr = self.table.page_ranges[pr_id]
        
        # SID -> RID
        self.table.keys[record.key] = self.table.base_rid
        # RID -> page_index
        for x in range(len(record.columns) + 4):
            # page_directory_indexes.append(self.table.free_base_pages[x])
            page_directory_indexes.append(cur_pr.free_base_pages[x])
        self.table.base_page_directory[self.table.base_rid] = page_directory_indexes
        pass

    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        pass

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
            indirection_value = indirection_base_page.get_record_int(rid_base)
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
            base_value= base_page.get_record_int(rid_base)
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
        self.table.update_base_rid(INDIRECTION_COLUMN, rid_base, rid) #indirection 
        self.table.update_base_rid(SCHEMA_ENCODING_COLUMN, rid_base, schema_encoding)
        
        pass

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass
