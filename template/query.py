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
        self.table.update_page(INDIRECTION_COLUMN, indirection)
        self.table.update_page(RID_COLUMN, rid)
        self.table.update_page(TIMESTAMP_COLUMN, timestamp)
        self.table.update_page(SCHEMA_ENCODING_COLUMN, schema_encoding)
        
        # add each column's value to the respective page
        for x in range(len(record.columns)):
            self.table.update_page(x + 4, columns[x])
        
        # SID -> RID
        self.table.keys[record.key] = self.table.base_rid
        # RID -> page_index
        for x in range(len(record.columns) + 4):
            page_directory_indexes.append(self.table.free_pages[x])
        self.table.page_directory[self.table.base_rid] = page_directory_indexes
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
        print(key, columns)
        self.table.tail_rid += 1
        # default values for the tail record
        schema_encoding = '' #'0' * (self.table.num_columns + 4)
        timestamp = int(time.time())
        rid = self.table.tail_rid # rid of current tail page
        rid_base = self.table.keys[key] # rid of base page with key
        indirection = 0
        print("len of tail page: " + str(len(self.table.pages_tail)))
        # If there are no tail pages (i.e. first update performed)
        # initiate new tail pages if tail page array empty
        if len(self.table.pages_tail) == 0: #tail page list empty
            print("creating new tail pages on init")
            tail_page_directory = []
            self.table.create_page_tail("indirection_t") #index 0
            self.table.create_page_tail("rid_t") #index 1
            self.table.create_page_tail("timestamp_t")#index 2
            self.table.create_page_tail("schema_t")#index 3
            for x in range(self.table.num_columns):
                self.table.create_page_tail(x)
            # Add the indices to the tail page directory
            for x in range(len(columns) + 4):
                tail_page_directory.append(self.table.free_pages_tail[x])
            # update tail page directory
            self.table.tail_page_directory[rid] = tail_page_directory
        else: #already initialized tail pages
            print("oongoaaaaaaaaaaaaaaa")
            # check if a tail record was created for this key in this page 
            # check indirection pointer of the rid in the base page
            # get indirection value in base page
            indirection_base_index = self.table.page_directory[rid_base][0]
            print(self.table.page_directory)
            
            indirection_base_page = self.table.pages_base[indirection_base_index]
            indirection_value = indirection_base_page.get_record_int(rid_base)
            indirection = indirection_value
            print(f"indirection: {indirection}")
            if(indirection_value != 0): #not a 0 => values has been updated before
                # check schema encoding to see if there's a previous tail page 
                # get the latest tail pages
                matching_tail_pages = self.table.tail_page_directory[indirection_value]
                print("page tail dir", self.table.tail_page_directory)
                # Get the schema encoding page of the matching tail page
                schema_tail_page_index = matching_tail_pages[3] # schema index
                schema_tail_page = self.table.pages_tail[schema_tail_page_index]
                print("len of tail pages: ", len(self.table.pages_tail))
                # Get the schema encoding of the latest tail page
                latest_schema = schema_tail_page.get_record_int(indirection_value)
                print(f"schema: {latest_schema}")
                if latest_schema > 0: #there is at least one column that's updated
                    # create tail pages for everyone
                    print("creating new tail pages")
                    tail_page_directory = []
                    self.table.create_page_tail(0) #indirection
                    self.table.create_page_tail(1) #rid 
                    self.table.create_page_tail(2)#timestamp
                    self.table.create_page_tail(3)#schema
                    for x in range(self.table.num_columns):
                        self.table.create_page_tail(x + 4)
                    # Add the indices to the tail page directory
                    for x in range(len(columns) + 4):
                        tail_page_directory.append(self.table.free_pages_tail[x])
                    # update tail page directory
                    # set map of RID -> tail page indexes
                    self.table.tail_page_directory[rid] = tail_page_directory
                    
            
        # find schema encoding of the new tail record
        # by comparing value of all the columns of this new tail record
        # with the record in the base page
        for x in range(4 + len(columns)):
            # get base page val @ rid_base
            print("on col: ", x)
            base_page_index = self.table.page_directory[rid_base][x]
            base_page = self.table.pages_base[base_page_index]
            base_value= base_page.get_record_int(rid_base)
            if x == 0: #indirection
                schema_encoding += str(int(indirection == base_value))
            elif x == 1: # rid
                schema_encoding += str(int(rid == base_value))
            elif x == 2: #timestamp 
                schema_encoding += str(int(timestamp == base_value))
            elif x == 3: #schema encoding, 0 on init
                schema_encoding += str(int(0 == base_value))
            else:
                schema_encoding += str(int(columns[x - 4] != None))
        schema_encoding = int(schema_encoding, 2)
        print(schema_encoding)
        # write to the tail pages
        tail_page_directory = []
        self.table.update_page_tail(0, indirection)
        self.table.update_page_tail(1, rid)
        self.table.update_page_tail(2, timestamp)
        self.table.update_page_tail(3, schema_encoding)
        for x in range(len(columns)):
            if columns[x] != None:
                self.table.update_page_tail(x + 4, columns[x])
        # Add the indices to the tail page directory
        for x in range(len(columns) + 4):
            tail_page_directory.append(self.table.free_pages_tail[x])
        # update tail page directory
        # set map of RID -> tail page indexes
        self.table.tail_page_directory[rid] = tail_page_directory
                    
        # update base page indirection and schema encoding 
        self.table.update_base_rid(0, rid_base, rid) #indirection 
        self.table.update_base_rid(3, rid_base, schema_encoding)
        
        pass

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass
