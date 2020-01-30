from template.table import Table, Record
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
        page_directory_indexes = []
        record = Record(self.table.records + 1, columns[0], columns)
        schema_encoding = 0 # '0' * self.table.num_columns
        timestamp = int(time.time())
        rid = record.rid
        indirection = 0 # None
        
        self.table.update_page(0, schema_encoding)
        self.table.update_page(1, timestamp)
        self.table.update_page(2, rid)
        self.table.update_page(3, indirection)
        
        # add each column's value to the respective page
        for x in range(len(record.columns)):
            self.table.update_page(x + 4, columns[x])
        
        # SID -> RID
        self.table.keys[record.key] = self.table.records
        # RID -> page_index
        for x in range(len(record.columns) + 4):
            page_directory_indexes.append(self.table.free_pages[x])
        self.table.page_directory[self.table.records] = page_directory_indexes
        # [self.table.free_pages[i] for i in range(record.columns) + 4]
        self.table.records += 1
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
        # initiate new tail pages if tail page array empty
        if not self.table.pages_tail: #tail page list empty
            print("creating new tail pages")
            self.table.create_page_tail("indirection_t") #index 0
            self.table.create_page_tail("rid_t") #index 1
            self.table.create_page_tail("timestamp_t")#index 2
            self.table.create_page_tail("schema_t")#index 3
            for x in range(self.table.num_columns):
                self.table.create_page_tail(x)
                
        # tail page record info gathering
        # get next available space in tail page = rid tail
        rid_tail_index = self.table.free_pages_tail[2] #doesnt matter which column
        rid_tail = self.table.pages_tail[rid_tail_index].records + 1
        rid = self.table.keys[key]
        schema_encoding = 0
        timestamp = int(time.time())
        indirection = 0 # point to self, use None?
        record = Record(rid, columns[0], columns)
        
        # update the tail pages
        self.table.update_page_tail(0, schema_encoding)
        self.table.update_page_tail(1, timestamp)
        self.table.update_page_tail(2, rid_tail)
        self.table.update_page_tail(3, indirection)
        
        # add each column's value to the respective tail page
        for x in range(len(record.columns)):
            self.table.update_page_tail(x + 4, columns[x])
            
        # update base page indirection and schema encoding
        self.table.update_base_indirection(rid, rid_tail)
        # rid_tail_index = self.table.free_pages_tail[0] #doesnt matter which column
        # rid_tail = self.table.pages_tail[rid_tail_index].records
        # !!! replace value at rid of base page indrection
        # base_indirection_index = self.table.free_pages[3]
        # self.table.update_page(3, rid_tail)
        
        pass

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass
