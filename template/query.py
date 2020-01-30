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
        page_directory_indexes = []
        record = Record(self.table.records + 1, columns[0], columns)
        schema_encoding = 0 # '0' * self.table.num_columns
        timestamp = int(time.time())
        rid = record.rid
        indirection = 0 # None
        
        # Write to the pagse
        self.table.update_page(INDIRECTION_COLUMN, indirection)
        self.table.update_page(RID_COLUMN, rid)
        self.table.update_page(TIMESTAMP_COLUMN, timestamp)
        self.table.update_page(SCHEMA_ENCODING_COLUMN, schema_encoding)
        
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
        pass

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass
