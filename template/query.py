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
        self.table.records += 1

        page_directory_indexes = []
        record = Record(self.table.records, columns[0], columns)
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
        self.table.keys[record.key] = self.table.records
        # RID -> page_index
        for x in range(len(record.columns) + 4):
            page_directory_indexes.append(self.table.free_pages[x])
        self.table.page_directory[self.table.records] = page_directory_indexes
        # [self.table.free_pages[i] for i in range(record.columns) + 4]
        pass

    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        # key = SID
        # query_columns = columns we are interested in
        # query.select(choice(keys), [1, 1, 1, 1, 1])
        # 906659671 [1, 1, 1, 1, 1]
        # SID = 906659671, columns wanted = key, g1, g2, g3, g4

        print(f"Select: SID = {key} {query_columns}")
        #print(f"query_columns: {query_columns}")

        # Find RID from key, keys = {SID: RID}
        rid = self.table.keys[key]
        print(f"Found RID: {rid}")

        # Find physical pages' indices for RID from page_directory [RID:[x x x x x]]
        page_indices = self.table.page_directory[rid]
        print(f"Found pages: {page_indices}")

        # Get desired columns page indices
        for i in range(len(query_columns)):
            if query_columns[i] == 1:
                page = page_indices[i+4]
                print(f"Column {i+4} -> Page: {page}")
        print(f"\n")

        # Return records?

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
