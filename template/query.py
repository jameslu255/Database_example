from template.table import *
from template.index import Index
import time

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    """

    def __init__(self, table):
        self.table = table
        Index(self.table)
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
            # Ignores 0 because columns[0] is just the student id.
            if x != 0:
                #subtract 1 from x because we want to start with assignment 1
                Index.add_values(self,x - 1, rid, columns[x])
        
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

        # Get desired columns' page indices
        columns = []
        for i in range(len(query_columns)):
            if query_columns[i] == 1:
                page_index = page_indices[i+4]
                page = self.table.pages[page_index]
                # Get record data from row rid-1 (RIDs start at 1)
                data = page.get_record_int(rid-1)
                columns.append(data)
                print(f"Column {i+4} -> Page Index: {page_index} -> Data: {data}")

        # Return set of columns from the record
        print(f"Return columns: {columns}\n")
        return columns
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

        # If there are no tail pages (i.e. first update performed)
        # initiate new tail pages if tail page array empty
        if len(self.table.pages_tail) == 0: #tail page list empty
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
            # check if a tail record was created for this key in this page
            # check indirection pointer of the rid in the base page

            # get indirection value in base page
            indirection_base_index = self.table.page_directory[rid_base][INDIRECTION_COLUMN]

            indirection_base_page = self.table.pages_base[indirection_base_index]
            indirection_value = indirection_base_page.get_record_int(rid_base)
            indirection = indirection_value

            if(indirection_value != 0): #not a 0 => values has been updated before
                # check schema encoding to see if there's a previous tail page
                # get the latest tail pages
                matching_tail_pages = self.table.tail_page_directory[indirection_value]

                # Get the schema encoding page of the matching tail page
                schema_tail_page_index = matching_tail_pages[SCHEMA_ENCODING_COLUMN] # schema index
                schema_tail_page = self.table.pages_tail[schema_tail_page_index]

                # Get the schema encoding of the latest tail page
                latest_schema = schema_tail_page.get_record_int(indirection_value)

                if latest_schema > 0: #there is at least one column that's updated
                    # create tail pages for everyone
                    tail_page_directory = []
                    self.table.create_page_tail(INDIRECTION_COLUMN)
                    self.table.create_page_tail(RID_COLUMN)
                    self.table.create_page_tail(TIMESTAMP_COLUMN)
                    self.table.create_page_tail(SCHEMA_ENCODING_COLUMN)
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
            base_page_index = self.table.page_directory[rid_base][x]
            base_page = self.table.pages_base[base_page_index]
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
        self.table.update_page_tail(INDIRECTION_COLUMN, indirection)
        self.table.update_page_tail(RID_COLUMN, rid)
        self.table.update_page_tail(TIMESTAMP_COLUMN, timestamp)
        self.table.update_page_tail(SCHEMA_ENCODING_COLUMN, schema_encoding)
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
