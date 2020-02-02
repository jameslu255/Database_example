from template.page import *
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

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
        # SID -> RID
        self.keys = {}
        # RID -> Page_base
        self.page_directory = {}
        # RID -> Page_tail
        self.page_tail_directory = {}
        # page array - base
        self.pages = []
        # page index array - base
        self.free_pages = []
        # page array - tail
        self.pages_tail = []
        # page index array - tail
        self.free_pages_tail = []
        # number of records a table has
        self.records = 0
        # tail page id 
        self.tail_rid = 1
        
        # create pages for the indirection, rid, timestamp, schema encoding column
        self.create_page("indirection") #index 0
        self.create_page("rid") #index 1
        self.create_page("timestamp")#index 2
        self.create_page("schema")#index 3
        
        # self.create_page_tail("indirection") #index 0
        # self.create_page_tail("rid") #index 1
        # self.create_page_tail("timestamp")#index 2
        # self.create_page_tail("schema")#index 3
        
        # create pages for the key and the data columns
        for x in range(num_columns):
            self.create_page(x)
            # self.create_page_tail(x)
        pass

    def __merge(self):
        pass
        
    def create_page_tail(self, col):
        print("creating new tail page for " + str(col))
        # create the page and push to array holding pages
        self.pages_tail.append(Page())
        # keep track of index of page relative to aray index
        if(len(self.free_pages_tail) < self.num_columns + 4): #when initializing
            self.free_pages_tail.append(len(self.pages_tail) - 1)
        else: # when creating new page and need to update the index 
            self.free_pages_tail[col] = len(self.pages_tail) - 1
        
    def update_page_tail(self, col, value):
        # update the page linked to the col
        print(str(col) + " writing val: " + str(value) + " of type " + str(type(value)))
        index_relative = self.free_pages_tail[col]
        pg = self.pages_tail[index_relative]
        error = pg.write(value)
        if error == -1: # maximum size reached in page
            print("col:" + str(col) + " in page " + str(index_relative) + " is full, making a new one")
            # create new page
            page = Page()
            # write to new page
            page.write(value)
            # append the new page
            self.pages_tail.append(page)
            # update free page index to point to new blank page
            self.free_pages_tail[col] = len(self.pages_tail) - 1
            # self.free_pages[col].append(len(pages) - 1)
    
    def update_base_rid(self, column_index, rid, value):
        if(column_index < 0 or column_index > self.num_columns):
            print("updating a rid in base " + str(column_index) + " out of bounds")
        base_page_index = self.free_pages[column_index]
        self.pages[base_page_index].set_record_byte(rid, value)
    
    def create_page(self, col_name):
        print("creating new page for " + str(col_name))
        # create the page and push to array holding pages
        self.pages.append(Page())
        # keep track of index of page relative to aray index
        self.free_pages.append(len(self.pages) - 1)
        # self.page_directory[col_name] = Page()
        
    def update_page(self, index, value):
        # update the page linked to the index
        index_relative = self.free_pages[index]
        error = self.pages[index_relative].write(value)
        if error == -1: # maximum size reached in page
            print("col:" + str(index) + " in page " + str(index_relative) + " is full, making a new one")
            # create new page
            page = Page()
            # write to new page
            page.write(value)
            # append the new page
            self.pages.append(page)
            # update free page index to point to new blank page
            self.free_pages[index] = len(self.pages) - 1
            # self.free_pages[index].append(len(pages) - 1)

 
