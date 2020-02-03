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
        self.base_page_directory = {}
        # RID -> Page_tail
        self.tail_page_directory = {}
        # page array - base
        self.base_pages = []
        # page index array - base
        self.free_base_pages = []
        # page array - tail
        self.tail_pages = []
        # page index array - tail
        self.free_tail_pages = []
        # number of records a table has
        self.base_rid = 0
        # tail page id 
        self.tail_rid = 0
        
        # create pages for the indirection, rid, timestamp, schema encoding column
        self.create_base_page("indirection") #index 0
        self.create_base_page("rid") #index 1
        self.create_base_page("timestamp")#index 2
        self.create_base_page("schema")#index 3
        
        # create pages for the key and the data columns
        for x in range(num_columns):
            self.create_base_page(x)
        pass

    def __merge(self):
        pass
        
    def create_tail_page(self, col):
        # create the page and push to array holding pages
        self.tail_pages.append(Page())
        # keep track of index of page relative to aray index
        if(len(self.free_tail_pages) < self.num_columns + 4): #when initializing
            self.free_tail_pages.append(len(self.tail_pages) - 1)
        else: # when creating new page and need to update the index 
            self.free_tail_pages[col] = len(self.tail_pages) - 1
        
    def update_tail_page(self, col, value):
        # update the page linked to the col
        print(str(col) + " writing val: " + str(value) + " of type " + str(type(value)))
        index_relative = self.free_tail_pages[col]
        pg = self.tail_pages[index_relative]
        error = pg.write(value)
        if error == -1: # maximum size reached in page
            print("col:" + str(col) + " in page " + str(index_relative) + " is full, making a new one")
            # create new page
            page = Page()
            # write to new page
            page.write(value)
            # append the new page
            self.tail_pages.append(page)
            # update free page index to point to new blank page
            self.free_tail_pages[col] = len(self.tail_pages) - 1
            # self.free_base_pages[col].append(len(pages) - 1)
    
    def update_tail_rid(self, column_index, rid, value):
        if(column_index < 0 or column_index > self.num_columns):
            print("updating a rid in base " + str(column_index) + " out of bounds")
        print("updating tail rid " + str(rid) + " @ col " + str(column_index) + " with value: " + str(value))
        # tail_page_index = self.free_tail_pages[column_index]
        # print("tail page index: " + str(tail_page_index) + " accessed")
        self.tail_pages[column_index].set_record(rid, value)
        
    def update_base_rid(self, column_index, rid, value):
        if(column_index < 0 or column_index > self.num_columns):
            print("updating a rid in base " + str(column_index) + " out of bounds")
        print("updating rid " + str(rid) + " @ col " + str(column_index) + " with value: " + str(value))
        base_page_index = self.free_base_pages[column_index]
        self.base_pages[base_page_index].set_record(rid, value)
    
    def create_base_page(self, col_name):
        print("creating new page for " + str(col_name))
        # create the page and push to array holding pages
        self.base_pages.append(Page())
        # keep track of index of page relative to aray index
        self.free_base_pages.append(len(self.base_pages) - 1)
        # self.base_page_directory[col_name] = Page()
        
    def update_base_page(self, index, value):
        # update the page linked to the index
        index_relative = self.free_base_pages[index]
        error = self.base_pages[index_relative].write(value)
        if error == -1: # maximum size reached in page
            print("col:" + str(index) + " in page " + str(index_relative) + " is full, making a new one")
            # create new page
            page = Page()
            # write to new page
            page.write(value)
            # append the new page
            self.base_pages.append(page)
            # update free page index to point to new blank page
            self.free_base_pages[index] = len(self.base_pages) - 1
            # self.free_base_pages[index].append(len(pages) - 1)

 
