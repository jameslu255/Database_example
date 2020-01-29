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
        # RID -> Page
        self.page_directory = {}
        # page array
        self.pages = []
        # page index array
        self.free_pages = []
        # number of records a table has
        self.records = 0
        
        # create pages for the indirection, rid, timestamp, schema encoding column
        self.create_page("indirection") #index 0
        self.create_page("rid") #index 1
        self.create_page("timestamp")#index 2
        self.create_page("schema")#index 3
        
        # create pages for the key and the data columns
        for x in range(num_columns):
            self.create_page(x)
        pass

    def __merge(self):
        pass
        
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
            print("page for " + str(index) + " is full, making a new one")
            # create new page
            self.pages.append(Page())
            # update free page index to point to new blank page
            self.free_pages[index] = len(self.pages) - 1
            # self.free_pages[index].append(len(pages) - 1)
 
