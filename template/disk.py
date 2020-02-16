from template.page_range import *
import pickle
import os

# test disk
# from template.db import Database
# from template.query import Query
# from random import choice, randint, sample, seed


"""
 encode page range object into a file called disk
 decode disk file to return an array of page ranges
"""

class Disk:
    def __init__(self, file_name):
        self.file_name = file_name
        
        # create the file if it DNE, empty it if it does
        open(file_name, 'w').close()
        
        self.num_table = 0
        
    # write the page range object into the disk file for that specific table
    def encode_page_range(self, pageRange, table_id):        
        array_page_ranges = {}
        
        # grab any existing page ranges in the disk file
        if os.stat(self.file_name).st_size > 0: #file not empty
            array_page_ranges = self.decode()
        
        # add the new page range to the array 
        if table_id in array_page_ranges:
            array_page_ranges[table_id].append(pageRange)
        else:
            array_page_ranges[table_id] = [pageRange]
        
        # write the new updated page range to file
        with open(self.file_name, 'wb') as page_range_file:
            pickle.dump(array_page_ranges, page_range_file)    
            
    # write the table's entire page range object into the disk file
    def encode_table(self, table):
        self.num_table += 1
        table_id = table.table_id
        
        array_page_ranges = {}
        
        # grab any existing page ranges in the disk file
        if os.stat(self.file_name).st_size > 0: #file not empty
            array_page_ranges = self.decode()
        
        # add the new page range to the array 
        array_page_ranges[table_id] = table.page_ranges
        
        # write the new updated page range to file
        with open(self.file_name, 'wb') as page_range_file:
            pickle.dump(array_page_ranges, page_range_file)
            
    # return an array of page ranges
    def decode(self):
        array_page_ranges = {}
        
        #load n amount of page ranges to the empty array
        with open(self.file_name, 'rb') as page_range_file:
            array_page_ranges = pickle.load(page_range_file)
            
        return array_page_ranges
        
    # if given page is dirty, update the page in page range
    def update(self, pageRange, table_id):
        cur_pr_id = pageRange.id_num
        array_page_ranges = self.decode()
        for i, x in enumerate(array_page_ranges):
            if x.id_num == cur_pr_id: #match page range id
                array_page_ranges[table_id][i] = pageRange #replace the page page range
        
        
# # test disk
# d = Disk("disk")
# db = Database()
# grades_table = db.create_table('Grades', 5, 0)
# table_temp = db.create_table('test', 5, 0)
# query = Query(grades_table)
# query_temp = Query(table_temp)

# # insert things so there are at least two pages
# records = {}
# seed(3562901)
# for i in range(0, 1000):
    # key = 92106429 + i
    # records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    # query.insert(*records[key])
# keys = sorted(list(records.keys()))

# records = {}
# for i in range(0, 50):
    # key = 92106429 + i
    # records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    # query_temp.insert(*records[key])
# keys = sorted(list(records.keys()))
# print("Insert finished")

# # write the page ranges in grades_table into disk 
# for pr in grades_table.page_ranges:
    # d.encode_page_range(pr, grades_table.table_id)
    
# for pr in table_temp.page_ranges:
    # d.encode_page_range(pr, table_temp.table_id)

# # read the page ranges in disk
# print("----------------base page-------------------------")
# print("         0          1          2          3          4          5          6          7          8")
# table_dict = d.decode()
# for key in table_dict:
    # print("table id is " + str(key))
    # for (i,y) in enumerate(table_dict[key]):
        # print("page range #" + str(i))
        # for x in range(y.base_pages[0].num_records):
            # for (page_num, page) in enumerate(y.base_pages):
                # byteval = page.data[x*8:(x*8 + 8)]
                # val = int.from_bytes(byteval, "big")
                # print("{0: 10d}".format(val), end = ' ')
            # print()