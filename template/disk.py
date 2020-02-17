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
        open(file_name, 'a+').close()
        
        self.num_table = 0
        
    # write the page range object into the disk file for that specific table
    # def encode_page_range(self, pageRange, table_id):        
        # arr = {}
        
        # # grab any existing page ranges in the disk file
        # if os.stat(self.file_name).st_size > 0: #file not empty
            # arr = self.decode()
        
        # # add the new page range to the array 
        # if table_id in arr: # no key for table_id
            # arr[table_id].append(pageRange)
        # else:
            # arr[table_id] = [pageRange]
        
        # # write the new updated page range to file
        # with open(self.file_name, 'wb') as page_range_file:
            # pickle.dump(arr, page_range_file)    
            
    # write the table's entire page range object into the disk file
    def encode_table(self, table):
        self.num_table += 1
        
        arr = {}
        
        # grab any existing page ranges in the disk file
        if os.stat(self.file_name).st_size > 0: #file not empty
            arr = self.decode()
        
        # add the new page range to the array 
        arr[table.name] = table
        
        # write the new updated page range to file
        with open(self.file_name, 'wb') as page_range_file:
            pickle.dump(arr, page_range_file)
            
    # return an array of page ranges
    def decode(self):
        arr = {}
        
        if os.stat(self.file_name).st_size <= 0: #file not empty
            return {}
            
        #load n amount of page ranges to the empty array
        with open(self.file_name, 'rb') as f:
            arr = pickle.load(f)
            
        return arr
        
    def empty_disk(self):
        open(self.file_name, 'w').close()
        
    # if given page is dirty, update the page in page range
    # def update(self, pageRange, table_id):
        # cur_pr_id = pageRange.id_num
        # arr = self.decode()
        # for i, x in enumerate(arr):
            # if x.id_num == cur_pr_id: #match page range id
                # arr[table_id][i] = pageRange #replace the page page range
        
        
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
# query.update(92106429, *[None, None, None, None, 69])

# records = {}
# for i in range(0, 50):
    # key = 92106429 + i
    # records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    # query_temp.insert(*records[key])
# keys = sorted(list(records.keys()))
# print("Insert finished")
# query_temp.update(92106429, *[None, None, None, None, 69])
# query_temp.update(92106429, *[None, None, None, None, 70])

# # write the page ranges in grades_table into disk 
# d.encode_table(grades_table)
# d.encode_table(table_temp)
# # for pr in grades_table.page_ranges:
    # # d.encode_page_range(pr, grades_table.table_id)
    
# # for pr in table_temp.page_ranges:
    # # d.encode_page_range(pr, table_temp.table_id)

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