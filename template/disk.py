from template.page_range import *
import pickle
import os


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

        # clear out any existing locks jecause they are unserializable
        table.lock_manager.clear_locks()
        table.base_page_manager.clear_locks()
        table.tail_page_manager.clear_locks()
        table.counters_to_int()
        
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
