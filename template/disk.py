import pickle
import os
from template.page_range import *

"""
 encode page range object into a file called disk
 decode disk file to return an array of page ranges
"""

class Disk:
    def __init__(self, file_name):
        self.file_name = file_name
        
        # create the file if it DNE, empty it if it does
        open(file_name, 'w').close()
        
        self.num_pr = 0
        
    # write the page range object into the disk file
    def encode_append(self, pageRange):
        self.num_pr += 1
        array_page_ranges = []
        
        # grab any existing page ranges in the disk file
        if os.stat(self.file_name).st_size > 0: #file not empty
            array_page_ranges = self.decode()
        
        # add the new page range to the array 
        array_page_ranges.append(pageRange)
        
        # write the new updated page range to file
        with open(self.file_name, 'wb') as page_range_file:
            pickle.dump(array_page_ranges, page_range_file)
            
    # return an array of page ranges
    def decode(self):
        array_page_ranges = []
        
        #load n amount of page ranges to the empty array
        with open(self.file_name, 'rb') as page_range_file:
            array_page_ranges = pickle.load(page_range_file)
            
        return array_page_ranges
        
    # if given page is dirty, update the page in page range
    def update(self, pageRange):
        cur_pr_id = pageRange.id_num
        array_page_ranges = self.decode()
        for x in array_page_ranges:
            if x.id_num == cur_pr_id: #match page range id
                x = pageRange
        
        
d = Disk("disk")
d.encode_append(PageRange(1,8))
d.encode_append(PageRange(2,8))
d.encode_append(PageRange(3,8))
for x in d.decode():
    print("page range id: " + str(x.id_num))