from template.table import *
from template.page import *
import mmap
import collections

# https://www.kunxi.org/2014/05/lru-cache-in-python/
class LRUCache:
    def __init__(self):
        self.cache = collections.OrderedDict()
        self.age = 0

    def set(self, key):
        self.age += 1
        try:
            self.cache.pop(key)
        finally:
            self.cache[key] = self.age 

    def find_lru():
        return self.cache.popitem(last=False)

class BufferPoolManager:
    """
    We'll use the Table class as the bufferpool. This class will be used to
    manage the Table class (bufferpool).
    """

    def __init__(num_columns, filename):
        self.dirty_pages = set()
        self.pinned_pages = dict()
        self.disk_location = {}
        self.lru_pages = LRUCache()
        self.filename = filename
        self.num_columns = num_columns

    def set_page_dirty(self, pr_id, page_num):
        page_num = (pr_id * self.num_columns) + page_num
        self.dirty_pages.add(page_num)

    def is_page_dirty(self, page_num):
        page_num = (pr_id * self.num_columns) + page_num
        return page_num in self.dirty_pages

    def get_num_pins(self, page_num):
        page_num = (pr_id * self.num_columns) + page_num
        return self.pinned_pages[page_num]

    def write_back(self, pages, page_num): 
        page_num = (pr_id * self.num_columns) + page_num
        # We have written this page to disk before 
        if page_num in disk_location:
            with open(self.filename, "r+b") as f:
                # Map the file to memory
                mm = mmap.mmap(f.fileno(), 0)
                # Get the starting and ending positions of the page
                start = self.disk_location[page_num][0]
                # Get the new ending
                end = start + len(pages[page_num].data)
                # update the new ending
                self.disk_location[page_num][1] = end
                # Write the data to file
                mm[start:end] = pages[page_num].data
                mm.close()
        # We have not written this page to disk before 
        else:
            # append binary code
            with open(self.filename, "ab") as f:
                # Get the current number of bytes (start)
                start = f.tell()
                # Write the data to file
                f.write(pages[page_num].data)
                # Get the current number of bytes (end)
                end = f.tell()
                # fill in the rest with 0 bytes
                remaining_bytes = 4096 - len(pages[page_num].data)
                # If we still have space left, write empty bytes til 4096
                # The idea is we want to have some cushion if the page needs
                # to be written to disk again and the page is larger than
                # what it originally was
                if remaining_bytes > 0:
                    f.write(bytes(remaining_bytes))
                # Store the offsets of the page 
                self.disk_location[page_num] = (start, end)
                mm.close()
        
    def update_page_usage(self, page_num):
        page_num = (pr_id * self.num_columns) + page_num
        self.lru_pages.set(page_num)

    def evict(self, pages):
        """
        Choose a page to evict using LRU policy. Write to disk if page 
        is dirty
        """
        # Find a page to evict. Note that this will remove the page from the
        # cache 
        page_age_pair = self.find_lru()
        if page_age_pair == None: 
            print("Could not find page to evict")
            return -1

        # The LRU page number
        page_num = page_age_pair[0]
        page_num = (pr_id * self.num_columns) + page_num
        # if page is dirty and not in use, write to disk
        if self.is_page_dirty(page_num) and self.get_num_pins(page_num) == 0:
            # Write the page to disk
            self.write_back(pages, page_num)
            # Maybe sent the value in pages to None, so we know that the page is
            # no longer in the bufferpool
            pages[page_num] = None
            # Page is no longer dirty
            self.dirty_pages.remove(page_num)
        else:
            if self.get_num_pins(page_num) != 0:
                print(f"Cannot evict. Page {page_num} is pinned")
            else:
                print(f"Cannot evict. Page {page_num} is not dirty")
            return -1

        return page_num 

    def fetch(self, pr_id, page_num):
        # Read binary mode. + is necessary for mmap to work
        with open(self.filename, "r+b") as f:
            page_num = (pr_id * self.num_columns) + page_num
            # Map the file to memory
            mm = mmap.mmap(f.fileno(), 0)
            # Get the starting and ending positions of the page
            start = self.disk_location[page_num][0]
            end = self.disk_location[page_num][1]
            # Construct the new page
            page = Page()
            # read the file
            page.data = mm[start: end]
            # Get the number of records
            page.num_records = len(page.data) / 8
            mm.close()
            return page
    
    def pin(self, page_num):
        page_num = (pr_id * self.num_columns) + page_num
        if page_num in pinned_pages:
            self.pinned_pages[page_num] += 1
        else:
            self.pinned_pages[page_num] = 1

    def unpin(self, page_num):
        self.pinned_pages[page_num] -= 1 
