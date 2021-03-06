from template.table import *
from template.page import *
from template.page_range import *
import mmap
import collections

# https://www.kunxi.org/2014/05/lru-cache-in-python/
class LRUCache:
    def __init__(self):
        self.cache = collections.OrderedDict()

    def erase(self, key):
        try:
            self.cache.pop(key)
        except KeyError:
            pass

    def get(self, key):
        if key in self.cache:
            return self.cache[key]
        return None

    def set(self, key, val):
        self.erase(key)
        self.cache[key] = val

    def find_lru(self):
        if len(self.cache) == 0:
            return None
        return self.cache.popitem(last=False)

class BufferPoolManager:
    """
    We'll use the Table class as the bufferpool. This class will be used to
    manage the Table class (bufferpool).
    """

    def __init__(self, num_columns, filename):
        # Set of pages that are dirty
        self.dirty_pages = set()
        # Pages that are in use
        self.pinned_pages = dict()
        # Disk Page Number -> Page Range ID
        self.disk_location = dict() 
        # Least Recently Used pages
        self.lru_pages = LRUCache()
        # Filename to store the pages
        self.filename = filename
        # Number of columns including the reserved columns
        self.num_columns = num_columns
        # Used to set dirty pages
        self._dirty_pages_lock = threading.Lock()
        self._lru_lock = threading.Lock()
        self._pinned_pages_lock = threading.Lock()
        self._f_lock = threading.Lock()

    def set_page_dirty(self, pr_id, page_num):
        # Transform the page number to increase with the page range
        # This gives us the disk page range number
        page_num = (pr_id * self.num_columns) + page_num
        with self._dirty_pages_lock:
            self.dirty_pages.add(page_num)

    def is_page_dirty(self, page_num):
        with self._dirty_pages_lock:
            return page_num in self.dirty_pages

    def get_num_pins(self, page_num):
        with self._pinned_pages_lock:
            if page_num not in self.pinned_pages:
                return 0
            return self.pinned_pages[page_num]

    def write_back(self, pages, page_num, pr_id): 
        # Find the index of the page relative to the page range
        # in the bufferpool
        page_relative_idx = page_num - (pr_id * self.num_columns) 
        if (page_relative_idx >= len(pages) or pages[page_relative_idx] == None):
            return

        with self._dirty_pages_lock:
            if page_num in self.dirty_pages:
                self.dirty_pages.remove(page_num)

        with self._f_lock:
            # We have written this page to disk before 
            if page_num in self.disk_location:
                with open(self.filename, "r+b") as f:
                    # Map the file to memory
                    mm = mmap.mmap(f.fileno(), 0)
                    # Get the starting and ending positions of the page
                    start = self.disk_location[page_num][0]
                    # Get the new ending
                    end = start + len(pages[page_relative_idx].data)
                    # update the new ending
                    self.disk_location[page_num] = (start, end)
                    # Write the data to file
                    mm[start:end] = pages[page_relative_idx].data
                    mm.close()
            # We have not written this page to disk before 
            else:
                # append binary code
                with open(self.filename, "ab") as f:
                    # Get the current number of bytes (start)
                    start = f.tell()
                    # Write the data to file
                    f.write(pages[page_relative_idx].data)
                    # Get the current number of bytes (end)
                    end = f.tell()
                    # fill in the rest with 0 bytes
                    remaining_bytes = 4096 - len(pages[page_relative_idx].data)
                    # If we still have space left, write empty bytes til 4096
                    # The idea is we want to have some cushion if the page needs
                    # to be written to disk again and the page is larger than
                    # what it originally was
                    if remaining_bytes > 0:
                        f.write(bytes(remaining_bytes))
                    # Store the offsets of the page 
                    self.disk_location[page_num] = (start, end)
        

        
    def update_page_usage(self, pr_id, page_num):
        # Transform the page number to increase with the page range
        page_num = (pr_id * self.num_columns) + page_num
        with self._lru_lock:
           self.lru_pages.set(page_num, pr_id)

    def find_evict(self):
        """
        Choose a page to evict using LRU policy. Write to disk if page 
        is dirty
        """
        # Find a page to evict. Note that this will remove the page from the
        # cache 
        with self._lru_lock:
            page_pair = self.lru_pages.find_lru()

            if page_pair == None: 
                # print("Could not find page to evict")
                return None

            # We can't evict this page
            if not self.is_page_dirty(page_pair[0]) or self.get_num_pins(page_pair[0]) > 0:
                # Put the pair to the end of our cache
                self.lru_pages.set(page_pair[0], page_pair[1])
                page_pair = None
                
                with self._dirty_pages_lock:
                    for dirty_page_num in self.dirty_pages:
                        pr_id = self.lru_pages.get(dirty_page_num)
                        if pr_id != None and self.get_num_pins(dirty_page_num) == 0:
                            page_pair = (dirty_page_num, pr_id)
                            self.lru_pages.erase(dirty_page_num)
                            break

            return page_pair

    def fetch(self, pr_id, page_num):
        # This gives us the disk page range number
        page_num = (pr_id * self.num_columns) + page_num
        # We assume that if the page is neitehr in the bufferpool
        # nor disk, it's probably an empty page
        with self._f_lock:
            if page_num not in self.disk_location:
                print(f"Page {page_num} not in disk")
                return Page()

            # Read binary mode. + is necessary for mmap to work
            with open(self.filename, "r+b") as f:
                # Map the file to memory
                mm = mmap.mmap(f.fileno(), 0)
                # Get the starting and ending positions of the page
                start = self.disk_location[page_num][0]
                end = self.disk_location[page_num][1]
                # Construct the new page
                page = Page()
                # read the file
                page.data = bytearray(mm[start: end])
                # Get the number of records
                page.num_records = int(len(page.data) / 8)
                mm.close()
                return page
    
    def pin(self, pr_id, page_num):
        # Transform the page number to increase with the page range
        page_num = (pr_id * self.num_columns) + page_num
        # Update the pin count
        with self._pinned_pages_lock:
            if page_num in self.pinned_pages:
                self.pinned_pages[page_num] += 1
            else:
                self.pinned_pages[page_num] = 1

    def unpin(self, pr_id, page_num):
        # Transform the page number to increase with the page range
        page_num = (pr_id * self.num_columns) + page_num
        with self._pinned_pages_lock:
            if page_num in self.pinned_pages:
                self.pinned_pages[page_num] -= 1

    def clear_locks(self):
        self._dirty_pages_lock = None
        self._lru_lock = None
        self._pinned_pages_lock = None 
        self._f_lock = None
        self.pinned_pages.clear()

    def reset_lock(self):
        if self._dirty_pages_lock == None:
            self._dirty_pages_lock = threading.Lock()
        if self._lru_lock == None:
            self._lru_lock = threading.Lock()
        if self._pinned_pages_lock == None:
            self._pinned_pages_lock = threading.Lock()
        if self._f_lock == None:
            self._f_lock = threading.Lock()
