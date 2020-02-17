from template.table import *
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

    def __init__(num_columns):
        self.dirty_pages = [False] * num_columns
        self.pinned_pages = [0] * num_columns
        self.lru_pages = LRUCache()

    def set_page_dirty(self, page_num):
        self.dirty_pages[page_num] = True

    def is_page_dirty(self, page_num):
        return self.dirty_pages[page_num]

    def get_num_pins(self, page_num):
        return self.pinned_pages[page_num]

    def write_back(self, pages, page_num):
        pass
    
    def update_page_usage(self, page_num):
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
        # if page is dirty and not in use, write to disk
        if self.is_page_dirty(page_num) and self.get_num_pins(page_num) == 0:
            self.write_back(pages, page_num)
            # Maybe sent the value in pages to None, so we know that the page is
            # no longer in the bufferpool
            pages[page_num] = None
        else:
            if self.get_num_pins(page_num) != 0:
                print(f"Cannot evict. Page {page_num} is pinned")
            else:
                print(f"Cannot evict. Page {page_num} is not dirty")
            return -1

        return page_num 

    def pin(self, page_num):
        if page_num == len(self.pinned_pages):
            self.pinned_pages.append(0)
        elif page_num < 0 or page_num > len(self.pinned_pages):
            print(f"Cannot pin page {page_num}")
        else:
            self.pinned_pages[page_num] += 1

        
    def unpin(self, page_num):
        if page_num < 0 or page_num > len(self.pinned_pages):
            print(f"Cannot pin page {page_num}")
        else:
            self.pinned_pages[page_num] -= 1 
        

