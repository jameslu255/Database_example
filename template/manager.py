from template.table import *
import collections

# https://www.kunxi.org/2014/05/lru-cache-in-python/
class LRUCache:
    def __init__(self):
        self.cache = collections.OrderedDict()

    def get(self, key):
        try:
            value = self.cache.pop(key)
            self.cache[key] = value
            return value
        except KeyError:
            return -1

    def set(self, key, value):
        try:
            self.cache.pop(key)
        finally:
            self.cache[key] = value

    def find_lru():
        return self.cache.popitem(last=False)

class Manager:
    """
    We'll use the Table class as the bufferpool. This class will be used to
    manage the Table class (bufferpool).
    """

    def __init__(num_columns):
        self.dirty_pages = [False] * num_columns
        self.lru_pages = LRUCache()

    def set_page_dirty(page_num):
        self.dirty_pages[page_num] = True

    def is_page_dirty(page_num):
        return self.dirty_pages[page_num]

    def write_back():
        pass
    
    def update_page_usage(page_num):
        original_usage = lru_pages.get(page_num)
        if (original_usage == -1):
            lru_pages.set(page_num, 1)
        else:
            lru_pages.set(page_num, original_usage + 1)

    def evict_page():
        """
        Choose a page to evict using LRU policy. Write to disk if page 
        is dirty
        """

        pass

    def pin():
        pass

    def unpin():
        pass

