from template.config import *
from template.counter import *

class LockManager:
    def __init__(self):
        self.shared_locks = dict()
        self.exclusive_locks = dict()

    def _update(self, locks, rid, mode, val):
        if rid in locks:
            locks[rid].add(val)
        else:
            locks[rid] = AtomicCounter(initial = 1)

    def acquire(self, rid, mode):
        # Cannot acquire lock
        if rid in self.exclusive_locks and self.exclusive_locks[rid].value > 0:
            return False
        if mode == 'R':
            self._update(self.shared_locks, rid, mode, 1)
        elif mode == 'W':
            self._update(self.exclusive_locks, rid, mode, 1)
        else:
            return False
        
        return True 

    def release(self, rid, mode):
        if mode == 'R':
            self._update(self.shared_locks, rid, mode, -1)
        elif mode == 'W':
            self._update(self.exclusive_locks, rid, mode, -1)
        else:
            return False
        
        return True 
