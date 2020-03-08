from template.config import *
from template.counter import *
import threading

class LockManager:
    def __init__(self):
        self.shared_locks = dict()
        self.exclusive_locks = dict()
        self._lock = threading.Lock()

    def _update(self, locks, rid, val):
        if rid in locks:
            locks[rid].add(val)
        else:
            with self._lock:
                locks[rid] = AtomicCounter(initial = 1)

    def acquire(self, rid, mode):
        # Cannot acquire lock
        if rid in self.exclusive_locks and self.exclusive_locks[rid].value > 0:
            print(f"Cannot Acquire lock, rid: {rid}")
            return False
        if mode == 'R':
            # print(f"Acquiring read lock, rid: {rid}")
            self._update(self.shared_locks, rid, 1)
        elif mode == 'W':
            # print(f"Acquiring write lock, rid: {rid}")
            self._update(self.exclusive_locks, rid, 1)
        else:
            return False
        
        return True 

    def release(self, rid, mode):
        if mode == 'R':
            # print(f"Releasing read lock, rid: {rid}")
            self._update(self.shared_locks, rid, -1)
        elif mode == 'W':
            # print(f"Releasing write lock, rid: {rid}")
            self._update(self.exclusive_locks, rid, -1)
        else:
            return False
        
        return True 

    def clear_locks(self):
        self.shared_locks.clear()
        self.exclusive_locks.clear()
        self._lock = None

    def reset_lock(self):
        if self._lock == None:
            self._lock = threading.Lock()
