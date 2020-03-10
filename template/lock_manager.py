from template.config import *
from template.rw_lock import *
import threading

class LockManager:
    def __init__(self):
        self.exclusive_locks = dict()
        self._rw_lock = ReaderWriterLock()

    def _update(self, locks, rid, val):
        self._rw_lock.start_write()
        if rid in locks:
            locks[rid] += val
        else:
            locks[rid] = 1
        self._rw_lock.end_write()

    def acquire(self, rid, mode):
        # Acquire read lock
        self._rw_lock.start_read()
        # Cannot acquire exclusive lock
        if rid in self.exclusive_locks and self.exclusive_locks[rid] > 0:
            # print(f"Cannot Acquire lock, rid: {rid}")
            self._rw_lock.end_read()
            return False
        self._rw_lock.end_read()
        if mode == 'R':
            # print(f"Acquiring read lock, rid: {rid}")
            pass
        elif mode == 'W':
            # print(f"Acquiring write lock, rid: {rid}")
            self._update(self.exclusive_locks, rid, 1)
        else:
            return False
        
        return True 

    def release(self, rid, mode):
        if mode == 'R':
            # print(f"Releasing read lock, rid: {rid}")
            pass
        elif mode == 'W':
            # print(f"Releasing write lock, rid: {rid}")
            self._update(self.exclusive_locks, rid, -1)
        else:
            return False
        
        return True 

    def clear_locks(self):
        # remove all locks for serialization
        self.exclusive_locks.clear()
        self._rw_lock = None

    def reset_lock(self):
        # resinstantiate the lock
        if self._rw_lock == None:
            self._rw_lock = ReaderWriterLock()
