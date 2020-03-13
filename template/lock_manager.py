from template.config import *
from template.rw_lock import *
import threading

class LockManager:
    def __init__(self):
        self.exclusive_locks = dict()
        self.shared_locks = dict()
        self.lock = threading.Lock()

    def _contains(self, locks, key):
        with self.lock:
            doesContain = key in locks
            return doesContain

    def _get(self, locks, key):
        with self.lock:
            val = locks[key]
            return val 

    def _update(self, locks, rid, val):
        if self._contains(locks, rid):
            with self.lock:
                locks[rid] += val
        else:
            with self.lock:
                locks[rid] = 1

    def lock_exists(self, locks, rid):
        return (self._contains(locks, rid) and
            self._get(locks, rid) > 0)

    def acquire(self, rid, mode):
        # Acquire read lock
        # Cannot acquire exclusive lock
        if mode == 'R':
            if self.lock_exists(self.exclusive_locks, rid):
                # print(f"Cannot Acquire lock, rid: {rid}")
                return False
            else:
                self._update(self.shared_locks, rid, 1)
        elif mode == 'W':
            if (self.lock_exists(self.shared_locks, rid) or
                self.lock_exists(self.exclusive_locks, rid)):
                # print(f"Acquiring write lock, rid: {rid}")
                return False
            else:
                self._update(self.exclusive_locks, rid, 1)
        else:
            return False
        
        return True 

    def release(self, rid, mode):
        if mode == 'R':
            # print(f"Releasing read lock, rid: {rid}")
            assert(self._contains(self.shared_locks, rid)), ("Cannot release unacquired shared lock")
            self._update(self.shared_locks, rid, -1)
        elif mode == 'W':
            assert(self._contains(self.exclusive_locks, rid)), ("Cannot release unacquired exclusive lock")
            # print(f"Releasing write lock, rid: {rid}")
            self._update(self.exclusive_locks, rid, -1)
        else:
            return False
        
        return True 

    def clear_locks(self):
        # remove all locks for serialization
        self.exclusive_locks.clear()
        self.shared_locks.clear()
        # self._rw_lock.clear_locks()
        self.lock = None

    def reset_lock(self):
        # resinstantiate the lock
        if self.lock == None:
            self.lock = threading.Lock()
