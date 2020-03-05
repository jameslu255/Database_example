import threading

class AtomicCounter:
    def __init__(self, initial=0):
    """Initialize a new atomic counter to given initial value (default 0)."""
        self.value = initial
        self._rlock = threading.Lock()
        self._wlock = threading.Lock()

    def get(self):

    def add(self, num=1):
        """Atomically increment the counter by num (default 1) and return the
        new value.
        """
        with self._rlock:
            self.value += num
            return self.value

