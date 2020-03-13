import threading
# https://arxiv.org/ftp/arxiv/papers/1309/1309.4507.pdf 
class ReaderWriterLock:
    def __init__(self):
        self._in = threading.Semaphore(1)
        self._out = threading.Semaphore(1)
        self._writer = threading.Semaphore(0)
        self._ctrin = 0
        self._ctrout = 0
        self._wait = False

    def start_read(self):
        self._in.acquire()
        self._ctrin += 1
        self._in.release()

    def end_read(self):
        self._out.acquire()
        self._ctrout += 1
        if (self._wait and self._ctrin == self._ctrout):
            self._writer.release()
        self._out.release()

    def start_write(self):
        self._in.acquire()
        self._out.acquire()
        if (self._ctrin == self._ctrout):
            self._out.release()
        else:
            self._wait = True
            self._out.release()
            self._writer.acquire()
            self._wait = False

    def end_write(self):
        self._in.release()

    def clear_locks(self):
        self._in = None
        self._out = None
        self._writer = None
        self._ctrin = 0
        self._ctrout = 0
        self._wait = False

    def reset_locks(self):
        if self._in == None:
            self._in = threading.Semaphore(1)
        if self._out == None:
            self._out = threading.Semaphore(1)
        if self._writer == None:
            self._writer = threading.Semaphore(0)       
        self._ctrin = 0
        self._ctrout = 0
        self._wait = False
