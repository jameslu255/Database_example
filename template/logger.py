import os
import shutil
import threading
from template.counter import *


LOG_LEVEL = 0

"""
 read/write any transaction updates and writes into logger
"""
"""
    NOTES
    -  the DBMS maintains a log in which every write is recorded.
    - if Tj reads an object last written by Ti, Tj must
    be aborted as well!
    - all active Xacts at the time of the crash are aborted when the
    system comes back up.
    - If any base/tail record created as a result of an aborted transaction 
    need not be removed from the database, they can just be marked as deleted.
"""
class Logger:
    """ STATIC VARIABLES """
    num_transactions = AtomicCounter()
    num_readers = 0
    sem_writer = threading.Semaphore()
    sem_reader = threading.Semaphore()
    
    def __init__(self, file_name):
        self.file_name = file_name
        
        # We will use this variable to save and restore num_transactions since
        # static variables are unserializable
        # Private
        self._saved_num_transactions = 0
        
        # create the file if it DNE, empty it if it does
        open(self.file_name, 'a+').close()
        
        
        # grab the number of transactions in logger rn, else init
        """
        if os.stat(self.file_name).st_size > 0: #file not empty
            # grab the first line of the file -> number of transactions
            with open(self.file_name,'r') as f:
                first_line = f.readline().strip()
                num_transactions = AtomicCounter(int(first_line))
        else: # file is empty, set zero for first line
            with open(self.file_name,'a') as f:
                f.write("0\n")
        """
          
    def getNumTransaction(self):
        # num = 0
        # if os.stat(self.file_name).st_size > 0: #file not empty
            # with open(self.file_name,'r') as f:
                    # first_line = f.readline().strip()
                    # num = int(first_line)
        # return num
        return Logger.num_transactions.value
    
    # write the transaction into the file
    def write(self, tid, command, old_val, new_val, bid):
        # writer requests for critical section
        self.sem_writer.acquire()
        with open(self.file_name, 'a') as f:
            transaction = str(tid) + " " + str(command) + " " # + str(old_val) + " " + str(new_val) + " " + str(bid) + "\n"
            # print(transaction)
            for o in old_val:
                transaction += str(o) + ","

            transaction += " "
            # print(transaction)
            for n in new_val:
                transaction += str(n) + ","

            transaction += " "

            transaction += str(bid) + "\n"
            f.write(transaction)
            
            
            f.write(transaction)
        # leaves the critical section       
        self.sem_writer.release()
            
    def commit(self, tid):
        # writer requests for critical section
        self.sem_writer.acquire()
        with open(self.file_name, 'a') as f:
            s = str(tid) + " " + "commited\n"
            f.write(s)
        # leaves the critical section
        self.sem_write.release()
        
        # print("tid in looger", tid, Logger.num_transactions.value)
        """
        from_file = open(self.file_name)
        l = from_file.readline()
        l = str(num_transactions) + "\n"
        to_file   = open(self.file_name,mode='w')
        to_file.write(l)
        shutil.copyfileobj(from_file,to_file)
        """
            
    def abort(self, tid):
    # writer requests for critical section
    self.sem_writer.acquire()
        with open(self.file_name, 'a') as f:
            s = str(tid) + " " + "aborted\n"
            f.write(s)
    # leaves the critical section
    self.sem_writer.release()
            
        """
        from_file = open(self.file_name)
        l = from_file.readline()
        l = str(num_transactions) + "\n"
        to_file   = open(self.file_name,mode='w')
        to_file.write(l)
        shutil.copyfileobj(from_file,to_file)
        """
    
    # def last_abort(self):
        # # read lines bottom up
        # for line in reversed(list(open(self.file_name))):
            # s = line.rstrip()
            # arg = line.rstrip().split()
            # if arg[1] == "aborted":
                # return arg[0]
                
        # return -1
                    
    # # read all the transactions from the newest to oldest
    # def read(self):
        # # read lines bottom up
        # for line in reversed(list(open(self.file_name))):
            # print(line.rstrip())
            
    # read all the transactions associated with a transaction id
    def read_tid(self, tid):
        # Reader wants to enter the critical section
        self.sem_reader.acquire()
        # The number of readers has now increased by 1
        Logger.num_readers += 1
        # there is atleast one reader in the critical section
        # this ensure no writer can enter if there is even one reader
        # thus we give preference to readers here
        if(Logger.num_readers == 1):
            self.sem_writer.acquire()
        # other readers can enter while this current reader is inside 
        # the critical section
        self.sem_reader.release()
            
        transactions = []
        # read lines bottom up
        for line in reversed(list(open(self.file_name))):
            s = line.rstrip()
            arg = line.rstrip().split()
            if arg[0] == str(tid):
                if arg[1] != "aborted" and arg[1] != "commited":
                    transactions.append(s)
                    # print(s)
        self.sem_reader.acquire() # reader ready to leave
        Logger.num_readers -= 1
        # that is, no reader is left in the critical section,
        if(Logger.num_readers == 0):
            self.sem_writer.release() # writer can enter
        self.sem_reader.release() # reader leave
        
        return transactions
        
    # read all the transactions associated with these transaction ids
    # def read_tids(self, tids):
        # transactions = []
        # x = 0
        # # read lines bottom up
        # for line in reversed(list(open(self.file_name))):
            # if x >= len(tids):
                # return transactions
                
            # s = line.rstrip()
            # arg = line.rstrip().split()
            # if arg[0] == str(tids[x]):
                # x += 1
                # transactions.append(s)
                # # print(s)
        
        # return transactions

    def counters_to_int(self):
        # if counter is an AtomicCounter, store int for deserialization
        if isinstance(Logger.num_transactions, AtomicCounter):
            self._saved_num_transactions = Logger.num_transactions.value
            Logger.num_transactions = None

    def reset_counters(self):
        # if counter is an int, convert to AtomicCounter during deserialization
        if isinstance(self._saved_num_transactions, int):
            self._saved_num_transactions = AtomicCounter(Logger.num_transactions)
