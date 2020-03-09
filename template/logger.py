import os
import shutil

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
    def __init__(self, file_name):
        self.file_name = file_name
        
        # create the file if it DNE, empty it if it does
        open(file_name, 'a+').close()
        
        self.num_transactions = 0
        
        # grab the number of transactions in logger rn, else init
        if os.stat(self.file_name).st_size > 0: #file not empty
            # grab the first line of the file -> number of transactions
            with open(self.file_name,'r') as f:
                first_line = f.readline().strip()
                self.num_transactions = int(first_line)
        else: # file is empty, set zero for first line
            with open(self.file_name,'a') as f:
                f.write("0\n")
          
    def getNumTransaction(self):
        # num = 0
        # if os.stat(self.file_name).st_size > 0: #file not empty
            # with open(self.file_name,'r') as f:
                    # first_line = f.readline().strip()
                    # num = int(first_line)
        # return num
        return self.num_transactions
    
    # write the transaction into the file
    def write(self, tid, command, old_val, new_val, bid):
        with open(self.file_name, 'a') as f:
            transaction = str(tid) + " " + str(command) + " " + str(old_val) + " " + str(new_val) + " " + str(bid) + "\n"
            f.write(transaction)
            
    def commit(self, tid):
        with open(self.file_name, 'a') as f:
            s = str(tid) + " " + "commited\n"
            f.write(s)
        
        
        # update num transaction count on top of file
        self.num_transactions += 1
        print("tid in looger", tid, self.num_transactions)
        from_file = open(self.file_name)
        l = from_file.readline()
        l = str(self.num_transactions) + "\n"
        to_file   = open(self.file_name,mode='w')
        to_file.write(l)
        shutil.copyfileobj(from_file,to_file)
            
    def abort(self, tid):
        with open(self.file_name, 'a') as f:
            s = str(tid) + " " + "aborted\n"
            f.write(s)
            
        # update num transaction count on top of file
        self.num_transactions += 1
        from_file = open(self.file_name)
        l = from_file.readline()
        l = str(self.num_transactions) + "\n"
        to_file   = open(self.file_name,mode='w')
        to_file.write(l)
        shutil.copyfileobj(from_file,to_file)
        
    # read all the transactions from the newest to oldest
    def read(self):
        # read lines bottom up
        for line in reversed(list(open(self.file_name))):
            print(line.rstrip())
            
    # read all the transactions associated with a transaction id
    def read_tid(self, tid):
        transactions = []
        # read lines bottom up
        for line in reversed(list(open(self.file_name))):
            s = line.rstrip()
            arg = line.rstrip().split()
            if arg[0] == str(tid):
                transactions.append(s)
                print(s)
        
        return transactions
        
    # read all the transactions associated with these transaction ids
    def read_tids(self, tids):
        transactions = []
        x = 0
        # read lines bottom up
        for line in reversed(list(open(self.file_name))):
            if x >= len(tids):
                return transactions
                
            s = line.rstrip()
            arg = line.rstrip().split()
            if arg[0] == str(tids[x]):
                x += 1
                transactions.append(s)
                print(s)
        
        return transactions