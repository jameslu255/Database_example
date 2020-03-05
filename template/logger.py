LOG_LEVEL = 0

"""
 read/write any transaction updates and writes into logger
"""
"""
    NOTES
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
        
    # write the transaction into the file
    def write(self, tid, command, old_val, new_val):
        with open(self.file_name, 'a') as f:
            transaction = str(tid) + " " + str(command) + " " + str(old_val) + " " + str(new_val) + "\n"
            self.num_transactions += 1
            f.write(transaction)
            
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