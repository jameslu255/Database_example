from template.logger import Logger
from template.transaction import Transaction
from template.transaction_worker import TransactionWorker
from template.query import Query
from template.db import Database
import threading
import inspect

# l = Logger("log")
# l.write(1,"w",0,5)
# l.write(2,"u",5,50)
# l.write(2,"u",3,90)
# l.write(3,"u",9,50)
# print("reading all the lines from newest to oldest")
# l.read()
# print("reading all the lines of thread id 2")
# print(l.read_tid(2))
db = Database()

grades_table = db.create_table('Grades', 5, 0)
#populate the table

q = Query(grades_table)
# print(inspect.getfullargspec(q.update))
t1 = Transaction()
t2 = Transaction()
t1.add_query(q.insert, *[1, 2, 3, 4, 5])
t1.add_query(q.insert, *[0, 1, 2, 2, 5])
t2.add_query(q.insert, *[6, 7, 8, 9, 10])
txn_worker = TransactionWorker([t1,t2])
th1 = threading.Thread(target=txn_worker.run)
th1.start()

# l = Logger("log")