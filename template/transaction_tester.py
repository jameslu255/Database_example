from template.db import Database
from template.query import Query
from template.transaction import Transaction
from template.transaction_worker import TransactionWorker

import threading
from random import choice, randint, sample, seed

# Header Constants
PAGE_RANGE = "PAGE RANGE "
BASE_PAGES = "Base Pages"
TAIL_PAGE = "Tail Page "
INDIRECTION = "indirection"
RID = "RID"
TIME = "time"
SCHEMA = "schema"
TPS = "TPS"
BASE_RID = "Base RID"
KEY = "key"
G1 = "G1"
G2 = "G2"

def print_header_line(count):
    for j in range(count):
        print("_", end='')
    print()


db = Database()
db.open('ECS165')
grades_table = db.create_table('Grades', 5, 0)

keys = []
records = {}
# num_threads = 8
num_threads = 1
seed(8739878934)

# Generate random records
for i in range(0, 10000):
# for i in range(0, 10):
    key = 92106429 + i
    keys.append(key)
    records[key] = [key, 0, 0, 0, 0]
    q = Query(grades_table)
    q.insert(*records[key])

# create TransactionWorkers
transaction_workers = []
for i in range(num_threads):
    transaction_workers.append(TransactionWorker([]))

# generates 10k random transactions
# each transaction will increment the first column of a record 5 times
# for i in range(1000):
for i in range(1):
    k = randint(0, 2000 - 1)
    transaction = Transaction()
    for j in range(5):
        key = keys[k * 5 + j]
        q = Query(grades_table)
        transaction.add_query(q.select, key, 0, [1, 1, 1, 1, 1])
        q = Query(grades_table)
        transaction.add_query(q.increment, key, 1)
    transaction_workers[i % num_threads].add_transaction(transaction)

threads = []
for transaction_worker in transaction_workers:
    threads.append(threading.Thread(target = transaction_worker.run, args = ()))

for i, thread in enumerate(threads):
    print('Thread', i, 'started')
    thread.start()

for i, thread in enumerate(threads):
    thread.join()
    print('Thread', i, 'finished')

num_committed_transactions = sum(t.result for t in transaction_workers)
print(num_committed_transactions, 'transaction committed.')

query = Query(grades_table)
s = query.sum(keys[0], keys[-1], 1)


# -------------------- Print Table --------------------
for (i, y) in enumerate(grades_table.page_ranges):
    for j in range(104):
        print("_", end='')
    print()
    page_range_header = PAGE_RANGE + str(i)
    print(page_range_header.center(104, ' '))
    for j in range(104):
        print("_", end='')
    print()
    print(BASE_PAGES.center(104, ' '))
    print(INDIRECTION.center(12, ' '), end='|')
    print(RID.center(12, ' '), end='|')
    print(TIME.center(12, ' '), end='|')
    print(SCHEMA.center(12, ' '), end='|')
    print(TPS.center(12, ' '), end='|')
    print(KEY.center(12, ' '), end='|')
    print(G1.center(12, ' '), end='|')
    print(G2.center(12, ' '), end='|')
    print()
    for x in range(y.base_pages[0].num_records):
        for (page_num, page) in enumerate(y.base_pages):
            byte_val = page.data[x*8:(x*8 + 8)]
            val = int.from_bytes(byte_val, "big")
            # print("{0: 10d}".format(val), end=' ')
            print(str(val).center(12, ' '), end='|')
        print()
    for j in range(104):
        print("_", end='')
    print()
# ----------------------------------------------------------------------------------------------------

# # -------------------- Print Table --------------------
for (i, y) in enumerate(grades_table.page_ranges):
    print_header_line(104)
    page_range_header = PAGE_RANGE + str(y.id_num)
    print(page_range_header.center(100, ' '))
    print_header_line(104)
    print(BASE_PAGES.center(104, ' '))
    print(INDIRECTION.center(12, ' '), end='|')
    print(RID.center(12, ' '), end='|')
    print(TIME.center(12, ' '), end='|')
    print(SCHEMA.center(12, ' '), end='|')
    print(TPS.center(12, ' '), end='|')
    print(KEY.center(12, ' '), end='|')
    print(G1.center(12, ' '), end='|')
    print(G2.center(12, ' '), end='|')
    print()
    for x in range(y.base_pages[0].num_records):
        for (page_num, page) in enumerate(y.base_pages):
            byte_val = page.data[x*8:(x*8 + 8)]
            val = int.from_bytes(byte_val, "big")
            print(str(val).center(12, ' '), end='|')
        print()
#
    num_tail_pages = len(y.tail_pages)
    num_tail_page_sets = int(num_tail_pages / (grades_table.num_columns + 5))
    tail_page_set_start = 0
    tail_page_set_end = 4 + grades_table.num_columns + 1
    for n in range(num_tail_page_sets):
        tail_page_header = TAIL_PAGE + str(n)
        print(tail_page_header.center(104, ' '))
        print(INDIRECTION.center(12, ' '), end='|')
        print(RID.center(12, ' '), end='|')
        print(TIME.center(12, ' '), end='|')
        print(SCHEMA.center(12, ' '), end='|')
        print(BASE_RID.center(12, ' '), end='|')
        print(KEY.center(12, ' '), end='|')
        print(G1.center(12, ' '), end='|')
        print(G2.center(12, ' '), end='|')
        print()
        current_tail_page = y.tail_pages[tail_page_set_start]
        for x in range(current_tail_page.num_records):
            for (page_num, page) in enumerate(y.tail_pages[tail_page_set_start:tail_page_set_end]):
                byte_val = page.data[x * 8:(x * 8 + 8)]
                val = int.from_bytes(byte_val, "big")
                print(str(val).center(12, ' '), end='|')
                # if page_num == tail_page_set_end:
                #     break
            print()
        tail_page_set_start += 4 + grades_table.num_columns + 1
        tail_page_set_end += 4 + grades_table.num_columns + 1
    print_header_line(104)
# # ----------------------------------------------------------------------------------------------------
if s != num_committed_transactions * 5:
    print('Expected sum:', num_committed_transactions * 5, ', actual:', s, '. Failed.')
else:
    print('Pass.')
