from template.db import Database
from template.query import Query
from time import process_time
from random import choice, randrange

# Header Constants
PAGE_RANGE = "PAGE RANGE #"
BASE_PAGES = "Base Pages"
TAIL_PAGES = "Tail Pages"
INDIRECTION = "indirection"
RID = "RID"
TIME = "time"
SCHEMA = "schema"
TPS = "TPS"
BASE_RID = "Base RID"
KEY = "key"
G1 = "G1"
G2 = "G2"


# Student Id and 4 grades
db = Database()
grades_table = db.create_table('Grades', 3, 0)
query = Query(grades_table)
keys = []

# -------------------- Measuring Insert Performance --------------------
insert_time_0 = process_time()
for i in range(0, 10):
    query.insert(906659671 + i, 93, 0)
    keys.append(906659671 + i)
insert_time_1 = process_time()
# -------------------------------------------- Print Table --------------------------------------------
for (i, y) in enumerate(grades_table.page_ranges):
    print(PAGE_RANGE + str(i))
    header = BASE_PAGES
    print(header.center(100, ' '))
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
            byteval = page.data[x*8:(x*8 + 8)]
            val = int.from_bytes(byteval, "big")
            # print("{0: 10d}".format(val), end=' ')
            print(str(val).center(12, ' '), end='|')
        print()
    print("________________________________________________________________________________________________________")
# ----------------------------------------------------------------------------------------------------
print("Inserting 10k records took:  \t\t\t", insert_time_1 - insert_time_0)


# -------------------- Measuring UPDATE Performance --------------------
update_cols = [
    [randrange(0, 100), None, None],
    [None, randrange(0, 100), None],
    [None, None, randrange(0, 100)],
]

update_time_0 = process_time()
for i in range(0, 10):
    query.update(choice(keys), *(choice(update_cols)))
update_time_1 = process_time()
# -------------------------------------------- Print Table --------------------------------------------
for (i, y) in enumerate(grades_table.page_ranges):
    print(PAGE_RANGE + str(i))
    header = BASE_PAGES
    print(header.center(100, ' '))
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
            byteval = page.data[x*8:(x*8 + 8)]
            val = int.from_bytes(byteval, "big")
            # print("{0: 10d}".format(val), end=' ')
            print(str(val).center(12, ' '), end='|')
        print()
    print("________________________________________________________________________________________________________")
    header = TAIL_PAGES
    print(header.center(100, ' '))
    print(INDIRECTION.center(12, ' '), end='|')
    print(RID.center(12, ' '), end='|')
    print(TIME.center(12, ' '), end='|')
    print(SCHEMA.center(12, ' '), end='|')
    print(BASE_RID.center(12, ' '), end='|')
    print(KEY.center(12, ' '), end='|')
    print(G1.center(12, ' '), end='|')
    print(G2.center(12, ' '), end='|')
    print()
    for x in range(y.tail_pages[0].num_records):
        for (page_num, page) in enumerate(y.tail_pages):
            byteval = page.data[x*8:(x*8 + 8)]
            val = int.from_bytes(byteval, "big")
            print(str(val).center(12, ' '), end='|')
        print()
    print("________________________________________________________________________________________________________")
# ----------------------------------------------------------------------------------------------------
print("Updating 10k records took:  \t\t\t", update_time_1 - update_time_0)




# -------------------- Measuring Select Performance --------------------
select_time_0 = process_time()
for i in range(0, 10):
    query.select(choice(keys), [1, 1, 1])
select_time_1 = process_time()
print("Selecting 10k records took:  \t\t\t", select_time_1 - select_time_0)

# -------------------- Measuring Aggregate Performance --------------------
agg_time_0 = process_time()
for i in range(0, 10, 1):
    result = query.sum(i, 1, randrange(0, 3))
agg_time_1 = process_time()
print("Aggregate 10k of 100 record batch took:\t", agg_time_1 - agg_time_0)

# -------------------- Measuring Delete Performance --------------------
delete_time_0 = process_time()
for i in range(0, 10):
    query.delete(906659671 + i)
delete_time_1 = process_time()
print("Deleting 10k records took:  \t\t\t", delete_time_1 - delete_time_0)