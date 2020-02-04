from template.db import Database
from template.query import Query
from time import process_time
from random import choice, randrange

# Student Id and 4 grades
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
keys = []

# Measuring Insert Performance
insert_time_0 = process_time()
for i in range(0, 10):
    query.insert(906659671 + i, 93, 0, 0, 0)
    keys.append(906659671 + i)
insert_time_1 = process_time()
print("----------------base page-------------------------")
print("         0          1          2          3          4          5          6          7          8")
for (i,y) in enumerate(grades_table.page_ranges):
    print("page range #" + str(i))
    for x in range(y.base_pages[0].num_records):
        for (page_num, page) in enumerate(y.base_pages):
            byteval = page.data[x*8:(x*8 + 8)]
            val = int.from_bytes(byteval, "big")
            print("{0: 10d}".format(val), end = ' ')
        print()
        
# for (page_num, page) in enumerate(grades_table.base_pages):
    # print(f"base_page_col: {page_num}" )
    # # Loop through the page's data
    # # Data starts at 4
    # for i in range(0, page.num_records):
        # byteval = page.data[i*8:(i*8 + 8)]
        # val = int.from_bytes(byteval, "big")
        # print(f"Val: {val}")
        
print("Inserting 10k records took:  \t\t\t", insert_time_1 - insert_time_0)

# Measuring update Performance
update_cols = [
    [randrange(0, 100), None, None, None, None],
    [None, randrange(0, 100), None, None, None],
    [None, None, randrange(0, 100), None, None],
    [None, None, None, randrange(0, 100), None],
    [None, None, None, None, randrange(0, 100)],
]

update_time_0 = process_time()
query.update(906659671, None, None, None, None, 100)
query.update(906659671, None, None, None, None, 85)
query.update(906659671, None, None, None, None, 99)
# for i in range(0, 3):
    # query.update(choice(keys), *(choice(update_cols)))
update_time_1 = process_time()
print("         0          1          2          3          4          5          6          7          8")
for (i,y) in enumerate(grades_table.page_ranges):
    print("page range #" + str(i))
    for x in range(y.base_pages[0].num_records):
        for (page_num, page) in enumerate(y.base_pages):
            byteval = page.data[x*8:(x*8 + 8)]
            val = int.from_bytes(byteval, "big")
            print("{0: 10d}".format(val), end = ' ')
        print()

print("----------------tail page-------------------------")
print("         0          1          2          3          4          5          6          7          8")
# for x in range(grades_table.tail_pages[0].num_records):
    # for (page_num, page) in enumerate(grades_table.tail_pages):
        # byteval = page.data[x*8:(x*8 + 8)]
        # val = int.from_bytes(byteval, "big")
        # print("{0: 10d}".format(val), end = ' ')
    # print()
    
print(grades_table.tail_page_directory)
    
for (i,y) in enumerate(grades_table.page_ranges):
    print("page range #" + str(i))
    for (page_num, page) in enumerate(y.tail_pages):
        print(f"Tail PG: {page_num}" )
        # Loop through the page's data
        # Data starts at 4
        for z in range(0, 10):
            byteval = page.data[z*8:(z*8 + 8)]
            val = int.from_bytes(byteval, "big")
            print(f"Val: {val}")

print("Updating 10k records took:  \t\t\t", update_time_1 - update_time_0)

# Measuring Select Performance
select_time_0 = process_time()
for i in range(0, 10000):
    query.select(choice(keys), [1, 1, 1, 1, 1])
select_time_1 = process_time()
print("Selecting 10k records took:  \t\t\t", select_time_1 - select_time_0)

# Measuring Aggregate Performance
agg_time_0 = process_time()
for i in range(0, 10000, 100):
    result = query.sum(i, 100, randrange(0, 5))
agg_time_1 = process_time()
print("Aggregate 10k of 100 record batch took:\t", agg_time_1 - agg_time_0)

# Measuring Delete Performance
delete_time_0 = process_time()
query.delete(906659671)
# for i in range(0, 10000):
    # query.delete(906659671 + i)
delete_time_1 = process_time()

print("         0          1          2          3          4          5          6          7          8")
for (i,y) in enumerate(grades_table.page_ranges):
    print("page range #" + str(i))
    for x in range(y.base_pages[0].num_records):
        for (page_num, page) in enumerate(y.base_pages):
            byteval = page.data[x*8:(x*8 + 8)]
            val = int.from_bytes(byteval, "big")
            print("{0: 10d}".format(val), end = ' ')
        print()
    
for (i,y) in enumerate(grades_table.page_ranges):
    print("page range #" + str(i))
    for (page_num, page) in enumerate(y.tail_pages):
        print(f"Tail PG: {page_num}" )
        # Loop through the page's data
        # Data starts at 4
        for z in range(0, 10):
            byteval = page.data[z*8:(z*8 + 8)]
            val = int.from_bytes(byteval, "big")
            print(f"Val: {val}")

# for (page_num, page) in enumerate(grades_table.tail_pages):
    # print(f"Tail PG: {page_num}" )
    # Loop through the page's data
    # Data starts at 4
    # for i in range(0, 10):
        # byteval = page.data[i*8:(i*8 + 8)]
        # val = int.from_bytes(byteval, "big")
        # print(f"Val: {val}")
print("Deleting 10k records took:  \t\t\t", delete_time_1 - delete_time_0)
