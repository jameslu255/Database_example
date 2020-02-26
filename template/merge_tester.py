from template.db import Database
from template.query import Query
from time import process_time
from random import choice, randrange

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

# Student Id and 4 grades
db = Database()
grades_table = db.create_table('Grades', 3, 0)
query = Query(grades_table)
keys = []


def print_header_line(count):
    for j in range(count):
        print("_", end='')
    print()


# -------------------- Measuring Insert Performance --------------------
insert_time_0 = process_time()
for i in range(0, 520):
    query.insert(906659671 + i, 93, 0)
    keys.append(906659671 + i)
insert_time_1 = process_time()
# -------------------- Print Table --------------------
# for (i, y) in enumerate(grades_table.page_ranges):
#     for j in range(104):
#         print("_", end='')
#     print()
#     page_range_header = PAGE_RANGE + str(i)
#     print(page_range_header.center(104, ' '))
#     for j in range(104):
#         print("_", end='')
#     print()
#     print(BASE_PAGES.center(104, ' '))
#     print(INDIRECTION.center(12, ' '), end='|')
#     print(RID.center(12, ' '), end='|')
#     print(TIME.center(12, ' '), end='|')
#     print(SCHEMA.center(12, ' '), end='|')
#     print(TPS.center(12, ' '), end='|')
#     print(KEY.center(12, ' '), end='|')
#     print(G1.center(12, ' '), end='|')
#     print(G2.center(12, ' '), end='|')
#     print()
#     for x in range(y.base_pages[0].num_records):
#         for (page_num, page) in enumerate(y.base_pages):
#             byte_val = page.data[x*8:(x*8 + 8)]
#             val = int.from_bytes(byte_val, "big")
#             # print("{0: 10d}".format(val), end=' ')
#             print(str(val).center(12, ' '), end='|')
#         print()
#     for j in range(104):
#         print("_", end='')
#     print()
# ----------------------------------------------------------------------------------------------------
print("Inserting 10 records took:  \t\t\t", insert_time_1 - insert_time_0)
print()


# -------------------- Measuring update Performance --------------------
update_cols = [
    [randrange(0, 100), None, None],
    [None, randrange(0, 100), None],
    [None, None, randrange(0, 100)],
]

update_time_0 = process_time()
for i in range(0, 515):
    # query.update(choice(keys), *(choice(update_cols)))
    query.update(906659671 + i, *[None, 78, None])

query.update(906660185, *[None, 88, None])
query.update(906660189, *[None, 88, None])
query.update(906660189, *[None, 98, None])
update_time_1 = process_time()
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
print("Updating 520 records took:  \t\t\t", update_time_1 - update_time_0)
print()


# -------------------- Measuring Merge Performance --------------------
# merge_time_0 = process_time()
# grades_table.merge(grades_table.page_ranges[0])
# merge_time_1 = process_time()
# -------------------- Print Table --------------------
# for (i, y) in enumerate(grades_table.page_ranges):
#     print_header_line(104)
#     page_range_header = PAGE_RANGE + str(i)
#     print(page_range_header.center(104, ' '))
#     print_header_line(104)
#     print(BASE_PAGES.center(104, ' '))
#     print(INDIRECTION.center(12, ' '), end='|')
#     print(RID.center(12, ' '), end='|')
#     print(TIME.center(12, ' '), end='|')
#     print(SCHEMA.center(12, ' '), end='|')
#     print(TPS.center(12, ' '), end='|')
#     print(KEY.center(12, ' '), end='|')
#     print(G1.center(12, ' '), end='|')
#     print(G2.center(12, ' '), end='|')
#     print()
#     for x in range(y.base_pages[0].num_records):
#         for (page_num, page) in enumerate(y.base_pages):
#             byte_val = page.data[x*8:(x*8 + 8)]
#             val = int.from_bytes(byte_val, "big")
#             # print("{0: 10d}".format(val), end=' ')
#             print(str(val).center(12, ' '), end='|')
#         print()
#
#     num_tail_pages = len(y.tail_pages)
#     num_tail_page_sets = int(num_tail_pages / (grades_table.num_columns + 5))
#     tail_page_set_start = 0
#     tail_page_set_end = 7
#     for n in range(num_tail_page_sets):
#         tail_page_header = TAIL_PAGE + str(n)
#         print(tail_page_header.center(104, ' '))
#         print(INDIRECTION.center(12, ' '), end='|')
#         print(RID.center(12, ' '), end='|')
#         print(TIME.center(12, ' '), end='|')
#         print(SCHEMA.center(12, ' '), end='|')
#         print(BASE_RID.center(12, ' '), end='|')
#         print(KEY.center(12, ' '), end='|')
#         print(G1.center(12, ' '), end='|')
#         print(G2.center(12, ' '), end='|')
#         print()
#         current_tail_page = y.tail_pages[tail_page_set_start]
#         for x in range(current_tail_page.num_records):
#             for (page_num, page) in enumerate(y.tail_pages[tail_page_set_start:tail_page_set_end + 1]):
#                 byte_val = page.data[x * 8:(x * 8 + 8)]
#                 val = int.from_bytes(byte_val, "big")
#                 print(str(val).center(12, ' '), end='|')
#                 # if page_num == tail_page_set_end:
#                 #     break
#             print()
#         tail_page_set_start += 8
#         tail_page_set_end += 8
#     print_header_line(104)
# ----------------------------------------------------------------------------------------------------
# print("Merging 10 records took:  \t\t\t", merge_time_1 - merge_time_0)

