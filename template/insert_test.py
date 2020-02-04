from template.page import *
from template.table import *
from random import choice, randrange
import time 
# Create a record with RID=1, key=906659671, col=[90, 83, 93]
# Record class may not be needed since data is stored 
# column-wise in the pages
#record = Record(1, 906659671, [90, 83, 93])

KEY_COLUMN = 5

# Generate some dummy data
data = []
# SID->RID
keys = dict()
# Set the number of records to be generated
num_records = 10
num_columns = 9
for i in range(1, num_records):
    key = 906659671 + i
    record = Record(i, key, [randrange(0, 100), randrange(0, 100),
    randrange(0, 100), randrange(0, 100)])
    data.append(record)
    keys[key] = i

# Page Range Data Structure (List for now)

# Make empty base pages for each column
pages = [Page() for i in range(num_columns)]

# Need data structure to keep track of the indexes of the free pages
free_pages = [i for i in range(num_columns)]

# Insert simulation
# Make a page directory from RID->Page Numbers
# <int, list>
page_directory = dict()

# For each row in the data
for record in data:
    # Keep track of the page numbers for the record
    record_pages = []
    # For each column in the record
    for i in range(num_columns):
        # Get the next avaliable page page
        page = pages[free_pages[i]]
        # If the page is full
        if not page.has_capacity():
            # Store the index of the next free page
            free_pages[i] = len(pages)
            # Construct the next page
            page = Page()
            # Add the page to the page range 
            pages.append(page)
        if i == INDIRECTION_COLUMN:
            page.write(0)
        elif i == RID_COLUMN:
            page.write(record.rid)
        elif i == TIMESTAMP_COLUMN:
            page.write(int(time.time()))
        elif i == SCHEMA_ENCODING_COLUMN:
            page.write(0)
        elif i == KEY_COLUMN:
            page.write(record.key)
        else: 
            # Get the value of the column
            col_val = record.columns[i - 5]
            # Write the col to the page
            page.write(col_val)
        # Add the page number to the record pages array
        record_pages.append(free_pages[i])
    # Map the RID to the page numbers
    page_directory[record.rid] = record_pages

print("Before Update")
# Check to see if pages contain the records
# Loop through each base page
for (page_num, page) in enumerate(pages):
    print(f"PG: {page_num}" )
    # Loop through the page's data
    # Data starts at 4
    for i in range(0, page.num_records):
        byteval = page.data[i*8:(i*8 + 8)]
        val = int.from_bytes(byteval, "big")
        print(f"Val: {val}")

#########################################################

