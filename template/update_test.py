from page import *
from table import *
from random import choice, randrange

# Create a record with RID=1, key=906659671, col=[90, 83, 93]
# Record class may not be needed since data is stored 
# column-wise in the pages
#record = Record(1, 906659671, [90, 83, 93])

# Generate some dummy data
data = []
# SID->RID
keys = dict()
# Set the number of records to be generated
num_records = 10
for i in range(1, num_records):
    key = 906659671 + i
    record = Record(i, key, [randrange(0, 100), randrange(0, 100),
    randrange(0, 100), randrange(0, 100)])
    data.append(record)
    keys[key] = i


# Page Range Data Structure (List for now)

# Make empty base pages for each column
pages = [Page() for i in range(len(record.columns))]

# Need data structure to keep track of the indexes of the free pages
free_pages = [i for i in range(len(record.columns))]

# Insert simulation
# Make a page directory from RID->Page Numbers
# <int, list>
page_directory = dict()

# For each row in the data
for record in data:
    # Keep track of the page numbers for the record
    record_pages = []
    # For each column in the record
    for (i, col) in enumerate(record.columns):
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
        # Write the col to the page
        page.write(col)
        # Add the page number to the record pages array
        record_pages.append(free_pages[i])
    # Map the RID to the page numbers
    page_directory[record.rid] = record_pages

print("Before Update")
# Check to see if pages contain the records
# Loop through each base page
for (i, page) in enumerate(pages):
    print(f"PG: {i}" )
    # Loop through the page's data
    # Data starts at 4
    for i in range(4, page.num_records):
        print(f"Val: {page.data[i]}")


#########################################################
print("After Update:")

#update_cols = [
#    [randrange(0, 100), None, None, None ],
#    [None, randrange(0, 100), None, None],
#    [None, None, randrange(0, 100), None],
#    [None, None, None, randrange(0, 100)]
#]

# UPDATE Students 
# SET A1= '72', A2= '45'
# WHERE SID = 906659675

# Let's just update A1 for now (i.e. column 1)
sid = 906659675
rid = keys[sid]
# Get the base page for the first column
matching_page_idx = page_directory[rid][1]
matching_page= pages[matching_page_idx]
# Make a new tail page
tail_page = Page()
# Set a new tail record ID
# TODO: need the page ranges implemented
# I'm thinking we can keep track of these tail record IDs
# in a page range class since they only increment by 1 
# per each update
tail_record_id = 1
# Set the data at RID to new value
tail_page.data[rid] = 72
# Set the indirection column of base record to 
# RID of latest tail record
#matching_page.data[INDIRECTION_COLUMN] = tail_record_id
# if RID of previous TR is not 0, set INDIRECTION_COLUMN of 
# new TR to previous TR


