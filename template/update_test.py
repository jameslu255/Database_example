from page import *
from table import *
from random import choice, randrange

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
# TODO: need the page ranges implemented
# I'm thinking we can keep track of these tail record IDs
# in a page range class since they only increment by 1 
# per each update

# In the real code, we would see if there is already
# an update for the record. If there is, we'd need to create
# a new set of tail pages and append them to the tail page 
# array in the page range class


# Page range initially
free_tail_pages = [] 
tail_pages = []
# Set a new tail record ID
tail_record_id = 1

# Let's just update A1 for now (i.e. column 4)
# (First 5 columns (0-4) are reserved)
sid = 906659675
rid = keys[sid]
a1_col = 5

# Get the matching base pages for the first column
matching_pages = page_directory[rid]
matching_col_page = matching_pages[a1_col]

# Get the record's schema encoding the in base page
schema_enc = matching_pages[SCHEMA_ENCODING_COLUMN][rid]

def get_schema_encoding_bit(schema_encoding, column):
    """
    bin() stores binary like so
    bin(2) = '0b10'
    """
    binary_enc = bin(schema_encoding)[2:]
    # should probably check if col is in bounds of the schema encoding 
    return binary_enc[column]
    

# Get the column's bit in the schema encoding
schema_bit = get_schema_encoding_bit(schema_enc, a1_col)

# Check if any available tail pages
# Conditions for creating a new tail page
# 1. If there are no tail pages (i.e. first update performed)
if tail_pages == []:
    # Make empty tail pages for each column
    tail_pages = [Page() for i in range(num_columns)]
    # Need data structure to keep track of the indexes of the free pages
    free_tail_pages = [i for i in range(num_columns)]

#2. Base Page Schema encoding says there was something already 
# updated for that column, so we will follow the
# Indirection pointer to the latest update
# then create our new set of tail pages


if schema_bit == '1':
    # Get the base page's indirection column
    indir_bp = matching_pages[INDIRECTION_COLUMN]

    # Store the indirection ptr from the record's base page
    curr_ptr = indir_bp.get_record_int(rid)

    # Get the tail page that's the base page is pointing to
    curr_tail_page = tail_pages[curr_ptr]

    # follow the ptr to the latest update
    while (curr_ptr != 0):
        # Store the indirection ptr from the record's base page
        curr_ptr = indir_bp.get_record_int(rid)

        # Get the tail page that's the base page is pointing to
        curr_tail_page = tail_pages[indir_start]

        
        schema_bit = 
        
    

# Set the data at RID to new value
tail_page.data[rid] = 72
# Set the indirection column of base record to 
# RID of latest tail record
#matching_page.data[INDIRECTION_COLUMN] = tail_record_id
# if RID of previous TR is not 0, set INDIRECTION_COLUMN of 
# new TR to previous TR


