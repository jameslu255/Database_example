from page import *
from table import *
from random import choice, randrange
import sys
import time

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
tail_page_directory = dict()

# Set a new tail record ID
tail_record_id = 1

# Let's just update A1 for now (i.e. column 4)
# (First 5 columns (0-4) are reserved)
sid = 906659675
rid = keys[sid]
new_val = 94
a1_col = 5

# Get the matching pages for the rid
matching_pages = page_directory[rid]
matching_col_page = matching_pages[a1_col]

# Get the record's schema encoding the in base page
schema_enc = pages[matching_pages[SCHEMA_ENCODING_COLUMN]].get_record_int(rid)

def is_schema_encoding_set(schema_encoding, column):
    mask = 1 << column
    res = schema_encoding & mask
    print(res)
    return res > 0
    
# Get the indirection value
# Store the indirection ptr from the record's base page
curr_ptr = pages[matching_pages[INDIRECTION_COLUMN]].get_record_int(rid)

# Check if any available tail pages
# Conditions for creating a new tail page

# 1. If there are no tail pages (i.e. first update performed)
# for the given record
if curr_ptr == 0:
    # Make empty tail pages for each column
    new_tail_pages = [Page() for i in range(num_columns)]
    # Need data structure to keep track of the indexes of the free pages
    new_free_tail_pages = [i for i in range(num_columns)]
    # Add the indices to the tail page directory
    tail_page_directory[tail_record_id] = new_free_tail_pages

    # Write the data to the new tail pages
    new_tail_pages[INDIRECTION_COLUMN].write(0)
    new_tail_pages[RID_COLUMN].write(tail_record_id)
    new_tail_pages[TIMESTAMP_COLUMN].write(int(time.time()))
    new_tail_pages[SCHEMA_ENCODING_COLUMN].write(1 << a1_col)
    new_tail_pages[a1_col].write(new_val)
    
    # Store the current number of tail pages
    old_num_tail_pages = len(tail_pages)

    # Add the list of new tail pages
    tail_pages.extend(new_tail_pages)
    # Add in the indexes of the new free tail pages
    new_free_tail_pages = [i for i in range(old_num_tail_pages, len(tail_pages))]
    # Add the list of free tail page indexes
    free_tail_pages.extend(new_free_tail_pages)

    # Add the indices to the tail page directory
    tail_page_directory[tail_record_id] = new_free_tail_pages

    # Update the base record columns
    pages[matching_pages[INDIRECTION_COLUMN]].set_record(rid, tail_record_id)
    print(pages[matching_pages[INDIRECTION_COLUMN]].get_record_int(rid))
    pages[matching_pages[SCHEMA_ENCODING_COLUMN]].set_record(rid, 1 << a1_col)
    print(pages[matching_pages[SCHEMA_ENCODING_COLUMN]].get_record_int(rid))
else:
    # Else, there exists a tail page for this record
    
    #2. Base Page Schema encoding says there was something already 
    # updated for that column, so we will follow the
    # Indirection pointer to the latest update
    # then create our new set of tail pages
    
    # Get the base pages associated with the record
    #matching_tail_pages = tail_page_directory[rid]

    # Get the latest tail pages
    matching_tail_pages = tail_page_directory[curr_ptr]

    # We want to write to the tail page if we can
    schema_enc_tail_page = tail_pages[matching_tail_pages[SCHEMA_ENCODING_COLUMN]]

    # Get the schema encoding of the latest tail page
    latest_schema_enc = schema_enc_tail_page.get_record_int(rid)

    # Get the bit of that column
    is_tail_schema_bit_set = is_schema_encoding_set(latest_schema_enc, a1_col)

    # If there's a previous tail page
    if is_tail_schema_bit_set:
        # Create the new tail pages
        tail_record_id += 1
        # Create the new tail pages
        new_tail_pages = [Page() for i in range(num_columns)]
        # We know that these aren't full because they are new pages
        # Curr ptr is the RID of the tail RID that the base record is curently
        # pointing to
        new_tail_pages[INDIRECTION_COLUMN].write(curr_ptr)
        new_tail_pages[RID_COLUMN].write(tail_record_id)
        new_tail_pages[TIMESTAMP_COLUMN].write(int(time.time()))
        new_tail_pages[SCHEMA_ENCODING_COLUMN].write(1 << a1_col)
        new_tail_pages[a1_col].write(new_val)
        old_num_tail_pages = len(tail_pages)

        # Add the list of new tail pages
        tail_pages.extend(new_tail_pages)
        # Add in the indexes of the new free tail pages
        new_free_tail_pages = [i for i in range(old_num_tail_pages, len(tail_pages))]
        # Add the indices to the tail page directory
        tail_page_directory[tail_record_id] = new_free_tail_pages
        free_tail_pages.extend(new_free_tail_pages)

        # Since this tail page already exists, we need to update the schema encoding
        # not override it with the column bit vector
        new_schema_encoding = latest_schema_enc | 1 << a1_col

        # Set the indirection record of the base RID to the latest tail RID 
        pages[matching_pages[INDIRECTION_COLUMN]].set_record(rid, tail_record_id)
        pages[matching_pages[SCHEMA_ENCODING_COLUMN]].set_record(rid, new_schema_encoding)
    else:
        # tail_schema_bit == '0'
        # i.e. there isn't a previous tail record (this is the latest one)
        value = curr_ptr
        if tail_pages[matching_tail_pages[free_tail_pages[INDIRECTION_COLUMN]]].write(value) == -1: # maximum size reached in page
            page = Page()
            # write to new page
            page.write(value)
            # append the new page
            tail_pages.append(page)
            # update free page index to point to new blank page
            free_tail_pages[INDIRECTION_COLUMN] = len(self.tail_pages) - 1
         
        value = tail_record_id
        if tail_pages[matching_tail_pages[free_tail_pages[RID_COLUMN]]].write(value) == -1: # maximum size reached in page
            page = Page()
            # write to new page
            page.write(value)
            # append the new page
            tail_pages.append(page)
            # update free page index to point to new blank page
            free_tail_pages[RID_COLUMN] = len(self.tail_pages) - 1
        
        value = int(time.time())
        if tail_pages[matching_tail_pages[free_tail_pages[TIMESTAMP_COLUMN]]].write(value) == -1: # maximum size reached in page
            page = Page()
            # write to new page
            page.write(value)
            # append the new page
            tail_pages.append(page)
            # update free page index to point to new blank page
            free_tail_pages[TIMESTAMP_COLUMN] = len(self.tail_pages) - 1
         
        value = new_val
        if tail_pages[matching_tail_pages[free_tail_pages[a1_col]]].write(new_val) == -1: # maximum size reached in page
            page = Page()
            # write to new page
            page.write(value)
            # append the new page
            tail_pages.append(page)
            # update free page index to point to new blank page
            free_tail_pages[a1_col] = len(self.tail_pages) - 1
                
        # Since this tail page already exists, we need to update the schema encoding
        # not override it with the column bit vector
        new_schema_encoding = latest_schema_enc | 1 << a1_col

        # Update base page records
        pages[matching_pages[SCHEMA_ENCODING_COLUMN]].set_record(rid, new_schema_encoding)
        pages[matching_pages[INDIRECTION_COLUMN]].set_record(rid, tail_record_id)


# Check to see if pages contain the records
# Loop through each base page
for (page_num, page) in enumerate(tail_pages):
    print(f"PG: {page_num}" )
    # Loop through the page's data
    # Data starts at 4
    for i in range(0, page.num_records):
        byteval = page.data[i*8:(i*8 + 8)]
        val = int.from_bytes(byteval, "big")
        print(f"Val: {val}")

