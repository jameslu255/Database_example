#from template.table import Table
from BTrees.IIBTree import IIBTree

"""
# optional: Indexes the specified column of the specified table to speed up select queries
# This data structure is usually a B-Tree
"""

btree_list = []


class Index:

    def __init__(self, table):
        for i in range(table.num_columns):
            btree_list.append(IIBTree())
            btree_list[0].update({1: 5})
            print(list(btree_list[0].values()))
        

    """
    # returns the location of all records with the given value
    """

    def locate(self, value):
        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        pass
