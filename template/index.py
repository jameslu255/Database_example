#from template.table import Table
from BTrees.IIBTree import IIBTree

"""
# optional: Indexes the specified column of the specified table to speed up select queries
# This data structure is usually a B-Tree
"""

btree_list = []



#creating btrees for each assignment
class Index:
    def __init__(self, table):
        #initializes number of trees by creating a tree for each assignment. Number assignment is defined by columns.
        for i in range(table.num_columns):
            btree_list.append(IIBTree())
    def add_values(self, column_num, RID, value):
        #places RID and score on the correct assignment B-tree
        btree_list[column_num].insert(RID, value)
    def create_dictionary(self, column_num, value, RID):
        btree_list[column_num].insert(value, RID)

    def print_trees(self):
        #prints out all values from assignment 0-4
        for i in range(len(btree_list)):
            if (i == 0):
                print("Students")
                for pair in (btree_list[i].iteritems()):
                    print("Student ID: " + str(pair[0]) + ", RID: " + str(pair[1]))
            else:
                print("Assignment " + str(i))
                for pair in (btree_list[i].iteritems()):
                    print("RID: " + str(pair[0]) + ", Score: " + str(pair[1]))
        print(btree_list[0].get(906659672))
        print(Index.locate(self, 20, 93, 2))
    """
    def get_value(self, assignment_num, key):
        if (key > 0 and key <= self.table.base_rid):
            return btree_list[assignment_num].get(key, -1)
        else:
            return 0
        """

    """
    # returns the location of all records with the given value
    # takes in a range of scores and an assignment and returns all RID's and their respective scores
    """

    def locate(self, lower_range, upper_range, assignment_num):
        #filters the btree by removing the all the values below lower_range
        list = btree_list[assignment_num].byValue(lower_range)
        final_list = []
        #adds the RID and Score into final_list as a tuple
        for i in range(len(list)):
            if (btree_list[assignment_num].get(list[i][1], -1) <= upper_range):
                final_list.append(tuple((list[i][1], btree_list[assignment_num].get(list[i][1], -1))))
        return final_list
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
