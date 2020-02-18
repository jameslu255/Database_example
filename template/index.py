#from template.table import Table
from BTrees.OOBTree import OOBTree

"""
# optional: Indexes the specified column of the specified table to speed up select queries
# This data structure is usually a B-Tree
"""

btree_list = []


#creating btrees for each assignment
class Index:
    def __init__(self, num_columns):
        #initializes number of trees by creating a tree for each assignment. Number assignment is defined by columns.
        for i in range(num_columns):
            btree_list.append(OOBTree())
    def add_values(self, column_num, key, value):
        #places RID and score on the correct assignment B-tree
        if btree_list[column_num].has_key(key):
            new_list = btree_list[column_num].get(key).append(value)
            btree_list[column_num].insert(key, new_list)
        else:
            btree_list[column_num].insert(key, [value])
    def create_dictionary(self, column_num, key, value):
        btree_list[column_num].insert(key, value)

    def update_btree(self, column_num, key, value, new_key):
        print("Student score is " + str(key) + " and RID of student is " + str(value) + " and new score is " + str(new_key))
        current_list = btree_list[column_num].get(key)
        print("Students that got score at " + str(key) + " is " + str(current_list))
        if (len(current_list) == 1):
            btree_list[column_num].pop(key)
        else:
            current_list.remove(value)
            btree_list[column_num].__setitem__(key, current_list)
        print("Removed " + str(value) + " from list:" + str(key) + " -> " + str(btree_list[column_num].get(key, "Key was removed since it is empty now")))
        #btree_list[column_num].update({new_key: [value]})

        current_list = btree_list[column_num].get(new_key, -1)
        if (current_list == -1):
            self.add_values(column_num, new_key, value)
        else:
            current_list.append(value)
            btree_list[column_num].__setitem__(new_key, current_list)
        print("Added "+ str(value) + " to list:" + str(new_key) + " -> " + str(btree_list[column_num].get(new_key)))

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
                    print("Score: " + str(pair[0]) + ", RID: " + str(pair[1]))

    def get_value(self, column_num, key):
        return btree_list[column_num].get(key, "Key not found")

    """
    # returns the location of all records with the given value
    # takes in a range of scores and an assignment and returns all RID's and their respective scores
    """

    def locate(self, lower_range, upper_range, assignment_num):
        #filters the btree by removing the all the values below lower_range
        list = btree_list[assignment_num].byValue(lower_range)
        final_list = []
        #adds the RID and Score inzto final_list as a tuple
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