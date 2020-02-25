#from template.table import Table
from BTrees.OOBTree import OOBTree

"""
# optional: Indexes the specified column of the specified table to speed up select queries
# This data structure is usually a B-Tree
"""



#creating btrees for each assignment
class Index:
    def __init__(self, num_columns):
        #initializes number of trees by creating a tree for each assignment. Number assignment is defined by columns.
        self.btree_list = []
        for i in range(num_columns):
            self.btree_list.append(OOBTree())
    def add_values(self, column_num, key, value):
        #places RID and score on the correct assignment B-tree
        if self.btree_list[column_num].has_key(key):
            new_list = self.btree_list[column_num].get(key).add(value)
            self.btree_list[column_num].insert(key, new_list)
        else:
            self.btree_list[column_num].insert(key, {value})
    def create_dictionary(self, column_num, key, value):
        self.btree_list[column_num].insert(key, {value})

    def update_btree(self, column_num, key, value, new_key):
        #print("Student score is " + str(key) + " and RID of student is " + str(value) + " and new score is " + str(new_key))
        current_list = self.btree_list[column_num].get(key, "key not found")
        current_key = key

        #self.print_tree(column_num)
        if (current_list == "key not found"):
            found = False
            prev_key = key
            print(column_num)
            print("Attempting fix")
            while (found == False):
                current_key = self.btree_list[column_num].minKey(prev_key)
                print(current_key)
                current_list = self.btree_list[column_num].get(current_key, "key not found")
                if (value in current_list):
                    found = True
                    print("Fixed!")
                prev_key = current_key + 1
        #print("Students that got score at " + str(current_key) + " is " + str(current_list))
        if (len(current_list) == 1):
            self.btree_list[column_num].pop(current_key)
        else:
            if( value in current_list):
                current_list.remove(value)
            self.btree_list[column_num].__setitem__(current_key, current_list)
        #print("Removed " + str(value) + " from list:" + str(current_key) + " -> " + str(self.btree_list[column_num].get(current_key, "Key was removed since it is empty now")))

        current_list = self.btree_list[column_num].get(new_key, -1)
        if (current_list == -1):
            self.add_values(column_num, new_key, value)
        else:
            current_list.add(value)
            self.btree_list[column_num].__setitem__(new_key, current_list)
        #print("Added " + str(value) + " to list:" + str(new_key) + " -> " + str(self.btree_list[column_num].get(new_key)))


    def print_trees(self):
        #prints out all values from assignment 0-4
        for i in range(len(self.btree_list)):
            if (i == 0):
                print("Students")
                for pair in (self.btree_list[i].iteritems()):
                    print("Student ID: " + str(pair[0]) + ", RID: " + str(pair[1]))
            else:
                print("Assignment " + str(i))
                for pair in (self.btree_list[i].iteritems()):
                    print("Score: " + str(pair[0]) + ", RID: " + str(pair[1]))

    def print_tree(self, column):
        for pair in (self.btree_list[column].iteritems()):
            print("Score: " + str(pair[0]) + ", RID: " + str(pair[1]))

    def get_value(self, column_num, key):
        return self.btree_list[column_num].get(key, "Key not found")

    """
    # returns the location of all records with the given value
    # takes in a range of scores and an assignment and returns all RID's and their respective scores
    """

    def locate(self, value, column):
        #filters the btree by removing the all the values below lower_range
        return self.btree_list[column].get(value, "Value does not exist.");
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
