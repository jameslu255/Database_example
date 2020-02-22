from template.table import Table
import os

class Database():

    def __init__(self):
        self.tables = []
        pass

    def open(self):
        if os.path.exists("base_pages.bin"):
            os.remove("base_pages.bin")
        if os.path.exists("tail_pages.bin"):
            os.remove("tail_pages.bin")

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key)
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        pass
