from template.table import Table
from template.disk import Disk
import os

class Database():

    def __init__(self):
        self.tables = []

        self.num_tables = 0

        self.file_name = ""

        pass

    def open(self, file_name):
        disk = Disk(file_name)
        self.file_name = file_name

        # read disk
        table_dict = disk.decode()
        for key in table_dict:
            # print("table id is " + str(key))
            # update tables array
            self.tables.append(table_dict[key])

        # print("fin open!" + str(len(self.tables)))
        pass

    def close(self):
        # print("close!" + str(len(self.tables)))
        disk = Disk(self.file_name)
        disk.empty_disk()
        # save each table into disk
        for t in self.tables:
            # print(t.name)
            disk.encode_table(t)

        self.tables = []
        pass

    def get_table(self, table_name):
        for table in self.tables:
            if table.name == table_name:
                return table

        return "error, table not found"

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        self.num_tables += 1

        table = Table(name, num_columns, key)

        self.tables.append(table)

        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        pass
