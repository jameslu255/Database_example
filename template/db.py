from template.table import Table

class Database():

    def __init__(self):
        self.tables = []
        
        self.num_tables = 0
        
        pass

    def open(self):
        pass

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        self.num_tables += 1
        
        table = Table(name, num_columns, key, self.num_tables)
        
        self.tables.append(table)
        
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        pass
