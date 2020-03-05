from template.table import Table, Record
from template.index import Index
from template.logger import Logger

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self, id):
        self.queries = []
        
        self.id = id
        
        self.logger = Logger("log")
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args):
        # to run the query:
        # query.method(*args)
        self.queries.append((query, args))
        print("add query", query, args)

    def run(self):
        for query, args in self.queries:
            query(*args)
        pass

    def abort(self):
        # look at logger, get transaction id (thread id), get information associated from that transaction
        # (rid)
        pass

    def commit(self):
        pass

