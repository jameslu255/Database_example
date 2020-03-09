from template.table import Table, Record
from template.index import Index
from template.logger import Logger

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self, id, query):
        self.query = query
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

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            query_type = str(query.__name__)
            print(query_type)
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()


    def abort(self):
        # TODO: do roll-back and any other necessary operations
        # write 'tid aborted'
        self.logger.abort(self.id)


        # Undo the stuff that occurred
        # Step 1: clear queries
        self.queries = []
        # Step 2: roll back
        line_read = self.logger.read_tid(self.id)  # queries that happened already before the abort


        return False


    def commit(self):
        # TODO: commit to database
        # write 'tid commited'
        self.logger.commit(self.id)
        return True
