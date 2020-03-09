from template.table import Table, Record
from template.index import Index
from template.logger import Logger

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        
        self.logger = Logger("log")
        # update num transaction count on top of file
        Logger.num_transactions.add(1)
        self.id = self.logger.getNumTransaction()
        
        print("this is transaction #",self.id)
        
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
            result = query(*args, txn_id = self.id)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()


    def abort(self):
        print("aborting! Query failed for", self.id)
        # write 'tid aborted'
        self.logger.abort(self.id)
        # undo all the queries of this transaction
        # q = self.logger.read_tid(self.id) # queries that happened already before the abort
        self.queries = []
        return False
        
    def commit(self):
        print("commiting! Query successful for",self.id)
        # write 'tid commited'
        self.logger.commit(self.id)
        return True