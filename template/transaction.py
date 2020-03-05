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

    def run(self):
        for query, args in self.queries:
            print("run query", query, args)
            error = -1
            query_type = str(query.__name__)
            # check if query runs successfully
            if query_type == "update": # Query.update
                print("query update")
                # error = query(*args)
            elif query_type == "insert": # Query.insert
                print("query insert"):
                # error = query(*args)
            elif query_type == "select": # Query.select
                print("query select")
                # check to see if Tj reads an object last written by Ti, Tj must be aborted as well!
            else:
                raise Exception(" unexpected query type {}".format(query_type))
                
            if(error == -1):
                print("aborting! Query failed for",query, args)
                self.abort()
            else: # no error write to log
                self.logger.write(self.id, query_type, error, args[1:], args[0])
                
        # if all queries run successfully 
        self.commit()
        
        pass

    def abort(self):
        # write 'tid aborted'
        self.logger.abort(self.id)
        # undo all the queries of this transaction
        q = self.logger.read_tid(self.id) # queries that happened already before the abort
        
        
        pass

    def commit(self):
        # write 'tid commited'
        self.logger.commit(self.id)
        pass

