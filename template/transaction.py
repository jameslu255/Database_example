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

    def run(self):
        # !!!!!!!!!!!!!!!!! THIS CODE GIVEN IN THE SKELETON PROVIDED
        # for query, args in self.queries:
        #     result = query(*args)
        #     # If the query has failed the transaction should abort
        #     if result == False:
        #         return self.abort()
        # return self.commit()


        for query, args in self.queries:
            print("run query", query, args)
            error = -1
            query_type = str(query.__name__)
            # check if query runs successfully
            if query_type == "update": # Query.update
                print("query update")
                error = query(*args)
            elif query_type == "insert": # Query.insert
                print("query insert")
                error = query(*args)
            elif query_type == "select": # Query.select
                print("query select")
                # check to see if Tj reads an object last written by Ti, Tj must be aborted as well!
            elif query_type == "delete": # Query.delete
                print("query delete")
                error = query(*args)
            elif query_type == "sum":
                print("query sum")
            else:
                raise Exception(" unexpected query type {}".format(query_type))
                
            if error == -1:
                print("aborting! Query failed for",query, args)
                self.abort()
            else: # no error write to log
                self.logger.write(self.id, query_type, error, args[1:], args[0])
                
        # if all queries run successfully 
        self.commit()
        
        pass

    def abort(self):
        # TODO: do roll-back and any other necessary operations
        # write 'tid aborted'
        self.logger.abort(self.id)

        # Undo the stuff that occurred
        # Step 1: clear queries
        self.queries = []
        # Step 2: read the logger to know what to undo
        line_read = self.logger.read_tid(self.id)  # queries that happened already before the abort
        # line_read = [ transaction id, what transactions occurred (could be 1+), [old val], [new val], base RID ]
        # line_read = ["1 update 0,0,0 10,20,30 512", "1 insert 0,0,0 1,2,3 513", ...]
        # we want the log to be in this format instead: 1 insert 0,0,0,0 2,3,4,5 1
        read_array = line_read.split()  # ["1", "update", "0,0,0", "0,0,0", "RID"]
        tid = int(read_array[0])
        query_str = read_array[1]
        old_values = self.parse_string_array(read_array[2])
        new_values = self.parse_string_array(read_array[2])
        base_RID = int(read_array[4])

        table = self.query.table
        key = table.keys{base_RID}

        if query_str == "update":
            # To undo an update: update w/ old values
            self.query.update(key, *old_values)
        elif query_str == "insert":
            self.query.delete(*key)
        elif query_str == "delete":
            self.query.insert(*old_values)

        return False


    def commit(self):
        # TODO: commit to database
        # write 'tid commited'
        self.logger.commit(self.id)

        return True


    @staticmethod
    def parse_string_array(string):
        values = []
        parsed_string = string.split(',')
        for i in range(len(parsed_string)):
            value = int(parsed_string[i])
            values.append(value)
        return values
