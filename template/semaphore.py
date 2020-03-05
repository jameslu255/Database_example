from template import transaction


class Semaphore:
    def __init__(self, count):
        self.count = count


    def sem_down(self):
        if self.count == 0:
            # TODO: Since there is NO WAITING, we want to ABORT: call abort() in transaction class.
        else:
            self.count -= 1
        pass


    def sem_up(self):
        # Since there is NO WAITING, we do not have to 'unblock' any transactions when resources are returned.
        self.count += 1
        pass
