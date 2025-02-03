from enum import Enum


class TransactionStatus(Enum):
    ONGOING = 1
    WAITING = 2
    ABORTED = 3
    COMMITTED = 4


class Transaction:
    def __init__(self, tid, timestamp):
        self.tid = tid
        self.startTime = timestamp
        self.status = TransactionStatus.ONGOING
        self.isReadOnly = True
