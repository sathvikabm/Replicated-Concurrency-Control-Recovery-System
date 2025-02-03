from Transaction import Transaction, TransactionStatus
from DataManager import DataManager
from SiteManager import SiteManager
from IOManager import IOManager
from Site import Site, SiteStatus


class TransactionManager:
    def __init__(self, site_manager, io_manager):
        self.site_manager = site_manager
        self.io_manager = io_manager
        self.transactionclosingcycle = (
            -1
        )  # to keep track of the transaction that closes the cycle
        self.transactions = (
            {}
        )  # HashMap which has int, Transaction - Hashmap of Transaction id to Transaction object pointer mappings.
        self.waitlist = (
            {}
        )  # waitlist: HashMap of int, List[int] - key is the site ids which are down and value is the list of transactions blocked on that site waiting for recovery to proceed further
        self.serializationGraph = (
            {}
        )  # HashMap of transaction_id, List[depending transactions where edge exists] - Adjacency list of serialization graph, when a dependecy is detected the edge is added later checked for cycles
        self.transactionHistory = (
            {}
        )  # HashMap of transaction_id, HashMap[variable_id, if R or W was performed] - transaction_id, variable_id, Read/Write is kept track for detecting cycles

    # fetches a particular transaction
    def get_trans(self, transaction_id):
        return self.transactions[transaction_id]

    # adding it to Transaction, and adding it in the serializationGraph
    def begin(self, transaction_id, timestamp):
        # Create a new Transaction object
        new_transaction = Transaction(transaction_id, timestamp)
        new_transaction.TransactionStatus = TransactionStatus.ONGOING
        self.transactions[transaction_id] = new_transaction
        # Add transaction to the serialization graph
        self.serializationGraph[transaction_id] = []

    # add it to transactionhistory and serializationgraph, checking sites at which variable is available at which site
    def read(self, transaction_id, variable_id):
        transaction = self.transactions.get(transaction_id)
        ts_start_time = transaction.startTime
        variable_number = int(variable_id[1:])
        transaction_number = int(transaction_id[1:])
        # --------------------------------Logic for Cycle starts here ----------------------------------------------------
        # Record the read operation
        if transaction_number not in self.transactionHistory:
            self.transactionHistory[transaction_number] = {}
        if variable_number not in self.transactionHistory[transaction_number]:
            self.transactionHistory[transaction_number][variable_number] = []
        self.transactionHistory[transaction_number][variable_number].append("R")

        # Update serializationGraph for Read-Write dependencies
        for txn_id, operations in self.transactionHistory.items():
            if (
                txn_id != transaction_number
                and variable_number in operations
                and "W" in operations[variable_number]
            ):
                if txn_id not in self.serializationGraph:
                    self.serializationGraph[txn_id] = []
                if transaction_number not in self.serializationGraph:
                    self.serializationGraph[transaction_number] = []
                # print(f"from - {txn_id} to - {transaction_id}")
                self.serializationGraph[txn_id].append(transaction_number)
                # print("came until here - read")
                if self.check_for_cycle(transaction_number):
                    self.transactionclosingcycle = transaction_number

        if variable_number % 2 == 0:
            read_value = None

            # Iterate over all sites to find an available copy
            for site_id, site in enumerate(self.site_manager.sites, start=1):
                if site.status is SiteStatus.UP:
                    DMofSite = site.get_data_manager()

                    # Check if the transaction has a local copy at this site
                    if transaction_id in DMofSite.localCopiesPerTransaction:
                        if (
                            variable_id
                            in DMofSite.localCopiesPerTransaction[transaction_id]
                        ):
                            read_value = DMofSite.localCopiesPerTransaction[
                                transaction_id
                            ][variable_id]
                            print(
                                "READ: Transaction {} read {} from site {}. {} = {}, at timestamp {}.".format(
                                    transaction_id,
                                    variable_id,
                                    site_id,
                                    variable_id,
                                    read_value,
                                    self.io_manager.currentTime,
                                )
                            )
                            return

                    # Check the most recent snapshot at this site
                    snapshot = DMofSite.find_most_recent_snapshot(
                        ts_start_time, variable_id
                    )
                    if snapshot:
                        k = list(snapshot.items())[0]
                        timestamp = k[0]
                        read_value = k[1]
                        print(
                            "READ: Transaction {} Read {} from site {}. {} = {}, at timestamp {}.".format(
                                transaction_id,
                                variable_id,
                                site_id,
                                variable_id,
                                read_value,
                                timestamp,
                            )
                        )
                        return

            # If no available copies were found, print a message
            if read_value is None:
                print(
                    "READ: Transaction {} could not read {} from any available site at timestamp {}.".format(
                        transaction_id, variable_id, self.io_manager.currentTime
                    )
                )
        # if odd
        else:
            site_id = variable_number % 10 + 1
            site = self.site_manager.sites[site_id - 1]
            if site.status is SiteStatus.UP:
                DMofSite = site.get_data_manager()
                if transaction_id in DMofSite.localCopiesPerTransaction:
                    if (
                        variable_id
                        in DMofSite.localCopiesPerTransaction[transaction_id]
                    ):
                        read_value = DMofSite.localCopiesPerTransaction[transaction_id][
                            variable_id
                        ]
                        print(
                            "READ: Transaction {} read {} from site {}. {} = {}, at timestamp {}.".format(
                                transaction_id,
                                variable_id,
                                site_id,
                                variable_id,
                                read_value,
                                self.io_manager.currentTime,
                            )
                        )
                    else:
                        snapshot = DMofSite.find_most_recent_snapshot(
                            ts_start_time, variable_id
                        )
                        k = list(snapshot.items())[0]
                        timestamp = k[0]
                        read_value = k[1]
                        print(
                            "READ: Transaction {} Read {} from site {}. {} = {}, at timestamp {}.".format(
                                transaction_id,
                                variable_id,
                                site_id,
                                variable_id,
                                read_value,
                                timestamp,
                            )
                        )
                else:
                    snapshot = DMofSite.find_most_recent_snapshot(
                        ts_start_time, variable_id
                    )
                    k = list(snapshot.items())[0]
                    timestamp = k[0]
                    read_value = k[1]
                    print(
                        "READ: Transaction {} read {} from site {}. {} = {}, at timestamp {}.".format(
                            transaction_id,
                            variable_id,
                            site_id,
                            variable_id,
                            read_value,
                            timestamp,
                        )
                    )
            else:
                self.waitlist[site_id].append(transaction)
                transaction.status = TransactionStatus.WAITING

    # Store it in history then do the required changes
    def write(self, transaction_id, variable_id, value):
        # Retrieve the Transaction object
        transaction = self.transactions.get(transaction_id)
        variable_number = int(variable_id[1:])
        transaction_number = int(transaction_id[1:])

        # If the transaction exists and is currently read-only, set it to read-write
        if transaction and transaction.isReadOnly:
            transaction.isReadOnly = False

        # Record the write operation
        if transaction_number not in self.transactionHistory:
            self.transactionHistory[transaction_number] = {}
        if variable_number not in self.transactionHistory[transaction_number]:
            self.transactionHistory[transaction_number][variable_number] = []
        self.transactionHistory[transaction_number][variable_number].append("W")

        # Update serializationGraph for Write-Write dependencies
        for txn_id, operations in self.transactionHistory.items():
            if (
                txn_id != transaction_number
                and variable_number in operations
                and "W" in operations[variable_number]
            ):
                if txn_id not in self.serializationGraph:
                    self.serializationGraph[txn_id] = []
                if transaction_number not in self.serializationGraph:
                    self.serializationGraph[transaction_number] = []
                # print(f"from - {transaction_id} to - {txn_id}")
                self.serializationGraph[transaction_number].append(txn_id)
                if self.check_for_cycle(transaction_number):
                    self.transactionclosingcycle = transaction_number

        # Update serializationGraph for Read-Write dependencies
        for txn_id, operations in self.transactionHistory.items():
            if (
                txn_id != transaction_number
                and variable_number in operations
                and "R" in operations[variable_number]
            ):
                if txn_id not in self.serializationGraph:
                    self.serializationGraph[txn_id] = []
                if transaction_number not in self.serializationGraph:
                    self.serializationGraph[transaction_number] = []
                # print(f"from - {transaction_id} to - {txn_id}")
                self.serializationGraph[transaction_number].append(txn_id)
                # print("came until here")
                if self.check_for_cycle(transaction_number):
                    # print("Detected cycle")
                    self.transactionclosingcycle = transaction_number
        ##########################################################################################
        # even: add to all up sites
        if variable_number % 2 == 0:
            for site in self.site_manager.sites:
                if site.status is SiteStatus.UP:
                    DMofSite = site.get_data_manager()
                    DMofSite.update_local_copy(transaction_id, variable_id, value)
        # odd:
        else:
            site_id = variable_number % 10 + 1
            site = self.site_manager.sites[site_id - 1]
            if site.status is SiteStatus.UP:
                DMofSite = site.get_data_manager()
                DMofSite.update_local_copy(transaction_id, variable_id, value)
            else:
                self.waitlist[site_id].append(transaction)
                transaction.status = TransactionStatus.WAITING
        ##########################################################################################

    # once it ends we abort transactions forming cycles, and commit all changes to the sites
    def end(self, transaction_id):
        transaction = self.get_trans(transaction_id)
        start_time = transaction.startTime
        transaction_number = int(transaction_id[1:])

        if transaction is None:
            print(f"Transaction {transaction_id} not found.")
            return
        if transaction.isReadOnly:
            return
        site_bool_list = []
        for site in self.site_manager.sites:
            if transaction_id in site.get_data_manager().localCopiesPerTransaction:
                DMofSite = site.get_data_manager()
                if (
                    DMofSite.commit_transaction(
                        transaction_id, start_time, self.io_manager.currentTime
                    )
                    is True
                ):
                    site_bool_list.append(True)
                else:
                    site_bool_list.append(False)
                del site.get_data_manager().localCopiesPerTransaction[transaction_id]

        # print(f"TID - {transaction_number}")
        # print(f"node closing cycle - {self.transactionclosingcycle}")
        if self.transactionclosingcycle == transaction_number:
            print(
                "Transaction T{} aborted due to cycle formation.".format(
                    self.transactionclosingcycle
                )
            )
            # print(f"transaction closing - {self.transactionclosingcycle}")
            # print("cycle formed by this transaction hence aborted\n")
            transaction.status = TransactionStatus.ABORTED
            del self.transactions[transaction_id]
        elif False in site_bool_list:
            print(
                "Transaction {} aborted at timestamp {} due to conflicts.".format(
                    transaction_id, self.io_manager.currentTime
                )
            )
            transaction.status = TransactionStatus.ABORTED
        else:
            print(
                "Transaction {} successfully committed at timestamp {}.".format(
                    transaction_id, self.io_manager.currentTime
                )
            )
            transaction.status = TransactionStatus.COMMITTED

    ################################################################################
    # check if any transaction needs to be aborted due to cycle formation
    def check_for_cycle(self, start_txn):
        visited = set()
        recStack = set()
        return self.dfs_cycle_check(start_txn, visited, recStack)

    def dfs_cycle_check(self, node, visited, recStack):
        if node not in visited:
            visited.add(node)
            recStack.add(node)

            for neighbor in self.serializationGraph.get(node, []):
                if neighbor not in visited and self.dfs_cycle_check(
                    neighbor, visited, recStack
                ):
                    return True
                elif neighbor in recStack:
                    return True

        recStack.remove(node)
        return False

    # in case of failure to restart
    # def resume_transaction(self, transaction_id):
    #     transaction = self.transactions.get(transaction_id)
    #     if transaction is None or transaction.status != TransactionStatus.WAITING:
    #         # Transaction does not exist or is not in a waiting state
    #         return

    #     # Retrieve the history of operations for this transaction
    #     operations = self.transactionHistory.get(transaction_id, {})

    #     # Process each operation that was queued while the transaction was waiting
    #     for variable_id, ops in operations.items():
    #         for op in ops:
    #             if op == 'R':
    #                 self.process_read(transaction_id, variable_id)
    #             elif op == 'W':
    #                 # You need to provide the value for write operations
    #                 # This can be stored in the transaction history or handled differently
    #                 value = self.get_write_value_for_transaction(transaction_id, variable_id)
    #                 self.process_write(transaction_id, variable_id, value)

    #     # Update the status of the transaction
    #     transaction.status = TransactionStatus.ONGOING
