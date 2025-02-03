from Variable import Variable


class DataManager:
    def __init__(self):
        self.committedVariables = []  # self.initialize_variables()
        self.localCopiesPerTransaction = {}

    def update_local_copy(self, transaction_id, variable_id, value):
        if transaction_id not in self.localCopiesPerTransaction:
            self.localCopiesPerTransaction[transaction_id] = {}
        self.localCopiesPerTransaction[transaction_id][variable_id] = value

    def commit_transaction(self, transaction_id, start_time, commit_timestamp):
        # if the transaction will modify var in this site
        # print("trans id: ", transaction_id)
        bool_list = []
        for variable_id, value in self.localCopiesPerTransaction[
            transaction_id
        ].items():
            # print("want to modify:",variable_id, value)
            # print("start time: ", start_time)
            # print("now commit time: ", commit_timestamp)
            recent_snapshot = self.find_most_recent_snapshot(
                commit_timestamp, variable_id
            )
            k = list(recent_snapshot.items())[0]
            recent_timestamp = k[0]
            recent_value = k[1]
            # print("recent timestemp:", recent_timestamp, "value:", recent_value)
            if recent_timestamp < commit_timestamp and start_time >= recent_timestamp:
                for variable in self.committedVariables:
                    if variable.variable_name == variable_id:
                        # print("snapshots:", variable.snapshots)
                        variable.value = value
                        variable.update_snapshot(commit_timestamp, value)
                bool_list.append(True)
            else:
                bool_list.append(False)
        if False in bool_list:
            return False
        else:
            return True

    # get the most recent snapshot from the commotted  variables
    def find_most_recent_snapshot(self, timestamp, variable_id):
        for variable in self.committedVariables:
            if variable.variable_name == variable_id:
                return variable.find_snapshot_before_time(timestamp)
        return None

    def get_variables(self):
        return {
            variable.variable_name: variable.value
            for variable in self.committedVariables
        }
