import time


class Variable:
    def __init__(self):
        self.variable_name = ""
        self.value = None
        self.snapshots = []

    def update_snapshot(self, current_timestamp, value):
        # Get the current time as a timestamp
        # Prepend the new snapshot (as a tuple) to the snapshots list
        self.snapshots.append({current_timestamp: value})

    def find_snapshot_before_time(self, timestamp):
        # Iterate through the snapshots to find the most recent one before the given timestamp
        most_recent_snapshot = None
        for snapshot in self.snapshots:
            for ts, value in snapshot.items():
                if ts <= timestamp:
                    most_recent_snapshot = snapshot
        return most_recent_snapshot
