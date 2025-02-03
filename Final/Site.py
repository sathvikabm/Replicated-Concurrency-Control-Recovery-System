from enum import Enum
from DataManager import DataManager
from Variable import Variable


class SiteStatus(Enum):
    UP = 1
    DOWN = 2


class Site:
    def __init__(self, site_id):
        self.status = SiteStatus.DOWN  # Default status can be set to DOWN
        self.id = site_id
        self.data_manager = None

    def get_data_manager(self):
        if self.data_manager is None:
            self.data_manager = DataManager()
        return self.data_manager
