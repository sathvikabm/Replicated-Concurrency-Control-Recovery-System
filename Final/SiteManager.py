from Site import SiteStatus, Site
from Transaction import TransactionStatus
from DataManager import DataManager
from Variable import Variable


class SiteManager:
    def __init__(self):
        self.sites = self.initialize_sites()  # List of Site objects
        self.failureHistory = {}  # Dictionary to maintain site failure history

    # initialize values ith default, and store it in the corresponding sites
    def initialize_sites(self):
        sites = []
        for site_id in range(1, 11):
            site = Site(site_id)
            site.status = SiteStatus.UP  # Assuming sites start as UP
            site.data_manager = DataManager()
            for num in range(1, 21):
                # var_id = 'x' + str(num)
                var = Variable()
                var.variable_name = "x" + str(num)
                var.value = num * 10
                var.snapshots.append({0: num * 10})
                # Even indexed variables are at all sites
                if num % 2 == 0:
                    site.data_manager.committedVariables.append(var)
                # The odd indexed variables are at one site each
                elif num % 10 + 1 == site_id:
                    site.data_manager.committedVariables.append(var)
            sites.append(site)
        return sites

    def fail(self, site_id, current_time):
        # Fails the site with the given site_id
        for site in self.sites:
            if site.id == site_id:
                site.status = SiteStatus.DOWN
                if site_id not in self.failureHistory:
                    self.failureHistory[site_id] = []
                self.failureHistory[site_id].append(
                    self.failureHistory[site_id].append(current_time)
                )
                break

    def recover(self, site_id, current_time, transaction_manager):
        # Recovers the failed site with the given site_id
        for site in self.sites:
            if site.id == site_id:
                site.status = SiteStatus.UP
                break
