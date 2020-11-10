
from cassandra.cluster import Cluster


class RtestCluster(object):

    def __init__(self, contact_host: str, contact_port: int = 9042):
        self.cluster = Cluster(contact_points=[contact_host], port=contact_port)
        try:
            self.session = self.cluster.connect()
            self.is_up = True
        except Exception:
            self.is_up = False

