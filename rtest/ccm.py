import logging

from ccmlib import common
from ccmlib.cluster import Cluster as CCMCluster
from ccmlib.cluster_factory import ClusterFactory as CCMClusterFactory
from pathlib import Path


class CcmApi(object):

    ccm_path: str = '{}/.ccm'.format(str(Path.home()))
    clusters: dict = dict()

    def __init__(self):
        pass

    def create(self, cluster_name: str, cassandra_version: str, node_count: int):
        cluster_kwargs = {"version": cassandra_version}
        self._start_cluster(cluster_name, cluster_kwargs, node_count)

    def destroy(self, cluster_name: str):
        self._stop_cluster(cluster_name)

    def _start_cluster(self, cluster_name: str, cluster_kwargs: dict, node_count: int):
        """Docstring."""
        try:
            cluster: CCMCluster = CCMClusterFactory.load(self.ccm_path, cluster_name)
            logging.debug(
                "Found existing ccm {} cluster; clearing".format(cluster_name)
            )
            cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
            self.clusters[cluster_name] = cluster
        except Exception:
            logging.debug(
                "Creating new ccm cluster {} with {}",
                cluster_name,
                cluster_kwargs,
            )
            cluster = CCMCluster(
                self.ccm_path, cluster_name, **cluster_kwargs
            )
            cluster.set_configuration_options({"start_native_transport": True})
            common.switch_cluster(self.ccm_path, cluster_name)
            cluster.populate(node_count, ipformat=None)
            cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
            self.clusters[cluster_name] = cluster

    def _stop_cluster(self, cluster_name):
        self.clusters[cluster_name].stop()
        self.clusters[cluster_name].remove()
        self.clusters[cluster_name] = None
