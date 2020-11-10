import logging

from behave import given, when, then

from rtest.ccm import CcmApi
from rtest.cluster import RtestCluster


@given(r'a "{cluster_provider}" cluster named "{cluster_name}" is running and reachable via "{contact_point}"')
def _a_cluster_named_is_running(context, cluster_provider, cluster_name, contact_point):

    ccm = CcmApi()

    if cluster_provider == 'ccm':
        ccm.create(cluster_name, '3.11.8', 1)

    rtest_cluster = RtestCluster(contact_point)
    assert rtest_cluster.is_up is True

    logging.info('Cluster by {} with name {} is up!'.format(cluster_provider, cluster_name))
    ccm.destroy(cluster_name)


@when(r'a repair of "{keyspace_name}" keyspace  with "{validation_parallelism}" validation across "{token_ranges}" token ranges runs')
def _a_repair_of_keyspace_with_validation_across_token_ranges_runs(context, keyspace_name, validation_parallelism, token_ranges):
    logging.info('Running repair of {} with {} validation across {}'.format(keyspace_name, validation_parallelism, token_ranges))
    pass


@then(r'no anticompaction happened')
def _no_anticompaction_happened(context):
    pass


@then(r'{range_count} ranges are out of sync')
def _ranges_are_out_of_sync(context, range_count):
    logging.info('Would check {} ranges to be out of sync'.format(range_count))
