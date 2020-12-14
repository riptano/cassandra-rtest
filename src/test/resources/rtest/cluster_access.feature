@DryRun
Feature: Cluster access
  Quickly evaluate if cluster is configured well enough to run tests against

  Scenario: Access the Cassandra cluster
    Given a cluster is running and reachable

  Scenario: Validate the cluster setup
    Given a cluster is running and reachable
    And the cluster has 3 nodes
    Then we can run shell commands on all nodes
    And keyspace "repair_quality" is present
    And keyspace "repair_quality_rf2" is present
