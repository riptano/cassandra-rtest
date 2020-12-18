@DryRun
Feature: Cluster access
  Quickly evaluate if cluster is configured well enough to run tests against

  Scenario: Access the Cassandra cluster
    Given a cluster is running and reachable

  Scenario: Validate the cluster setup
    Given a cluster is running and reachable
    And the cluster has 3 nodes
    Then we can run shell commands on all nodes

  Scenario: Validate Medusa can restore a backup
    Given a cluster is running and reachable
    And the cluster has 3 nodes
    Then we can run shell commands on all nodes
    When we restore a backup called "repair-qa-tiny"
    Then keyspace "repair_quality" is present
    And keyspace "repair_quality_rf2" is present
    And a cluster is running and reachable

  Scenario: Validate we can restore backup again using the same cluster
    Given a cluster is running and reachable
    And the cluster has 3 nodes
    Then we can run shell commands on all nodes
    When we restore a backup called "repair-qa-tiny"
    Then keyspace "repair_quality" is present
    And keyspace "repair_quality_rf2" is present
    And a cluster is running and reachable
