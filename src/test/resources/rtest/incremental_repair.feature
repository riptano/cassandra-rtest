@Incremental
Feature: Incremental Repair
  Verify that incremental repairs work as expected

  Scenario Outline: Complete incremental repair without triggering compaction
    Given a cluster is running and reachable
    And we restore a backup called "repair-qa-small-2"
    And a "incremental" repair would find out-of-sync "<tokens>" ranges for keyspace "repair_quality" within 180 minutes
    When a repair of "repair_quality" keyspace in "incremental" mode with "<parallelism>" validation on "<tokens>" ranges runs
    Then repair finishes within a timeout of 180 minutes
    And repair must have finished successfully
    And all SSTables in "repair_quality" keyspace have a repairedAt value that is different than zero
    And within 30 minutes I cannot find any data in "repair_quality" keyspace showing a pending repair
    And a "incremental" repair would not find out-of-sync "<tokens>" ranges for keyspace "repair_quality" within 180 minutes
    Examples:
      | parallelism | tokens |
      |    parallel |    all |

  Scenario Outline: Complete incremental repair with compaction triggered while validation compaction runs
    Given a cluster is running and reachable
    And we restore a backup called "repair-qa-small-2"
    And a "incremental" repair would find out-of-sync "<tokens>" ranges for keyspace "repair_quality" within 180 minutes
    And all SSTables in "repair_quality" keyspace have a repairedAt value that is equal to zero
    When a repair of "repair_quality" keyspace in "incremental" mode with "<parallelism>" validation on "<tokens>" ranges runs
    Then I wait for validation compactions for any table in "repair_quality" keyspace to start
    And I perform a major compaction on all nodes for the "repair_quality" keyspace
    When repair finishes within a timeout of 180 minutes
    Then repair must have finished successfully
    Then I wait for compactions on all nodes for any table in "repair_quality" keyspace to finish
    And within 30 minutes I cannot find any data in "repair_quality" keyspace showing a pending repair
    And all SSTables in "repair_quality" keyspace have a the same repairedAt
    And a "incremental" repair would not find out-of-sync "<tokens>" ranges for keyspace "repair_quality" within 180 minutes
    Examples:
      | parallelism | tokens |
      |    parallel |    all |

  Scenario Outline: Force terminate incremental repair
    Given a cluster is running and reachable
    And we restore a backup called "repair-qa-small-2"
    When a repair of "repair_quality" keyspace in "incremental" mode with "<parallelism>" validation on "<tokens>" ranges runs
    Then I wait for validation compactions for any table in "repair_quality" keyspace to start
    When I force terminate the repair
    Then I can verify that repair threads get cleaned up within 5 minutes
    And all SSTables in "repair_quality" keyspace have a repairedAt value that is equal to zero

    Examples:
      | parallelism | tokens |
      |    parallel |    all |
