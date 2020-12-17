@Full
Feature: Full Repair
  Verify that full repairs work as expected

  Scenario Outline: Complete full repair
    Given a cluster is running and reachable
    And I cleanup the logs
    And we restore a backup called "repair-qa-small"
    And a "full" repair would find out-of-sync "<tokens>" ranges for keyspace "repair_quality"
    When a repair of "repair_quality" keyspace in "full" mode with "<parallelism>" validation on "<tokens>" ranges runs
    Then repair finishes within a timeout of 180 minutes
    And repair must have finished successfully
    And all SSTables in "repair_quality" keyspace have a repairedAt value that is equal to zero
    And a "full" repair would not find out-of-sync "<tokens>" ranges for keyspace "repair_quality"
    Examples:
      | parallelism |  tokens |
      |    parallel |     all |
#      |  sequential | primary |

  Scenario Outline: Force terminate full repair
    Given a cluster is running and reachable
    And I cleanup the logs
    And we restore a backup called "repair-qa-small"
    When a repair of "repair_quality" keyspace in "full" mode with "<parallelism>" validation on "<tokens>" ranges runs
    Then I wait for validation compactions for any table in "repair_quality" keyspace to start
    When I force terminate the repair
    Then I can verify that repair threads get cleaned up within 5 minutes
    Examples:
      | parallelism |  tokens |
      |    parallel |     all |
#      |  sequential | primary |
