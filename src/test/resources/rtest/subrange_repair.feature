@Subrange
Feature: Subrange Repair
  Verify that subrange repairs work as expected

  Scenario Outline: Complete subrange repair on a single token range
    Given a cluster is running and reachable
    And I cleanup the logs
    And I restore the initial state backup
    And a "full" repair would find out-of-sync "firstHalfToken" ranges for keyspace "repair_quality"
    And a "full" repair would find out-of-sync "secondHalfToken" ranges for keyspace "repair_quality"
    When a repair of "repair_quality" keyspace in "full" mode with "<parallelism>" validation on "firstHalfToken" ranges runs
    Then repair finishes within a timeout of 180 minutes
    And repair must have finished successfully
    And a "full" repair would not find out-of-sync "firstHalfToken" ranges for keyspace "repair_quality"
    And a "full" repair would find out-of-sync "secondHalfToken" ranges for keyspace "repair_quality"
    And all SSTables in "repair_quality" keyspace have a repairedAt value that is equal to zero
    Examples:
      | parallelism |
      |    parallel |

  Scenario Outline: Complete subrange repair on a token ranges with same replicas
    Given a cluster is running and reachable
    And I cleanup the logs
    And I restore the initial state backup
    And a "full" repair would find out-of-sync "2replicasShared" ranges for keyspace "repair_quality_rf2"
    And there would be exactly 2 endpoints mentioned during the repair preview
    When a repair of "repair_quality_rf2" keyspace in "full" mode with "<parallelism>" validation on "2replicasShared" ranges runs
    Then repair finishes within a timeout of 180 minutes
    And repair must have finished successfully
    And 1 repair session was used to process all ranges
    And all SSTables in "repair_quality_rf2" keyspace have a repairedAt value that is equal to zero
    And a "full" repair would not find out-of-sync "2replicasShared" ranges for keyspace "repair_quality_rf2"
    Examples:
      | parallelism |
      | PARALLEL    |

  Scenario Outline: Complete subrange repair on a token ranges with different replicas
    Given a cluster is running and reachable
    And I cleanup the logs
    And I restore the initial state backup
    And a "full" repair would find out-of-sync "3replicasShared" ranges for keyspace "repair_quality_rf2"
    And there would be exactly 3 endpoints mentioned during the repair preview
    When a repair of "repair_quality_rf2" keyspace in "full" mode with "<parallelism>" validation on "3replicasShared" ranges runs
    Then repair finishes within a timeout of 180 minutes
    And repair must have finished successfully
    And 2 repair session was used to process all ranges
    And all SSTables in "repair_quality_rf2" keyspace have a repairedAt value that is equal to zero
    And a "full" repair would not find out-of-sync "2replicasShared" ranges for keyspace "repair_quality_rf2"
    Examples:
      | parallelism |
      | PARALLEL    |

  Scenario Outline: Force terminate subrange repair
    Given a cluster is running and reachable
    And I cleanup the logs
    And I restore the initial state backup
    When a repair of "repair_quality" keyspace in "full" mode with "<parallelism>" validation on "firstHalfToken" ranges runs
    Then I wait for validation compactions for any table in "repair_quality" keyspace to start
    When I force terminate the repair
    Then I can verify that repair threads get cleaned up within 5 minutes
    Examples:
      | parallelism |
      |    parallel |
