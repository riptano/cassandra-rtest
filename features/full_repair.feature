Feature: Full Repair
  Verify that full repairs work as expected

  Scenario Outline: Sequential repair of all token ranges
    Given a "<cluster_provider>" cluster named "full-sequential-all_ranges" is running and reachable via "<contact_point>"
    When a repair of "tlp-stress" keyspace  with "sequential" validation across "all" token ranges runs
    Then no anticompaction happened
    Then 0 ranges are out of sync
    Examples:
      | cluster_provider | contact_point |
      | ccm              |     localhost |
