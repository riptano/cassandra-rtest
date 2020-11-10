package rtest;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import io.cucumber.java.Scenario;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import rtest.cassandra.AwsRtestCluster;
import rtest.cassandra.CcmRtestCluster;
import rtest.cassandra.RtestCluster;
import rtest.minireaper.MiniReaper;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.*;

public class StepDefinitions {

  String clusterKind;
  List<String> contactPoints;
  RtestCluster cluster;
  MiniReaper miniReaper;

  @Before
  public void setUp(Scenario scenario) {
    Map<String, String> envVars = System.getenv();

    // try parse the env variables provided by tlp-cluster
    this.contactPoints = envVars.keySet().stream()
        .filter(key -> key.startsWith("CLUSTER_CONTACT_POINT"))
        .map(envVars::get)
        .collect(toList());

    // we did not find any, so let's default to localhost
    if (this.contactPoints.size() == 0) {
      this.contactPoints.add("127.0.0.1");
      this.contactPoints.add("127.0.0.2");
      this.contactPoints.add("127.0.0.3");
    }

    this.clusterKind = envVars.getOrDefault("CLUSTER_KIND", "ccm");

    if (this.clusterKind.equalsIgnoreCase("ccm")) {
      initCcmCluster();
    } else if (this.clusterKind.equalsIgnoreCase("aws")) {
      initAwsCluster();
    } else {
      throw new RuntimeException(String.format("Unknown cluster kind: %s", this.clusterKind));
    }
  }

  @After
  public void cleanUp() {
    if (cluster != null) {
      cluster.shutDown();
    }
    if (miniReaper != null) {
      miniReaper.shutdown();
    }
  }

  private void initCcmCluster() {
    cluster = new CcmRtestCluster(this.contactPoints, 9042);
    miniReaper = new MiniReaper(this.contactPoints, ImmutableMap.of("127.0.0.1", 7100, "127.0.0.2", 7200, "127.0.0.3", 7300), cluster);
  }

  private void initAwsCluster() {
    cluster = new AwsRtestCluster(this.contactPoints, 9042);
    miniReaper = new MiniReaper(this.contactPoints, Maps.newHashMap(), cluster);
  }

  @Given("a cluster is running and reachable")
  public void aClusterIsRunningAndReachable() {
    assertTrue(cluster.isUp());
    assertTrue(miniReaper.isUp());
  }

  @And("the cluster has {int} nodes")
  public void theClusterHasNodes(int nodeCount) {
    assertEquals(
        "Cluster has unexpected number of nodes",
        nodeCount, cluster.getNodeCount()
    );
  }

  @Then("we can run shell commands on all nodes")
  public void weCanRunShellCommandsOnAllNodes() {
    for (String host : cluster.getContactPoints()) {
      assertTrue(
          String.format("Could not run shell commands on node %s", host),
          cluster.canRunShellCommands(host)
      );
    }
  }

  @And("keyspace {string} is present")
  public void keyspaceIsPresent(String keyspaceName) {
    assertTrue(
        "The keyspace is not present",
        cluster.keyspaceIsPresent(keyspaceName)
    );
  }

  @And("I restore the initial state backup")
  public void iRestoreTheInitialStateBackup() {
    cluster.restoreInitialStateBackup();
  }

  @When("a repair of {string} keyspace in {string} mode with {string} validation on {string} ranges runs")
  public void aRepairOfKeyspaceInFullModeWithValidationOnTokenRangesRuns(
      String keyspaceName,
      String repairMode,
      String validationType,
      String tokenRanges
  ) {
    boolean incremental = repairMode.equalsIgnoreCase("incremental");
    boolean repairStarted = miniReaper.startRepair(keyspaceName, validationType, tokenRanges, incremental, cluster);
    assertTrue(repairStarted);
  }

  @Then("repair finishes within a timeout of {int} minutes")
  public void repairFinishesWithinATimeoutOfMinutes(int timeoutMinutes) {
    boolean repairCompletedInTime = miniReaper.waitForRepair(timeoutMinutes);
    assertTrue(repairCompletedInTime);
  }

  @And("repair must have finished successfully")
  public void repairMustHaveFinishedSuccessfully() {
    assertTrue(miniReaper.latestRepairWasSuccess());
  }

  @And("a {string} repair would find out-of-sync {string} ranges for keyspace {string} within {int} minutes")
  public void thereAreOutOfSyncRangesForKeyspace(String repairMode, String tokenRanges, String keyspace, int timeoutMinutes) {
    assertFalse(
        "The keyspace was repaired when we expected otherwise",
        miniReaper.rangesAreInSync(keyspace, tokenRanges, repairMode, timeoutMinutes, cluster)
    );
    assertTrue(
        "The preview repair encountered an error",
        miniReaper.latestRepairWasSuccess()
    );
  }

  @And("a {string} repair would not find out-of-sync {string} ranges for keyspace {string} within {int} minutes")
  public void thereAreNoOutOfSyncRangesForKeyspace(String repairMode, String rangesSelector, String keyspace, int timeoutMinutes) {
    assertTrue(
        "The keyspace was not repaired when we expected otherwise",
        miniReaper.rangesAreInSync(keyspace, rangesSelector, repairMode, timeoutMinutes, cluster)
    );
    assertTrue(
        "The preview repair encountered an error",
        miniReaper.latestRepairWasSuccess()
    );
  }

  @And("all SSTables in {string} keyspace have a repairedAt value that is equal to zero")
  public void allSSTablesInKeyspaceHaveARepairedAtValueThatIsEqualToZero(String keyspace) {
    Map<String, Long> highestRepairedAt = miniReaper.getHighestRepairedAt(keyspace);
    assertTrue(
        "There were no files to check",
        highestRepairedAt.size() > 0
    );
    highestRepairedAt.forEach((host, value) -> {
      long highestRepairedAcrossKeyspace = value;
      assertEquals(String.format(
          "Host %s had a repairedAt value different than 0 from some SStable in keyspace %s", host, keyspace),
          0, highestRepairedAcrossKeyspace);
    });
  }

  @And("all SSTables in {string} keyspace have a repairedAt value that is different than zero")
  public void allSSTablesInKeyspaceHaveARepairedAtValueThatIsDifferentThanZero(String keyspace) {
    Map<String, Long> smallestRepairedAt = miniReaper.getSmallestRepairedAt(keyspace);
    assertTrue(
        "There were no files to check",
        smallestRepairedAt.size() > 0
    );
    smallestRepairedAt.forEach((host, value) -> {
      long smallestRepairedAcrossKeyspace = value;
      assertTrue(
          String.format("Host %s had a repairedAt value of 0 from some SStable in keyspace %s", host, keyspace),
          smallestRepairedAcrossKeyspace != 0
      );
    });
  }

  @And("all SSTables in {string} keyspace have a the same repairedAt")
  public void allSSTablesInKeyspaceHaveATheSameRepairedAt(String keyspace) {
    Map<String, Long> smallestRepairedAts = miniReaper.getSmallestRepairedAt(keyspace);
    Map<String, Long> highestRepairedAts = miniReaper.getHighestRepairedAt(keyspace);

    assertTrue(
        "There were no files to check",
        smallestRepairedAts.size() > 0 && highestRepairedAts.size() > 0
    );

    smallestRepairedAts.forEach((host, smallestRepairedAt) -> {
      long biggestRepairedAt = highestRepairedAts.get(host);
      assertEquals(
          String.format("Host %s does not have consistent repairedAt", host),
          smallestRepairedAt.longValue(), biggestRepairedAt);
    });

    long distinctRepairedAts = Stream
        .concat(
            smallestRepairedAts.values().stream(),
            highestRepairedAts.values().stream())
        .distinct()
        .count();
    assertEquals(
        "The repairedAt was inconsistent across the cluster",
        1, distinctRepairedAts
    );
  }

  @Then("I wait for validation compactions for any table in {string} keyspace to start")
  public void iWaitForValidationCompactionsForAnyTableInKeyspaceToStart(String keyspace) {
    int oneMinute = 1;
    boolean validationIsHappening = miniReaper.waitForValidation(keyspace, oneMinute);
    assertTrue(
        "Did not see validation happening",
        validationIsHappening
    );
  }

  @When("I force terminate the repair")
  public void iForceTerminateTheRepair() {
    miniReaper.terminateRepairEverywhere();
  }

  @Then("I can verify that repair threads get cleaned up within {int} minutes")
  public void iCanVerifyThatRepairThreadsGetCleanedUpWithinMinutes(int timeoutMinutes) {
    assertTrue(
        "Validation threads did not disappear in time",
        miniReaper.waitForRepairThreadsToDisappear(timeoutMinutes)
    );
  }

  @And("within {int} minutes I cannot find any data in {string} keyspace showing a pending repair")
  public void withinMinutesICannotFindAnyNonSystemDataShowingAPendingRepair(int timeoutMinutes, String keyspace) {
    cluster.getTableNamesIn(keyspace).forEach(table -> {
      boolean tableHasDataPending = miniReaper.waitForNoTableHavingDataPendingRepair(keyspace, table, timeoutMinutes);
      assertTrue(
          String.format("%s.%s still had data pending repair after the timeout", keyspace, table),
          tableHasDataPending
      );
    });
  }

  @When("I perform a major compaction on all nodes for the {string} keyspace")
  public void iPerformAMajorCompactionOnAllNodesForTheKeyspace(String keyspace) {
    assertTrue(
        "There was an error triggering major compactions",
        miniReaper.triggerMajorCompaction(keyspace));
  }

  @Then("I wait for compactions on all nodes for any table in {string} keyspace to finish")
  public void iWaitForCompactionsOnAllNodesForAnyTableInKeyspaceToFinish(String keyspace) {
    int timeout = 30;
    boolean compactionIsNotHappening = miniReaper.waitForNoCompaction(keyspace, timeout);
    assertTrue(
        "Compaction did not finish in time",
        compactionIsNotHappening
    );
  }

  @And("{int} repair session was used to process all ranges")
  public void repairSessionWasUsedToProcessAllRanges(int expectedRepairSessionCount) {
    assertEquals(
        "The repair used more than one repair session",
        expectedRepairSessionCount, miniReaper.countRepairSessionsInRecentRepair()
    );
  }

  @And("there would be exactly {int} endpoints mentioned during the repair preview")
  public void thereWouldBeExactlyEndpointsMentionedDuringTheRepairPreview(int expectedEndpointsCount) {
    assertEquals(
        "Wrong number of endpoints was involved in the repair (preview)",
        expectedEndpointsCount, miniReaper.countEndpointsInPreview()
    );
  }

  @When("we restore a backup called {string}")
  public void weRestoreABackupCalled(String backupName) throws InterruptedException {
    assertTrue(
      "Restoring backup failed",
      cluster.restoreBackup(backupName)
    );
    miniReaper.reconnect(5);
  }
}
