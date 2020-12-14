package rtest.minireaper;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import rtest.cassandra.RtestCluster;

import javax.management.Notification;
import javax.management.NotificationListener;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

public class MiniReaper implements NotificationListener {

  private final ConcurrentLinkedDeque<RepairEvent> repairEvents = new ConcurrentLinkedDeque<>();
  private final boolean isUp;
  private final Map<String, JmxProxy> jmxConnections;

  private int latestRepairCommandId = -1;

  public MiniReaper(List<String> contactPoints, Map<String, Integer> jmxPorts) {
    this.jmxConnections = connect(contactPoints, jmxPorts);
    this.isUp = true;
  }

  private Map<String, JmxProxy> connect(List<String> contactPoints, Map<String, Integer> jmxPorts) {
    return contactPoints.stream().map(contactPoint -> {
      int jmxPort = jmxPorts.getOrDefault(contactPoint, 7199);
      JmxProxy jmxProxy = JmxProxy.connect(contactPoint, jmxPort);
      return Pair.of(contactPoint, jmxProxy);
    }).collect(toMap(Pair::getLeft, Pair::getRight));
  }

  public boolean isUp() {
    return this.isUp;
  }

  public boolean startRepairPreview(String keyspaceName, String tokenRanges, String repairMode) {
    String validation = "parallel";
    boolean incremetnal = repairMode.equals("incremental");
    boolean preview = true;
    return startRepair(keyspaceName, validation, tokenRanges, incremetnal, preview);
  }

  public boolean startRepair(String keyspaceName, String validationType, String tokenRanges, boolean incremental) {
    boolean preview = false;
    return startRepair(keyspaceName, validationType, tokenRanges, incremental, preview);
  }

  private boolean startRepair(
      String keyspaceName,
      String validationType,
      String tokenRanges,
      boolean incremental,
      boolean preview
  ) {
    Optional<JmxProxy> alwaysTheSameJmx = alwaysTheSameJmxProxy();
    if (alwaysTheSameJmx.isPresent()) {
      repairEvents.clear();
      JmxProxy jmxProxy = alwaysTheSameJmx.get();
      jmxProxy.addListener(this);
      this.latestRepairCommandId = jmxProxy.startRepair(
          keyspaceName, validationType, tokenRanges, incremental, preview
      );
      return true;
    } else {
      return false;
    }
  }

  private Optional<JmxProxy> alwaysTheSameJmxProxy() {
    return jmxConnections.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .map(Map.Entry::getValue)
        .findFirst();
  }

  public void shutdown() {
    jmxConnections.values().forEach(JmxProxy::close);
  }

  @Override
  public void handleNotification(Notification notification, Object handback) {
    for (String line : notification.getMessage().split("\n")) {
      RepairEvent e = RepairEvent.parseMessage(line);
      boolean added = false;
      while (!added) {
        added = repairEvents.add(e);
      }
    }
  }

  public boolean waitForRepair(int timeoutMinutes) {
    BooleanSupplier repairCompleted = () -> {
      RepairEvent latestEvent = repairEvents.peekLast();
      if (latestEvent instanceof RepairEvent.RepairCompleted) {
        RepairEvent.RepairCompleted completionEvent = (RepairEvent.RepairCompleted) latestEvent;
        if (completionEvent.commandNumber == this.latestRepairCommandId) {
          this.latestRepairCommandId = -1;
          return true;
        }
      }
      return false;
    };
    int sleepStep500Millis = 500;
    return waitFor(repairCompleted, timeoutMinutes, sleepStep500Millis);
  }

  public boolean latestRepairWasSuccess() {
    long successMessageCount = repairEvents.stream()
        .filter(event -> event instanceof RepairEvent.RepairSuccess || event instanceof RepairEvent.PreviewSuccess)
        .count();
    return successMessageCount == 1;
  }

  public boolean allDataWasInSync() {
    long dataInSyncMessageCount = repairEvents.stream()
        .filter(event -> event instanceof RepairEvent.PreviewDataIsInSync)
        .count();
    return dataInSyncMessageCount == 1;
  }

  public Map<String, Long> getSmallestRepairedAt(String keyspace) {
    return jmxConnections.entrySet().stream().collect(toMap(
        Map.Entry::getKey,
        entry -> {
          JmxProxy jmxProxy = entry.getValue();
          return jmxProxy.getRepairStats(keyspace).stream()
              .mapToLong(stats -> stats.minRepaired)
              .min()
              .orElse(-1L);
        }
    ));
  }

  public Map<String, Long> getHighestRepairedAt(String keyspace) {
    return jmxConnections.entrySet().stream().collect(toMap(
        Map.Entry::getKey,
        entry -> {
          JmxProxy jmxProxy = entry.getValue();
          return jmxProxy.getRepairStats(keyspace).stream()
              .mapToLong(stats -> stats.maxRepaired)
              .max()
              .orElse(-1L);
        }
    ));
  }

  public boolean rangesAreInSync(String keyspace, String rangesSelector, String repairMode) {
    boolean previewStarted = this.startRepairPreview(keyspace, rangesSelector, repairMode);
    int FIVE_MINUTES = 5;
    boolean repairCompletedInTime = this.waitForRepair(FIVE_MINUTES);
    return previewStarted && repairCompletedInTime && allDataWasInSync();
  }

  public boolean waitForValidation(String keyspace, int timeoutMinutes) {
    int sleepStepMillis = 50;
    BooleanSupplier anyValidationHappening = () -> jmxConnections.values()
        .stream()
        .anyMatch(jmxProxy -> jmxProxy.hasValidationHappening(keyspace));
    return waitFor(anyValidationHappening, timeoutMinutes, sleepStepMillis);
  }

  public boolean waitForNoCompaction(String keyspace, int timeoutMinutes) {
    int sleepStepMillis = 50;
    BooleanSupplier noCompactionHappening = () -> jmxConnections.values()
        .stream()
        .noneMatch(jmxProxy -> jmxProxy.hasCompactionHappening(keyspace));
    return waitFor(noCompactionHappening, timeoutMinutes, sleepStepMillis);
  }

  private boolean waitFor(BooleanSupplier condition, int timeoutMinutes, long sleepStep) {
    long timeoutMillis = timeoutMinutes * 60 * 1000;
    long durationMillis = 0;
    long waitingStartTime = System.currentTimeMillis();

    while (durationMillis < timeoutMillis) {
      if (condition.getAsBoolean()) {
        return true;
      } else {
        sleepMillis(sleepStep);
        durationMillis = System.currentTimeMillis() - waitingStartTime;
      }
    }
    return false;
  }

  private void sleepMillis(long millis) {
    try {
      TimeUnit.MILLISECONDS.sleep(millis);
    } catch (InterruptedException e) {
    }
  }

  public void terminateRepairEverywhere() {
    jmxConnections.values().forEach(JmxProxy::terminateRepair);
  }

  public boolean waitForRepairThreadsToDisappear(int timeoutMinutes) {
    int sleepStepMillis = 500;
    BooleanSupplier noNodeDoesHasRepairThreads = () -> jmxConnections.values()
        .stream()
        .noneMatch(JmxProxy::hasActiveRepair);
    return waitFor(noNodeDoesHasRepairThreads, timeoutMinutes, sleepStepMillis);
  }

  public boolean waitForNoTableHavingDataPendingRepair(String keyspace, String table, int timeoutMinutes) {
    int sleepStepMillis = 500;
    BooleanSupplier tableHasNoBytesPendingRepair = () -> jmxConnections.values()
        .stream()
        .map(jmxProxy -> jmxProxy.getBytesPendingRepair(keyspace, table))
        .reduce(0L, Long::sum) == 0;
    return waitFor(tableHasNoBytesPendingRepair, timeoutMinutes, sleepStepMillis);
  }

  public boolean triggerMajorCompaction(String keyspace) {
    return jmxConnections.values()
        .stream()
        .allMatch(jmxProxy -> jmxProxy.triggerMajorCompaction(keyspace));
  }

  public long countRepairSessionsInRecentRepair() {
    return repairEvents.stream()
        .filter(event -> event instanceof RepairEvent.RepairSessionFinished)
        .count();
  }

  public int countEndpointsInPreview() {
    return countEndpointsInPreview(repairEvents.stream());
  }

  @VisibleForTesting
  protected static int countEndpointsInPreview(Stream<RepairEvent> events) {
    Set<String> endpointsMentioned = Sets.newHashSet();
    events.filter(event -> event instanceof RepairEvent.RepairPreviewDetail)
        .map(event -> (RepairEvent.RepairPreviewDetail) event)
        .forEach(detailEvent -> {
          endpointsMentioned.add(detailEvent.srcEndpoint);
          endpointsMentioned.add(detailEvent.dstEndpoint);
        });
    return endpointsMentioned.size();
  }

}

