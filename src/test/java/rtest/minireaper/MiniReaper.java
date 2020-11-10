package rtest.minireaper;

import com.beust.jcommander.internal.Maps;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelDuplexHandler;
import rtest.cassandra.RtestCluster;

import javax.management.Notification;
import javax.management.NotificationListener;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigInteger;

public class MiniReaper implements NotificationListener, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(MiniReaper.class);

  private final ConcurrentLinkedDeque<RepairEvent> repairEvents = new ConcurrentLinkedDeque<>();
  private final Map<String, JmxProxy> jmxConnections;
  private final List<String> contactPoints;
  private final Map<String, Integer> contactPorts;
  private final RtestCluster cluster;

  private int latestRepairCommandId = -1;

  public MiniReaper(List<String> contactPoints, Map<String, Integer> jmxPorts, RtestCluster cluster) {
    this.contactPoints = contactPoints;
    this.contactPorts = jmxPorts;
    this.jmxConnections = connect(contactPoints, jmxPorts);
    this.cluster = cluster;
  }

  private Map<String, JmxProxy> connect(List<String> contactPoints, Map<String, Integer> jmxPorts) {
    return contactPoints.stream().map(contactPoint -> {
      int jmxPort = jmxPorts.getOrDefault(contactPoint, 7199);
      JmxProxy jmxProxy = JmxProxy.connect(contactPoint, jmxPort);
      return Pair.of(contactPoint, jmxProxy);
    }).collect(toMap(Pair::getLeft, Pair::getRight));
  }

  public void reconnect(int maxAttempts) throws InterruptedException {
    int attempts=0;
    while (true) {
      try {
        shutdown();
        this.jmxConnections.clear();
        this.jmxConnections.putAll(connect(this.contactPoints, this.contactPorts));
        break;
      } catch (RuntimeException e) {
        ++attempts;
        if (attempts < maxAttempts) {
          Thread.sleep(60000);
        } else {
          throw e;
        }
      }
    }

  }

  public boolean isUp() {
    try {
      jmxConnections.values().forEach(JmxProxy::isUp);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  public boolean startRepairPreview(String keyspaceName, String tokenRanges, String repairMode, RtestCluster cluster) {
    String validation = "parallel";
    boolean incremetnal = repairMode.equals("incremental");
    boolean preview = true;
    return startRepair(keyspaceName, validation, tokenRanges, incremetnal, preview, cluster);
  }

  public boolean startRepair(String keyspaceName, String validationType, String tokenRanges, boolean incremental,
      RtestCluster cluster) {
    boolean preview = false;
    return startRepair(keyspaceName, validationType, tokenRanges, incremental, preview, cluster);
  }

  private boolean startRepair(String keyspaceName, String validationType, String tokenRanges, boolean incremental,
      boolean preview, RtestCluster cluster) {
    Boolean primaryRangeRepair = false;
    LOG.debug("starting repair on {}", tokenRanges);
    if (tokenRanges.equals("primary")) {
      primaryRangeRepair = true;
    }
    Optional<Pair<String, List<Segment>>> tokenRangesWithEndpoint = getTokenRangesWithEndpoint(tokenRanges, cluster, keyspaceName);
    Optional<List<Segment>> explicitTokenRanges = Optional.empty();
    if (tokenRangesWithEndpoint.isPresent()) {
      explicitTokenRanges = Optional.of(tokenRangesWithEndpoint.get().getRight());
    }
    Optional<JmxProxy> alwaysTheSameJmx = alwaysTheSameJmxProxy(tokenRangesWithEndpoint);
    if (alwaysTheSameJmx.isPresent()) {
      repairEvents.clear();
      JmxProxy jmxProxy = alwaysTheSameJmx.get();
      jmxProxy.addListener(this);
      this.latestRepairCommandId = jmxProxy.startRepair(
        keyspaceName, validationType, explicitTokenRanges, primaryRangeRepair, incremental, preview, cluster);
      return true;
    } else {
      return false;
    }
  }

  private Optional<JmxProxy> alwaysTheSameJmxProxy(Optional<Pair<String, List<Segment>>> tokenRangesWithEndpoint) {
    Optional<JmxProxy> jmxProxy = jmxConnections.entrySet().stream().sorted(Map.Entry.comparingByKey())
        .map(Map.Entry::getValue).findFirst();
    LOG.debug("jmxConnections: {}", jmxConnections.keySet());
    if (tokenRangesWithEndpoint.isPresent()) {
      jmxProxy = jmxConnections.entrySet().stream().filter(
        endpoint -> endpoint.getKey().equals(tokenRangesWithEndpoint.get().getLeft())).map(Map.Entry::getValue).findFirst();
    }
    LOG.debug("JMX proxy : {}", jmxProxy.get());
    return jmxProxy;
  }

  private Optional<Pair<String, List<Segment>>> getTokenRangesWithEndpoint(String tokenRanges, RtestCluster cluster, String keyspace) {
    Optional<Pair<String, List<Segment>>> endpointWithRanges = Optional.empty();
    List<Segment> explicitTokenRanges = null;
    if ("firstHalfToken".equals(tokenRanges)) {
      Segment segment = new Segment(cluster.getFirstHalfTokenRange());
      explicitTokenRanges = Collections.singletonList(segment);
    }
    if ("secondHalfToken".equals(tokenRanges)) {
      // splits the ranges between replicas in 2, then takes the right halves
      Segment segment = new Segment(cluster.getSecondHalfTokenRange());
      explicitTokenRanges = Collections.singletonList(segment);
    }
    if ("2replicasShared".equals(tokenRanges)) {
      // generates a bunch of ranges that are shared between 2 replicas
      Pair<Pair<BigInteger, BigInteger>, Pair<BigInteger, BigInteger>> ranges = cluster
          .getTwoReplicasSharedTokenRanges();
      Segment firstRange = new Segment(ranges.getLeft());
      Segment secondRange = new Segment(ranges.getRight());
      explicitTokenRanges = Arrays.asList(firstRange, secondRange);
    }
    if ("3replicasShared".equals(tokenRanges)) {
      // generates a bunch of ranges that are shared between 3 replicas
      Pair<Pair<BigInteger, BigInteger>, Pair<BigInteger, BigInteger>> ranges = cluster
          .getThreeReplicasSharedTokenRanges();
      Segment firstRange = new Segment(ranges.getLeft());
      Segment secondRange = new Segment(ranges.getRight());
      explicitTokenRanges = Arrays.asList(firstRange, secondRange);
    }

    if (explicitTokenRanges != null) {
      String candidateEndpoint = getCandidateEndpoint(explicitTokenRanges, keyspace);
      endpointWithRanges = Optional.of(Pair.of(candidateEndpoint, explicitTokenRanges));
      LOG.debug("Explicit token ranges {} / {}", candidateEndpoint, explicitTokenRanges);
    }
    

    return endpointWithRanges;
  }

  private String getCandidateEndpoint(List<Segment> ranges, String keyspace) {
    Map<Segment, List<String>> segmentToEndpoints = Maps.newHashMap();
    JmxProxy jmxProxy = alwaysTheSameJmxProxy(Optional.empty()).get();
    // Retrieve the token range to endpoint map for our keyspace to be repaired
    Map<List<String>, List<String>> tokenToEndpointMap = jmxProxy.getRangeToEndpointWithPortMap(keyspace);
    // Get the endpoints for the ranges to repair
    for (Segment rangeToRepair:ranges) {
      for (Entry<List<String>, List<String>> clusterRange:tokenToEndpointMap.entrySet()) {
        Segment rangeSegment = new Segment(clusterRange.getKey().get(0), clusterRange.getKey().get(1));
        if (rangeSegment.encloses(rangeToRepair)) {
          segmentToEndpoints.put(rangeToRepair, clusterRange.getValue());
          break;
        }
      }
    }

    // Find the overlapping node(s) for all the ranges we need to repair if there are several
    if (segmentToEndpoints.entrySet().size() > 1) {
      List<String> overlappingEndpoints = segmentToEndpoints.entrySet().stream().findFirst().get().getValue();
      for (Entry<Segment, List<String>> candidateEndpoints : segmentToEndpoints.entrySet()) {
        overlappingEndpoints.retainAll(candidateEndpoints.getValue());
      }
      // Returning the first overlapping endpoint and removing the port part
      return overlappingEndpoints.get(0).split(":")[0];
    } else {
      // Only one range to repair, we'll return the first endpoint for that range
      return segmentToEndpoints.entrySet().stream().findFirst().get().getValue().get(0).split(":")[0];
    }
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
      if (!repairEvents.isEmpty()) {
        RepairEvent latestEvent = repairEvents.peekLast();
        if (latestEvent != null) {
          if (latestEvent instanceof RepairEvent.RepairCompleted) {
            // Actual repair
            RepairEvent.RepairCompleted completionEvent = (RepairEvent.RepairCompleted) latestEvent;
            if (completionEvent.commandNumber == this.latestRepairCommandId) {
              this.latestRepairCommandId = -1;
              return true;
            }
          } else if (latestEvent.sourceMessage.contains("Repair preview")
              && latestEvent.sourceMessage.contains("finished in")) {
            // Repair preview
            List<String> previewEventsMessages = Lists.newArrayList();
            for (RepairEvent event : repairEvents) {
              previewEventsMessages.add(event.sourceMessage + " - type: " + event.getClass().getName());
              if (event instanceof RepairEvent.PreviewSuccess) {
                return true;
              }
            }
            // Preview failed :(
            LOG.error("Repair preview failed. Here are the notifications that were received:");
            for (String message : previewEventsMessages) {
              LOG.error(message);
            }
            return false;
          }
        }
      } else {
        LOG.debug("No event in the repair event queue yet...");
      }
      return false;
    };
    
    return waitFor(repairCompleted, timeoutMinutes, "Waiting for repair/preview to go through...");
  }

  public boolean latestRepairWasSuccess() {
    long successMessageCount = repairEvents.stream()
        .filter(event -> event instanceof RepairEvent.RepairSuccess || event instanceof RepairEvent.PreviewSuccess)
        .count();
    if (successMessageCount < 1) {
      repairEvents.stream().forEach(event -> LOG.error("Repair event: {}", event.sourceMessage));
    }
    return successMessageCount == 1;
  }

  public boolean allDataWasInSync() {
    long dataInSyncMessageCount = repairEvents.stream()
        .filter(event -> event instanceof RepairEvent.PreviewDataIsInSync).count();
    return dataInSyncMessageCount == 1;
  }

  public Map<String, Long> getSmallestRepairedAt(String keyspace) {
    return jmxConnections.entrySet().stream().collect(toMap(Map.Entry::getKey, entry -> {
      JmxProxy jmxProxy = entry.getValue();
      return jmxProxy.getRepairStats(keyspace).stream().mapToLong(stats -> stats.minRepaired).min().orElse(-1L);
    }));
  }

  public Map<String, Long> getHighestRepairedAt(String keyspace) {
    return jmxConnections.entrySet().stream().collect(toMap(Map.Entry::getKey, entry -> {
      JmxProxy jmxProxy = entry.getValue();
      return jmxProxy.getRepairStats(keyspace).stream().mapToLong(stats -> stats.maxRepaired).max().orElse(-1L);
    }));
  }

  public boolean rangesAreInSync(String keyspace, String rangesSelector, String repairMode, int timeoutMinutes,
      RtestCluster cluster) {
    boolean previewStarted = this.startRepairPreview(keyspace, rangesSelector, repairMode, cluster);
    boolean repairCompletedInTime = this.waitForRepair(timeoutMinutes);
    return previewStarted && repairCompletedInTime && allDataWasInSync();
  }

  public boolean waitForValidation(String keyspace, int timeoutMinutes) {
    BooleanSupplier anyValidationHappening = () -> jmxConnections.values().stream()
        .anyMatch(jmxProxy -> jmxProxy.hasValidationHappening(keyspace));
    return waitFor(anyValidationHappening, timeoutMinutes, "Waiting for validation compactions to start...");
  }

  public boolean waitForNoCompaction(String keyspace, int timeoutMinutes) {
    BooleanSupplier noCompactionHappening = () -> jmxConnections.values().stream()
        .noneMatch(jmxProxy -> jmxProxy.hasCompactionHappening(keyspace));
    return waitFor(noCompactionHappening, timeoutMinutes, "Waiting for compactions to finish...");
  }

  private boolean waitFor(BooleanSupplier condition, int timeoutMinutes, String waitMessage) {
    long timeoutMillis = timeoutMinutes * 60 * 1000;
    long durationMillis = 0;
    long waitingStartTime = System.currentTimeMillis();

    while (durationMillis < timeoutMillis) {
      if (condition.getAsBoolean()) {
        return true;
      } else {
        if (cluster.displayWaitMessage()) {
          System.err.println(waitMessage);
        }
        sleepMillis(cluster.getSleepTimeBetweenChecks());
        durationMillis = System.currentTimeMillis() - waitingStartTime;
      }
    }
    LOG.error("Timed out waiting for {} minutes", timeoutMinutes);
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
    BooleanSupplier noNodeDoesHasRepairThreads = () -> jmxConnections.values().stream()
        .noneMatch(JmxProxy::hasActiveRepair);
    return waitFor(noNodeDoesHasRepairThreads, timeoutMinutes, "Waiting for repair threads to disappear...");
  }

  public boolean waitForNoTableHavingDataPendingRepair(String keyspace, String table, int timeoutMinutes) {
    BooleanSupplier tableHasNoBytesPendingRepair = () -> jmxConnections.values().stream()
        .map(jmxProxy -> jmxProxy.getBytesPendingRepair(keyspace, table)).reduce(0L, Long::sum) == 0;
    return waitFor(tableHasNoBytesPendingRepair, timeoutMinutes, "Waiting for sstables to exit pending repair state...");
  }

  public boolean triggerMajorCompaction(String keyspace) {
    return jmxConnections.values().stream().allMatch(jmxProxy -> jmxProxy.triggerMajorCompaction(keyspace));
  }

  public long countRepairSessionsInRecentRepair() {
    return repairEvents.stream().filter(event -> event instanceof RepairEvent.RepairSessionFinished).count();
  }

  public int countEndpointsInPreview() {
    return countEndpointsInPreview(repairEvents.stream());
  }

  @VisibleForTesting
  protected static int countEndpointsInPreview(Stream<RepairEvent> events) {
    Set<String> endpointsMentioned = Sets.newHashSet();
    events.filter(event -> event instanceof RepairEvent.RepairPreviewDetail)
        .map(event -> (RepairEvent.RepairPreviewDetail) event).forEach(detailEvent -> {
          endpointsMentioned.add(detailEvent.srcEndpoint);
          endpointsMentioned.add(detailEvent.dstEndpoint);
        });
    return endpointsMentioned.size();
  }

  @Override
  public void close() throws IOException {
    shutdown();
  }

}

