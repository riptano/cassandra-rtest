package rtest.cassandra.jmx;

import rtest.cassandra.RtestCluster;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import javax.management.InstanceNotFoundException;
import javax.management.JMX;
import javax.management.ListenerNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.google.common.collect.Lists;
import org.apache.cassandra.db.compaction.CompactionManagerMBean;
import org.apache.cassandra.repair.consistent.admin.RepairStats;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.service.ActiveRepairServiceMBean;
import org.apache.cassandra.service.StorageServiceMBean;
import org.apache.cassandra.streaming.PreviewKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toList;

public final class JmxProxy
{

    private static final Logger LOG = LoggerFactory.getLogger(JmxProxy.class);
    private static final String JMX_URL = "service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi";

    private final JMXConnector jmxConn;
    private final MBeanServerConnection mbeanServerConn;
    private final StorageServiceMBean ssProxy;
    private final ActiveRepairServiceMBean rsProxy;
    private final CompactionManagerMBean cmProxy;

    private JmxProxy(final JMXConnector jmxConn,
            final MBeanServerConnection mbeanServerConn,
            final StorageServiceMBean ssProxy,
            final ActiveRepairServiceMBean rsProxy,
            final CompactionManagerMBean cmProxy)
    {
        this.jmxConn = jmxConn;
        this.mbeanServerConn = mbeanServerConn;
        this.ssProxy = ssProxy;
        this.rsProxy = rsProxy;
        this.cmProxy = cmProxy;
    }

    public static JmxProxy connect(final String host, final int port)
    {

        JMXServiceURL jmxUrl;
        try
        {
            jmxUrl = new JMXServiceURL(String.format(JMX_URL, host, port));
        }
        catch (MalformedURLException e)
        {
            throw new RuntimeException("Failure during preparations for JMX connection", e);
        }

        JMXConnector jmxConn;
        try
        {
            jmxConn = JMXConnectorFactory.connect(jmxUrl);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failure during establishing JMX connection", e);
        }

        try
        {
            ObjectName ssObjectName = new ObjectName("org.apache.cassandra.db:type=StorageService");
            MBeanServerConnection mbeanServerConn = jmxConn.getMBeanServerConnection();
            StorageServiceMBean ssProxy = JMX.newMBeanProxy(mbeanServerConn, ssObjectName, StorageServiceMBean.class);

            ObjectName rsObjectName = new ObjectName("org.apache.cassandra.db:type=RepairService");
            ActiveRepairServiceMBean rsProxy = JMX.newMBeanProxy(mbeanServerConn, rsObjectName,
                    ActiveRepairServiceMBean.class);

            ObjectName cmObjectName = new ObjectName("org.apache.cassandra.db:type=CompactionManager");
            CompactionManagerMBean cmProxy = JMX.newMBeanProxy(mbeanServerConn, cmObjectName,
                    CompactionManagerMBean.class);

            return new JmxProxy(jmxConn, mbeanServerConn, ssProxy, rsProxy, cmProxy);

        }
        catch (IOException | MalformedObjectNameException e)
        {
            throw new RuntimeException("Failure during establishing JMX connection", e);
        }
    }

    public void addListener(final JmxFacade listener)
    {
        try
        {
            ObjectName ssObjectName = new ObjectName("org.apache.cassandra.db:type=StorageService");
            maybeRemoveListener(listener, ssObjectName);
            this.mbeanServerConn.addNotificationListener(ssObjectName, listener, null, null);

        }
        catch (MalformedObjectNameException | InstanceNotFoundException | IOException e)
        {
            throw new RuntimeException("Could not add JMX notification listener");
        }
    }

    private void maybeRemoveListener(final JmxFacade listener, final ObjectName ssObjectName)
    {
        try
        {
            this.mbeanServerConn.removeNotificationListener(ssObjectName, listener);
        }
        catch (ListenerNotFoundException | InstanceNotFoundException | IOException ignored)
        {
            // let's not worry for now
        }
    }

    public void close()
    {
        try
        {
            jmxConn.close();
        }
        catch (IOException e)
        {
            // well, sucks to be us
        }
    }

    public Map<List<String>, List<String>> getRangeToEndpointWithPortMap(final String keyspace)
    {
        return ssProxy.getRangeToEndpointWithPortMap(keyspace);
    }

    public int startRepair(
            final String keyspaceName,
            final String validationType,
            final Optional<List<Segment>> tokenRanges,
            final Boolean primaryRange,
            final boolean incremental,
            final boolean preview,
            final RtestCluster cluster)
    {

        String parallelismOption = validationType.equalsIgnoreCase("parallel") ? "parallel" : "sequential";

        Map<String, String> options = new HashMap<>();
        options.put(RepairOption.PARALLELISM_KEY, parallelismOption);
        options.put(RepairOption.INCREMENTAL_KEY, Boolean.valueOf(incremental).toString());
        options.put(RepairOption.PRIMARY_RANGE_KEY, primaryRange.toString());
        options.put(RepairOption.JOB_THREADS_KEY, "1");

        if (preview)
        {
            options.put(RepairOption.PREVIEW, PreviewKind.ALL.name());
        }

        if (tokenRanges.isPresent())
        {
            options.put(RepairOption.RANGES_KEY, Segment.commaSeparatedRanges(tokenRanges.get()));
        }
        LOG.debug("Starting repair on keyspace {} with options: {}", keyspaceName, options);
        return ssProxy.repairAsync(keyspaceName, options);
    }

    public List<RepairStats> getRepairStats(final String keyspace)
    {
        return getRepairStats(keyspace, "", "");
    }

    public List<RepairStats> getRepairStats(final String keyspace, final String st, final String et)
    {
        String rangeString = (st.isEmpty() || et.isEmpty())
                ? null
                : String.format("%s:%s", st, et);
        List<CompositeData> repairStats = rsProxy.getRepairStats(Lists.newArrayList(keyspace), rangeString);
        return repairStats.stream().map(RepairStats::fromComposite).collect(toList());
    }

    private List<Map<String, String>> getCompactions(final String keyspace, final String taskType)
    {
        return cmProxy.getCompactions()
                .stream()
                .filter(compactionMap -> compactionMap.getOrDefault("keyspace", "").equals(keyspace))
                .filter(compactionMap -> compactionMap.getOrDefault("taskType", "").equals(taskType))
                .collect(toList());
    }

    public boolean hasValidationHappening(final String keyspace)
    {
        return getCompactions(keyspace, "Validation").size() > 0;
    }

    public boolean hasCompactionHappening(final String keyspace)
    {
        return getCompactions(keyspace, "Compaction").size() > 0;
    }

    public void terminateRepair()
    {
        ssProxy.forceTerminateAllRepairSessions();
    }

    public boolean hasActiveRepair()
    {
        int activeValidation = getThreadCount("ActiveTasks", "ValidationExecutor");
        int pendingValidation = getThreadCount("PendingTasks", "ValidationExecutor");
        int activeRepair = getThreadCount("ActiveTasks", "AntiEntropyStage");
        int pendingRepair = getThreadCount("PendingTasks", "AntiEntropyStage");
        int activeRepairTask = getThreadCount("ActiveTasks", "Repair-Task");
        int pendingRepairTask = getThreadCount("PendingTasks", "Repair-Task");
        return activeRepair + pendingRepair
            + activeValidation + pendingValidation
            + pendingRepairTask + activeRepairTask > 0;
    }

    public boolean isUp()
    {
        int activeValidation = getThreadCount("ActiveTasks", "ValidationExecutor");
        return activeValidation >= 0;
    }

    private int getThreadCount(final String state, final String scope)
    {
        try
        {
            ObjectName threadMetric = new ObjectName(String.format(
                    "org.apache.cassandra.metrics:name=%s,path=internal,scope=%s,type=ThreadPools",
                    state, scope));
            return (int) mbeanServerConn.getAttribute(threadMetric, "Value");
        }
        catch (InstanceNotFoundException infe)
        {
            // this happens when we abort the repair after seeing Validations, but AntiEntropy threads have not yet
            // appeared
            // this situation is very likely when doing sequential validations
            assert scope.equals("AntiEntropyStage") || scope.equals("Repair-Task");
            return 0;
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error when fetching ThreadPool metrics", e);
        }
    }

    public long getBytesPendingRepair(final String keyspace, final String table)
    {
        try
        {
            ObjectName bytesPendingRepairMetricName = new ObjectName(String.format(
                    "org.apache.cassandra.metrics:keyspace=%s,name=BytesPendingRepair,scope=%s,type=Table",
                    keyspace, table));
            return (long) mbeanServerConn.getAttribute(bytesPendingRepairMetricName, "Value");
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error when fetching BytesPendingRepair metric", e);
        }
    }

    public boolean triggerMajorCompaction(final String keyspace)
    {
        boolean splitOutput = true;
        try
        {
            ssProxy.forceKeyspaceCompaction(splitOutput, keyspace);
            return true;
        }
        catch (IOException | ExecutionException | InterruptedException e)
        {
            return false;
        }
    }

}
