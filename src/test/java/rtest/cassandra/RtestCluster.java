package rtest.cassandra;

import java.math.BigInteger;
import java.util.List;

import com.datastax.driver.core.AbstractTableMetadata;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toList;

public abstract class RtestCluster
{

    private static final Logger LOG = LoggerFactory.getLogger(RtestCluster.class);
    private static final int SLEEP_STEP_IN_MILLIS = 100;
    private static final boolean DISPLAY_WAIT_MESSAGE = false;

    private final String[] contactPoints;
    private final int contactPort;

    private Cluster cluster;
    private Session cqlSession;

    protected RtestCluster(final List<String> hosts, final int port)
    {
        this.contactPoints = hosts.toArray(new String[hosts.size()]);
        this.contactPort = port;
    }

    /**
     *  Connect to the cluster through the CQL port.
     */
    public void connect()
    {
        this.cluster = Cluster.builder()
                .addContactPoints(contactPoints)
                .withPort(contactPort)
                .build();
        this.cqlSession = cluster.newSession();
    }

    /**
     * Reconnects to the cluster through the CQL port with exponential retries.
     *
     * @param retries
     *            Number of retries
     * @throws InterruptedException
     */
    protected void reConnect(final int retries) throws InterruptedException
    {
        int attempts = 0;
        final int initialSleepTimeInMillis = 1000;
        if (!this.cluster.isClosed())
        {
            this.cluster.close();
        }
        long sleepMs = initialSleepTimeInMillis;
        while (attempts < retries)
        {
            try
            {
                connect();
                // force the driver to connect internally by asking for a keyspace
                keyspaceIsPresent("system");
                return;
            }
            catch (RuntimeException e)
            {
                LOG.debug("Failed connecting to the cluster...", e);
                Thread.sleep(sleepMs);
                sleepMs *= attempts;
                ++attempts;
            }
        }

        throw new RuntimeException("Could not re-connect to the cluster.");
    }

    public final boolean isUp()
    {
        return this.getSession() != null && !this.getSession().isClosed();
    }

    public final Cluster getCluster()
    {
        if (this.cluster.isClosed())
        {
            this.connect();
        }
        return this.cluster;
    }

    public final Session getSession()
    {
        if (this.cqlSession.isClosed() || this.cluster.isClosed())
        {
            this.connect();
        }
        return this.cqlSession;
    }

    public final List<String> getContactPoints()
    {
        return Lists.newArrayList(this.contactPoints);
    }

    public final int getNodeCount()
    {
        return this.getCluster().getMetadata().getAllHosts().size();
    }

    public final boolean keyspaceIsPresent(final String keyspaceName)
    {
        return cluster.getMetadata()
                .getKeyspaces()
                .stream()
                .anyMatch(ks -> ks.getName().equals(keyspaceName));
    }

    public final List<String> getTableNamesIn(final String keyspace)
    {
        return getCluster().getMetadata().getKeyspace(keyspace).getTables()
                .stream()
                .map(AbstractTableMetadata::getName)
                .collect(toList());
    }

    public final void shutDown()
    {
        if (this.cqlSession != null)
        {
            this.cqlSession.close();
        }
        if (this.cluster != null)
        {
            this.cluster.close();
        }
    }

    /**
     * @return Whether or not to display a wait message in the std err
     */
    public boolean displayWaitMessage()
    {
        return DISPLAY_WAIT_MESSAGE;
    }

    /**
     * @return The sleep time between checks when waiting for conditions
     */
    public int getSleepTimeBetweenChecks()
    {
        return SLEEP_STEP_IN_MILLIS;
    }

    public abstract void restoreInitialStateBackup();

    public abstract boolean canRunShellCommands(String host);

    public abstract boolean restoreBackup(String backupName) throws InterruptedException;

    public abstract Pair<BigInteger, BigInteger> getFirstHalfTokenRange();

    public abstract Pair<BigInteger, BigInteger> getSecondHalfTokenRange();

    public abstract Pair<Pair<BigInteger, BigInteger>, Pair<BigInteger, BigInteger>> getTwoReplicasSharedTokenRanges();

    public abstract Pair<Pair<BigInteger, BigInteger>, Pair<BigInteger, BigInteger>>
            getThreeReplicasSharedTokenRanges();
}
