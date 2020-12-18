package rtest.cassandra;


import com.datastax.driver.core.AbstractTableMetadata;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.google.common.collect.Lists;

import java.util.List;

import static java.util.stream.Collectors.toList;

public abstract class RtestCluster {

  protected final String[] contactPoints;
  private final int contactPort;

  private Cluster cluster;
  private Session cqlSession;

  protected RtestCluster(List<String> hosts, int port) {
    this.contactPoints = hosts.toArray(new String[hosts.size()]);
    this.contactPort = port;
    connect();
  }

  protected void connect() {
    this.cluster = Cluster.builder()
        .addContactPoints(contactPoints)
        .withPort(contactPort)
        .build();
    this.cqlSession = cluster.newSession();
  }

  protected void reConnect() {
    int retries = 0;
    long sleepExponent = 2;
    long sleepMs = 1000;
    while (retries < 10)
    try {
      connect();
      // force the driver to connect internally by asking for a keyspace
      keyspaceIsPresent("system");
      return;
    } catch (Exception e) {
      try {Thread.sleep(sleepMs);} catch (InterruptedException ignored) {}
      sleepMs = 1000 * sleepExponent;
      sleepExponent *= 2;
      retries++;
    }

    throw new RuntimeException("Could not re-connect to the cluster.");
  }

  public boolean isUp() {
    return this.getSession() != null && !this.getSession().isClosed();
  }

  public Cluster getCluster() {
    if (this.cluster.isClosed()) {
      this.connect();
    }
    return this.cluster;
  }

  public Session getSession() {
    if (this.cqlSession.isClosed() || this.cluster.isClosed()) {
      this.connect();
    }
    return this.cqlSession;
  }

  public List<String> getContactPoints() {
    return Lists.newArrayList(this.contactPoints);
  }

  public int getNodeCount() {
    return this.getCluster().getMetadata().getAllHosts().size();
  }

  public boolean keyspaceIsPresent(String keyspaceName) {
      return cluster.getMetadata()
          .getKeyspaces()
          .stream()
          .anyMatch(ks -> ks.getName().equals(keyspaceName));
  }

  public List<String> getTableNamesIn(String keyspace) {
    return getCluster().getMetadata().getKeyspace(keyspace).getTables()
        .stream()
        .map(AbstractTableMetadata::getName)
        .collect(toList());
  }

  public void shutDown() {
    if (this.cqlSession != null) {
      this.cqlSession.close();
    }
    if (this.cluster != null) {
      this.cluster.close();
    }
  }

  public abstract void cleanUpLogs();

  public abstract boolean logsAreEmpty();

  public abstract void restoreInitialStateBackup();

  public abstract boolean canRunShellCommands(String host);

  public abstract boolean restoreBackup(String backupName);
}
