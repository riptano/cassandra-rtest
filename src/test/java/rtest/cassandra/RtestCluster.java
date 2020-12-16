package rtest.cassandra;


import com.datastax.driver.core.AbstractTableMetadata;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;

import java.util.List;

import static java.util.stream.Collectors.toList;

public abstract class RtestCluster {

  protected final List<String> contactPoints;
  protected Cluster cluster;
  protected Session cqlSession;

  protected RtestCluster(List<String> hosts, int port) {
    this.contactPoints = hosts;
    this.cluster = connect(hosts, port);
    this.cqlSession = cluster.newSession();
  }

  private Cluster connect(List<String> hosts, int port) {
    return Cluster.builder()
        .addContactPoints(hosts.toArray(new String[hosts.size()]))
        .withPort(port)
        .build();
  }

  public boolean isUp() {
    return this.cqlSession != null;
  }

  public Session getSession() {
    return this.cqlSession;
  }

  public List<String> getContactPoints() {
    return this.contactPoints;
  }

  public int getNodeCount() {
    return this.cluster.getMetadata().getAllHosts().size();
  }

  public boolean keyspaceIsPresent(String keyspaceName) {
    try {
      KeyspaceMetadata ksmd = cluster.getMetadata().getKeyspace(keyspaceName);
      return ksmd != null;
    } catch (Exception e) {
      return false;
    }
  }

  public List<String> getTableNamesIn(String keyspace) {
    return cluster.getMetadata().getKeyspace(keyspace).getTables()
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
