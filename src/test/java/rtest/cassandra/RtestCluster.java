package rtest.cassandra;


import com.datastax.driver.core.*;

import java.net.InetAddress;
import java.util.List;

import static java.util.stream.Collectors.toList;

public abstract class RtestCluster {

  protected Cluster cluster;
  protected Session cqlSession;
  private String host;
  private int port;

  protected RtestCluster(String host, int port) {
    this.host = host;
    this.port = port;
    this.cluster = connect(host, port);
    this.cqlSession = cluster.newSession();
  }

  private Cluster connect(String host, int port) {
    return Cluster.builder()
        .addContactPoint(host)
        .withPort(port)
        .build();
  }

  public boolean isUp() {
    return this.cqlSession != null;
  }

  public Session getSession() {
    return this.cqlSession;
  }

  public List<String> getHosts() {
    return cluster.getMetadata().getAllHosts().stream()
        .map(Host::getBroadcastAddress)
        .map(InetAddress::toString)
        .map(s -> s.replace("/", ""))
        .collect(toList());
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

}
