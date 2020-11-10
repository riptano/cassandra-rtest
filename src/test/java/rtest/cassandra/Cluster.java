package rtest.cassandra;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.connection.ConnectionInitException;
import com.datastax.oss.driver.api.core.metadata.Node;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;

import static java.util.stream.Collectors.toList;

public abstract class Cluster {

  protected CqlSession cqlSession;
  private String host;
  private int port;
  private String dcName;

  protected Cluster(String host, int port, String dcName) {
    this.host = host;
    this.port = port;
    this.dcName = dcName;
    CqlSession session;
    session = connect(host, port, dcName);
    this.cqlSession = session;
  }

  private CqlSession connect(String host, int port, String dcName) {
    CqlSession session;
    try {
      session = CqlSession.builder()
          .addContactPoint(new InetSocketAddress(host, port))
          .withLocalDatacenter(dcName)
          .build();
    } catch (ConnectionInitException e) {
      session = null;
    }
    return session;
  }

  public void reconnect() {
    this.cqlSession = connect(host, port, dcName);
  }

  public boolean isUp() {
    return this.cqlSession != null;
  }

  public CqlSession getSession() {
    return this.cqlSession;
  }

  public List<String> getHosts() {
    Collection<Node> nodes = cqlSession.getMetadata().getNodes().values();
    return nodes.stream().map(node -> node.getBroadcastAddress().get().getAddress().getHostAddress()).collect(toList());
  }

  public List<String> getTableNamesIn(String keyspace) {
    return cqlSession.getMetadata().getKeyspace(keyspace).get().getTables().keySet()
        .stream()
        .map(CqlIdentifier::toString)
        .collect(toList());
  }

  public void shutDown() {
    this.cqlSession.close();
  }

  public abstract void cleanUpLogs();

  public abstract boolean logsAreEmpty();

  public abstract void restoreInitialStateBackup();

}
