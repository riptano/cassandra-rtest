package rtest.cassandra;

import com.jcraft.jsch.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class AwsRtestCluster extends RtestCluster {

  private static final String SSH_KEY_PATH = "/home/runner/.tlp-cluster/profiles/default/secret.pem";

  // in GHA the user is 'runner', but for ssh we need the one from tlp-cluster, which is 'ubuntu'
  private static final String sshUser = "ubuntu";

  private final JSch jsch = new JSch();
  private final UserInfo jschUserInfo = new AuthKeyUserInfo();

  private final Map<String, Session> sshSessions;

  public AwsRtestCluster(List<String> contactHosts, int port) {
    super(contactHosts, port);
    try {
      jsch.addIdentity(SSH_KEY_PATH);
    } catch (JSchException e) {
      throw new RuntimeException("Could not load SSH identity file", e);
    }

    try {
      jsch.setKnownHosts("/dev/null");
    } catch (JSchException e) {
      throw new RuntimeException("Could not set KnownHosts file", e);
    }

    this.sshSessions = openSessions(contactHosts);
  }

  private Map<String, Session> openSessions(List<String> hosts) {
    return hosts.stream().collect(toMap(
        host -> host,
        this::openSession
    ));
  }

  private Session openSession(String host) {
    try {
      Session session = this.jsch.getSession(this.sshUser, host, 22);
      session.setConfig("HashKnownHosts",  "no");
      session.setConfig("StrictHostKeyChecking", "no");
      session.setConfig("PreferredAuthentications", "publickey");
      session.setUserInfo(jschUserInfo);
      session.connect();
      return session;
    } catch (JSchException e) {
      throw new RuntimeException(String.format(
          "Could not open SSH session with %s: %s", host, e.getMessage()
      ));
    }
  }

  @Override
  public void shutDown() {
    super.shutDown();
    sshSessions.values().forEach(Session::disconnect);
  }

  @Override
  public void cleanUpLogs() {

  }

  @Override
  public boolean logsAreEmpty() {
    return false;
  }

  @Override
  public void restoreInitialStateBackup() {

  }

  @Override
  public boolean canRunShellCommands(String host) {
    int rc = runCommand(host, "whoami");
    return rc == 0;
  }

  @Override
  public boolean restoreBackup(String backupName) {
    String alwaysTheSameHost = getContactPoints()
        .stream()
        .sorted(String::compareTo)
        .collect(toList())
        .get(1);
    String tempDir = "/var/lib/cassandra/";
    String cmd = String.format(
        "medusa -v restore-cluster --backup-name %s --bypass-checks --temp-dir %s --verify", backupName, tempDir
    );

    int backupRestoreCommandRc = runCommand(alwaysTheSameHost, cmd);

    runCommand(alwaysTheSameHost, "echo \"DESCRIBE KEYSPACES;\" | cqlsh $(hostname) 9042");

    connect();
    return backupRestoreCommandRc == 0;
  }

  private int runCommand(String host, String command) {
    int exitStatus;
    Session session = sshSessions.get(host);
    try {
      ChannelExec channel = (ChannelExec) session.openChannel("exec");
      channel.setAgentForwarding(true);
      channel.setCommand(command);
      channel.setInputStream(null);
      channel.setErrStream(System.err);

      channel.connect();

      String stdout = readStream(channel, channel.getInputStream());
      String stderr = readStream(channel, channel.getErrStream());
      exitStatus = channel.getExitStatus();

      System.out.printf("Command '%s' completed with: %d%n", command, exitStatus);
      System.out.printf("Output was: %s", stdout);
      System.out.printf("Error was: %s", stderr);

      channel.disconnect();
    } catch (JSchException | IOException e) {
      throw new RuntimeException(String.format(
          "Running command '%s' on host '%s' failed: %S",
          command, host, e.getMessage()));
    }

    return exitStatus;
  }

  private String readStream(ChannelExec channel, InputStream s) {
    try {
      String output = "";
      byte[] tmp = new byte[1024];
      while (true) {
        while (s.available() > 0) {
          int i = s.read(tmp, 0, 1024);
          if (i < 0) break;
          output = new String(tmp, 0, i);
        }
        if (channel.isClosed()) {
          break;
        }
        try {
          Thread.sleep(1000);
        } catch (Exception ignored) {
        }
      }
      return output;
    } catch (IOException e) {
      throw new RuntimeException(String.format("Reading output failed: %s", e.getMessage()), e);
    }
  }

  private static class AuthKeyUserInfo implements UserInfo {

    @Override
    public String getPassphrase() {
      return null;
    }

    @Override
    public String getPassword() {
      return null;
    }

    @Override
    public boolean promptPassword(String s) {
      return false;
    }

    @Override
    public boolean promptPassphrase(String s) {
      return false;
    }

    @Override
    public boolean promptYesNo(String s) {
      return false;
    }

    @Override
    public void showMessage(String s) {

    }
  }
}
