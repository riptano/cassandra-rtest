package rtest.cassandra;

import com.jcraft.jsch.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

public class AwsRtestCluster extends RtestCluster {

  private static final String SSH_KEY_PATH = "/home/runner/.tlp-cluster/profiles/default/secret.pem";
  private final String sshUser = System.getProperty("user.name");
  private final JSch jsch = new JSch();
  private final UserInfo jschUserInfo = new AuthKeyUserInfo();

  private final Map<String, Session> sshSessions;

  public AwsRtestCluster(String host, int port) {
    super(host, port);

    try {
      jsch.addIdentity(SSH_KEY_PATH);
    } catch (JSchException e) {
      throw new RuntimeException("Could not load SSH identity file", e);
    }

    this.sshSessions = openSessions();
  }

  private Map<String, Session> openSessions() {
    return super.getHosts().stream()
        .collect(toMap(
            host -> host,
            this::openSession
        ));
  }

  private Session openSession(String host) {
    try {
      Session session = this.jsch.getSession(this.sshUser, host, 22);
      session.setUserInfo(jschUserInfo);
      session.connect();
      return session;
    } catch (JSchException e) {
      throw new RuntimeException(String.format(
          "Could not open SSH session with %s", host
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

  private int runCommand(String host, String command) {
    int exitStatus;
    String commandOutput = "";
    Session session = sshSessions.get(host);
    try {
      ChannelExec channel = (ChannelExec) session.openChannel("exec");
      channel.setCommand(command);
      channel.setInputStream(null);
      channel.setErrStream(System.err);
      InputStream in = channel.getInputStream();


      byte[] tmp = new byte[1024];
      while (true) {
        while (in.available() > 0) {
          int i = in.read(tmp, 0, 1024);
          if (i < 0) break;
          commandOutput = new String(tmp, 0, i);
        }
        if (channel.isClosed()) {
          exitStatus = channel.getExitStatus();
          break;
        }
        try {Thread.sleep(1000);} catch (Exception ignored) {}
      }
      channel.disconnect();
    } catch (JSchException | IOException e) {
      throw new RuntimeException(String.format(
          "Running command '%s' on host '%s' failed: %S",
          command, host, e.getMessage()));
    }

    System.out.printf("Command: %s | Output: %s", command, commandOutput);
    return exitStatus;
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
