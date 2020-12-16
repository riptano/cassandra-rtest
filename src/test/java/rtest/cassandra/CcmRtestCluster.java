package rtest.cassandra;

import com.google.common.collect.Lists;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

public class CcmRtestCluster extends RtestCluster {

  private final String clusterName;

  public CcmRtestCluster(List<String> contactHosts, int port) {
    super(contactHosts, port);
    clusterName = runCmd("cat ~/.ccm/CURRENT").get(0);
  }

  @Override
  public void cleanUpLogs() {
    for (int i = 1; i <= super.getContactPoints().size(); i++) {
      String cmd = String.format("echo -n > ~/.ccm/%s/node%d/logs/system.log", clusterName, i);
      runCmd(cmd);
    }
  }

  @Override
  public boolean logsAreEmpty() {
    int logSizeSum = 0;
    for (int i = 1; i <= super.getContactPoints().size(); i++) {
      String cmd = String.format(
          "ls -l %s/.ccm/%s/node%d/logs/system.log | awk '{print $5}'",
          System.getProperty("user.home"), clusterName, i
      );
      int logFileSize = Integer.parseInt(runCmd(cmd).get(0));
      logSizeSum += logFileSize;
    }
    return logSizeSum == 0;
  }

  @Override
  public void restoreInitialStateBackup() {
    getSession().execute("TRUNCATE TABLE system_distributed.parent_repair_history");
    getSession().execute("TRUNCATE TABLE system_distributed.repair_history");
    getSession().execute("TRUNCATE TABLE system.repairs");

    getSession().close();

    runCmd("ccm stop");

    runCmd(String.format("rm -r ~/.ccm/%s/node1/data0/repair_quality", clusterName));
    runCmd(String.format("cp -r ~/.ccm/%s-backup/node1/data0/repair_quality ~/.ccm/%s/node1/data0/repair_quality", clusterName, clusterName));
    runCmd(String.format("rm -r ~/.ccm/%s/node1/data0/repair_quality_rf2", clusterName));
    // runCmd(String.format("cp -r ~/.ccm/%s-backup/node1/data0/repair_quality_rf2 ~/.ccm/%s/node1/data0/repair_quality_rf2", clusterName, clusterName));

    String sstableRepairedSet = "/Users/zvo/.ccm/repository/gitCOLONcassandra-4.0-beta3/tools/bin/sstablerepairedset";
    String findCommand = String.format("find ~/.ccm/%s | grep Data | grep repair_quality | grep -v snapshot", clusterName);
    runCmd(String.format(
        "%s --really-set --is-unrepaired $(%s)",
        sstableRepairedSet, findCommand
    ));

    runCmd("ccm start");

    runCmd(String.format("ccm node2 nodetool compact"));
    runCmd(String.format("ccm node3 nodetool compact"));

    runCmd("ccm node1 cqlsh -e \"TRUNCATE TABLE system.compaction_history;\"");
    runCmd("ccm node2 cqlsh -e \"TRUNCATE TABLE system.compaction_history;\"");
    runCmd("ccm node3 cqlsh -e \"TRUNCATE TABLE system.compaction_history;\"");

    runCmd("ccm node3 cqlsh -e \"TRUNCATE TABLE system.repairs;\"");
    runCmd("ccm node3 cqlsh -e \"TRUNCATE TABLE system.repairs;\"");
    runCmd("ccm node3 cqlsh -e \"TRUNCATE TABLE system.repairs;\"");
  }

  @Override
  public boolean canRunShellCommands(String host) {
    return true;
  }

  @Override
  public boolean restoreBackup(String backupName) {
    return true;
  }

  private List<String> runCmd(String cmd) {
    try {
      Process process = Runtime.getRuntime().exec(new String[]{"/bin/bash", "-c", cmd});
      process.waitFor();
      BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      List<String> output = Lists.newArrayList();
      String line;
      while ((line = reader.readLine()) != null) {
        output.add(line);
      }

      if (process.exitValue() !=0) {
        throw new RuntimeException(String.format("Command '%s' failed", cmd));
      }

      return output;
    } catch (InterruptedException | IOException e) {
      throw new RuntimeException(String.format("Running shell command '%s' failed: %s", cmd, e.getMessage()));
    }
  }

}
