package rtest.cassandra;

import com.google.common.collect.Lists;

import org.apache.commons.lang3.tuple.Pair;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class CcmRtestCluster extends RtestCluster {

  private final String clusterName;
  // Token ranges per backup that we know to be out of sync
  private final Pair<BigInteger, BigInteger> firstHalfTokenRange;
  private final Pair<BigInteger, BigInteger> secondHalfTokenRange;
  private final Pair<Pair<BigInteger, BigInteger>, Pair<BigInteger, BigInteger>> twoReplicasShared;
  private final Pair<Pair<BigInteger, BigInteger>, Pair<BigInteger, BigInteger>> threeReplicasShared;

  public CcmRtestCluster(List<String> contactHosts, int port) {
    super(contactHosts, port);
    clusterName = "repair_qa";
    // Out of sync token range, split in two halves
    firstHalfTokenRange = Pair.of(new BigInteger("-9223372036854775808"), new BigInteger("-6148914691236517206"));
    secondHalfTokenRange = Pair.of(new BigInteger("-6148914691236517206"), new BigInteger("-3074457345618258603"));
    
    // Out of sync token ranges pair with the same 2 replica sets
    twoReplicasShared = Pair.of(Pair.of(new BigInteger("-3074457345618258603"), new BigInteger("-1")), Pair.of(new BigInteger("10"), new BigInteger("3074457345618258602")));

    // Out of sync token ranges pair with distinct replica sets
    threeReplicasShared = Pair.of(Pair.of(new BigInteger("-3074457345618258603"), new BigInteger("3074457345618258602")), Pair.of(new BigInteger("3074457345618258602"), new BigInteger("-9223372036854775808")));

  }

  @Override
  public void restoreInitialStateBackup() {
    runCmd("ccm stop");
    try {
      runCmd(String.format("ccm switch %s", clusterName));  
    } catch(RuntimeException e) {
      runCmd(String.format("ccm create %s -v github:apache/trunk -n 3", clusterName));
    }
    
    URL res = getClass().getClassLoader().getResource("ccm");
    File file;
    try {
      file = Paths.get(res.toURI()).toFile();
    } catch (URISyntaxException e) {
      e.printStackTrace();
      throw new RuntimeException("Couldn't find ccm backup file", e);
    }
    String absolutePath = file.getAbsolutePath();

    // Restore backup ccm cluster
    
    runCmd(String.format("setopt rm_star_silent; rm -Rf ~/.ccm/%s/*", clusterName));
    runCmd(String.format("cp -R %s/* ~/.ccm/%s/", absolutePath , clusterName));
    try {
      String ccmClusterPath = String.format("%s/.ccm/%s/", System.getProperty("user.home"), clusterName);
      String clusterConf = new String(Files.readAllBytes(Paths.get(ccmClusterPath + "cluster.conf")), StandardCharsets.UTF_8);
      String newClusterConf = clusterConf.replaceAll("~", System.getProperty("user.home"));
      Files.write(Paths.get(ccmClusterPath + "cluster.conf"), newClusterConf.getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      e.printStackTrace();
      throw new RuntimeException("Couldn't replace user home dir in ccm cluster.conf file.", e);
    }

    runCmd("ccm start");
  }


  @Override
  public boolean canRunShellCommands(String host) {
    return true;
  }

  @Override
  public boolean restoreBackup(String backupName) {
    restoreInitialStateBackup();
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

  @Override
  public Pair<BigInteger, BigInteger> getFirstHalfTokenRange() {
    return firstHalfTokenRange;
  }

  @Override
  public Pair<BigInteger, BigInteger> getSecondHalfTokenRange() {
    return secondHalfTokenRange;
  }

  @Override
  public Pair<Pair<BigInteger, BigInteger>, Pair<BigInteger, BigInteger>> getTwoReplicasSharedTokenRanges() {
    return twoReplicasShared;
  }

  @Override
  public Pair<Pair<BigInteger, BigInteger>, Pair<BigInteger, BigInteger>> getThreeReplicasSharedTokenRanges() {
    return threeReplicasShared;
  }
}
