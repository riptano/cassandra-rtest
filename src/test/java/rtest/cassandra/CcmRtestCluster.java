package rtest.cassandra;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CcmRtestCluster extends RtestCluster
{
    private static final Logger LOG = LoggerFactory.getLogger(CcmRtestCluster.class);

    private final String clusterName;
    // Token ranges per backup that we know to be out of sync
    private final Pair<BigInteger, BigInteger> firstHalfTokenRange;
    private final Pair<BigInteger, BigInteger> secondHalfTokenRange;
    private final Pair<Pair<BigInteger, BigInteger>, Pair<BigInteger, BigInteger>> twoReplicasShared;
    private final Pair<Pair<BigInteger, BigInteger>, Pair<BigInteger, BigInteger>> threeReplicasShared;

    public CcmRtestCluster(final List<String> contactHosts, final int port)
    {
        super(contactHosts, port);
        clusterName = "repair_qa";
        // Out of sync token range, split in two halves
        firstHalfTokenRange = Pair.of(new BigInteger("-9223372036854775808"), new BigInteger("-6148914691236517206"));
        secondHalfTokenRange = Pair.of(new BigInteger("-6148914691236517206"), new BigInteger("-3074457345618258603"));

        // Out of sync token ranges pair with the same 2 replica sets
        twoReplicasShared = Pair.of(Pair.of(new BigInteger("-3074457345618258603"), new BigInteger("-1")),
                Pair.of(new BigInteger("10"), new BigInteger("3074457345618258602")));

        // Out of sync token ranges pair with distinct replica sets
        threeReplicasShared = Pair.of(
                Pair.of(new BigInteger("-3074457345618258603"), new BigInteger("3074457345618258602")),
                Pair.of(new BigInteger("3074457345618258602"), new BigInteger("-9223372036854775808")));

    }

    @Override
    public void connect()
    {
        restoreInitialStateBackup();
        super.connect();
    }

    @Override
    public void restoreInitialStateBackup()
    {
        final int numberOfCcmNodes = 3;
        runCmd("ccm stop", true);
        runCmd(String.format("ccm remove %s", clusterName), true);
        runCmd(String.format("ccm create %s -v github:apache/trunk -n 3", clusterName));

        URL res = getClass().getClassLoader().getResource("ccm");
        File file;
        try
        {
            file = Paths.get(res.toURI()).toFile();
        }
        catch (URISyntaxException e)
        {
            e.printStackTrace();
            throw new RuntimeException("Couldn't find ccm backup file", e);
        }
        String absolutePath = file.getAbsolutePath();

        // Restore backup ccm cluster

        runCmd(String.format("setopt rm_star_silent; rm -Rf ~/.ccm/%s/*", clusterName));
        runCmd(String.format("cp -R %s/* ~/.ccm/%s/", absolutePath, clusterName));
        try
        {
            String ccmClusterPath = String.format("%s/.ccm/%s/", System.getProperty("user.home"), clusterName);
            // Replace home dir in cluster.conf
            String clusterConf = new String(Files.readAllBytes(Paths.get(ccmClusterPath + "cluster.conf")),
                    StandardCharsets.UTF_8);
            String newClusterConf = clusterConf.replaceAll("~", System.getProperty("user.home"));
            Files.write(Paths.get(ccmClusterPath + "cluster.conf"), newClusterConf.getBytes(StandardCharsets.UTF_8));
            // Replace home dir in cassandra.yaml for all 3 nodes
            for (int i = 1; i <= numberOfCcmNodes; i++)
            {
                Path cassandraYamlPath = Paths.get(ccmClusterPath + "node" + i + "/conf/cassandra.yaml");
                String cassandraYaml
                    = new String(Files.readAllBytes(cassandraYamlPath),
                StandardCharsets.UTF_8);
                String newYaml = cassandraYaml.replaceAll("~", System.getProperty("user.home"));
                Files.write(cassandraYamlPath, newYaml.getBytes(StandardCharsets.UTF_8));
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
            throw new RuntimeException("Couldn't replace user home dir in ccm cluster.conf file.", e);
        }

        runCmd("ccm start");
    }

    @Override
    public boolean canRunShellCommands(final String host)
    {
        return true;
    }

    @Override
    public boolean restoreBackup(final String backupName)
    {
        restoreInitialStateBackup();
        return true;
    }

    private List<String> runCmd(final String cmd)
    {
        return runCmd(cmd, false);
    }

    private List<String> runCmd(final String cmd, final boolean ignoreFailure)
    {
        try
        {
            Process process = Runtime.getRuntime().exec(new String[]
            {
                    "/bin/bash", "-c", cmd
            });
            process.waitFor();
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            List<String> output = Lists.newArrayList();
            String line;
            while ((line = reader.readLine()) != null)
            {
                output.add(line);
            }

            if (process.exitValue() != 0 && !ignoreFailure)
            {
                LOG.error("Command {} failed with output: {}", cmd, StringUtils.join(output, " / "));
                throw new RuntimeException(String.format("Command '%s' failed", cmd));
            }

            return output;
        }
        catch (InterruptedException | IOException e)
        {
            if (ignoreFailure)
            {
                return Collections.emptyList();
            }
            throw new RuntimeException(String.format("Running shell command '%s' failed: %s", cmd, e.getMessage()));
        }
    }

    @Override
    public Pair<BigInteger, BigInteger> getFirstHalfTokenRange()
    {
        return firstHalfTokenRange;
    }

    @Override
    public Pair<BigInteger, BigInteger> getSecondHalfTokenRange()
    {
        return secondHalfTokenRange;
    }

    @Override
    public Pair<Pair<BigInteger, BigInteger>, Pair<BigInteger, BigInteger>> getTwoReplicasSharedTokenRanges()
    {
        return twoReplicasShared;
    }

    @Override
    public Pair<Pair<BigInteger, BigInteger>, Pair<BigInteger, BigInteger>> getThreeReplicasSharedTokenRanges()
    {
        return threeReplicasShared;
    }
}
