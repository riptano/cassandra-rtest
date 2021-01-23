package rtest.cassandra;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Collectors;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class AwsRtestCluster extends RtestCluster
{

    private static final String SSH_KEY_PATH = "~/.tlp-cluster/profiles/default/secret.pem".replaceFirst("^~",
            System.getProperty("user.home"));
    private static final Logger LOG = LoggerFactory.getLogger(AwsRtestCluster.class);
    private static final int SLEEP_STEP_IN_MILLIS = 30000;
    private static final boolean DISPLAY_WAIT_MESSAGE = true;
    // in GHA the user is 'runner', but for ssh we need the one from tlp-cluster, which is 'ubuntu'
    private static final String SSH_USER = "ubuntu";
    private static final int SSH_PORT = 22;
    private static final int TIMEOUT_NODES_UP_AFTER_RESTORE_IN_MINS = 5;

    private final JSch jsch;
    private final UserInfo jschUserInfo = new AuthKeyUserInfo();
    // Token ranges per backup that we know to be out of sync
    private final Pair<BigInteger, BigInteger> firstHalfTokenRange;
    private final Pair<BigInteger, BigInteger> secondHalfTokenRange;
    private final Pair<Pair<BigInteger, BigInteger>, Pair<BigInteger, BigInteger>> twoReplicasShared;
    private final Pair<Pair<BigInteger, BigInteger>, Pair<BigInteger, BigInteger>> threeReplicasShared;

    public AwsRtestCluster(final List<String> contactHosts, final int port)
    {
        super(contactHosts, port);
        try
        {
            jsch = new JSch();
            jsch.addIdentity(SSH_KEY_PATH);
            // Out of sync token range, split in two halves
            firstHalfTokenRange = Pair.of(new BigInteger("7833785626871182443"), new BigInteger("7862504351480351809"));
            secondHalfTokenRange = Pair.of(new BigInteger("7862504351480351809"),
                    new BigInteger("7891223076089521175"));

            // Out of sync token ranges pair with the same 2 replica sets
            twoReplicasShared = Pair.of(
                    Pair.of(new BigInteger("5169796967849597012"), new BigInteger("5177342124992655929")),
                    Pair.of(new BigInteger("-4020532397543894235"), new BigInteger("-3996618720982477278")));

            // Out of sync token ranges pair with distinct replica sets
            threeReplicasShared = Pair.of(
                    Pair.of(new BigInteger("5169796967849597012"), new BigInteger("5177342124992655929")),
                    Pair.of(new BigInteger("-1137006695544464376"), new BigInteger("-1117480221842164055")));
        }
        catch (JSchException e)
        {
            throw new RuntimeException("Could not load SSH identity file", e);
        }

        try
        {
            jsch.setKnownHosts("/dev/null");
        }
        catch (JSchException e)
        {
            throw new RuntimeException("Could not set KnownHosts file", e);
        }
    }

    private Session openSession(final String host)
    {
        try
        {
            Session session = this.jsch.getSession(SSH_USER, host, SSH_PORT);
            session.setConfig("HashKnownHosts", "no");
            session.setConfig("StrictHostKeyChecking", "no");
            session.setConfig("PreferredAuthentications", "publickey");
            session.setUserInfo(jschUserInfo);
            session.connect();
            return session;
        }
        catch (JSchException e)
        {
            throw new RuntimeException(String.format(
                    "Could not open SSH session with %s: %s", host, e.getMessage()));
        }
    }

    @Override
    public void restoreInitialStateBackup()
    {

    }

    @Override
    public boolean canRunShellCommands(final String host)
    {
        int rc = runCommand(host, "whoami").getExitStatus();
        return rc == 0;
    }

    @Override
    public boolean restoreBackup(final String backupName) throws InterruptedException
    {
        String alwaysTheSameHost = getContactPoints()
                .stream()
                .sorted(String::compareTo)
                .collect(Collectors.toList())
                .get(1);
        String tempDir = "/var/lib/cassandra/";
        String cmd = String.format(
                "medusa -v restore-cluster --backup-name %s --bypass-checks --temp-dir %s >> medusa_restore.log 2>&1",
                backupName, tempDir);
        shutDown();
        CommandResult commandResult = runCommand(alwaysTheSameHost, cmd);
        if (commandResult.getExitStatus() != 0)
        {
            LOG.error("Command exited with code: {}", commandResult.getExitStatus());
            // Restore failed or didn't complete before timeout. Writing logs in the output for diagnosis purposes.
            displayMedusaLogs(backupName, alwaysTheSameHost, tempDir);
            return false;
        }
        else
        {
            Instant startTime = Instant.now();
            // re-connect the cql session after the restore
            while (startTime.plus(TIMEOUT_NODES_UP_AFTER_RESTORE_IN_MINS, ChronoUnit.MINUTES)
                    .compareTo(Instant.now()) > 0)
            {
                try
                {
                    reConnect(2);
                    if (this.getCluster().getMetadata().getAllHosts().stream().filter(host -> !host.isUp())
                            .count() == 0)
                    {
                        return true;
                    }
                    else
                    {
                        throw new RuntimeException("Some hosts are still down.");
                    }
                }
                catch (RuntimeException e)
                {
                    LOG.error("Cluster restore is still running...");
                    Thread.sleep(SLEEP_STEP_IN_MILLIS);
                }
            }
            displayMedusaLogs(backupName, alwaysTheSameHost, tempDir);
            return false;
        }
    }

    private void displayMedusaLogs(final String backupName, final String alwaysTheSameHost, final String tempDir)
    {
        String logCmd = String.format(
                "cat medusa_restore.log && cat /var/lib/cassandra/medusa-job-*/stderr", backupName, tempDir);
        openSession(alwaysTheSameHost);
        runCommand(alwaysTheSameHost, logCmd);
    }

    public static final class CommandResult
    {
        private final int exitStatus;
        private final String stdout;
        private final String stderr;

        public CommandResult(final int rc, final String out, final String err)
        {
            this.exitStatus = rc;
            this.stdout = out;
            this.stderr = err;
        }

        public int getExitStatus()
        {
            return exitStatus;
        }

        public String getStdout()
        {
            return stdout;
        }

        public String getStderr()
        {
            return stderr;
        }
    }

    private CommandResult runCommand(final String host, final String command)
    {
        int exitStatus;
        String stdout = "";
        String stderr = "";
        Session session = null;
        try
        {
            session = openSession(host);
            ChannelExec channel = (ChannelExec) session.openChannel("exec");
            channel.setAgentForwarding(true);
            channel.setCommand(command);
            channel.setInputStream(null);
            channel.setErrStream(System.err);

            channel.connect();

            stdout = readStream(channel, channel.getInputStream());
            stderr = readStream(channel, channel.getErrStream());
            exitStatus = channel.getExitStatus();

            channel.disconnect();
        }
        catch (JSchException | IOException e)
        {
            throw new RuntimeException(String.format(
                    "Running command '%s' on host '%s' failed: %s\nStdout was:\n%s\nStderr was:\n%s\n",
                    command, host, e.getMessage(), stdout, stderr));
        }
        finally
        {
            if (session != null)
            {
                session.disconnect();
            }
        }

        return new CommandResult(exitStatus, stdout, stderr);
    }

    private String readStream(final ChannelExec channel, final InputStream s)
    {
        final int oneMeg = 1024;
        final int sleepTimeInMillis = 1000;
        try
        {
            String output = "";
            byte[] tmp = new byte[oneMeg];
            while (true)
            {
                while (s.available() > 0)
                {
                    int i = s.read(tmp, 0, oneMeg);
                    if (i < 0)
                    {
                        break;
                    }
                    output = new String(tmp, 0, i);
                }
                if (channel.isClosed())
                {
                    break;
                }
                try
                {
                    Thread.sleep(sleepTimeInMillis);
                }
                catch (Exception ignored)
                {
                }
            }
            return output;
        }
        catch (IOException e)
        {
            throw new RuntimeException(String.format("Reading output failed: %s", e.getMessage()), e);
        }
    }

    private static final class AuthKeyUserInfo implements UserInfo
    {

        @Override
        public String getPassphrase()
        {
            return null;
        }

        @Override
        public String getPassword()
        {
            return null;
        }

        @Override
        public boolean promptPassword(final String s)
        {
            return false;
        }

        @Override
        public boolean promptPassphrase(final String s)
        {
            return false;
        }

        @Override
        public boolean promptYesNo(final String s)
        {
            return false;
        }

        @Override
        public void showMessage(final String s)
        {

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

    @Override
    public boolean displayWaitMessage()
    {
        return DISPLAY_WAIT_MESSAGE;
    }

    @Override
    public int getSleepTimeBetweenChecks()
    {
        return SLEEP_STEP_IN_MILLIS;
    }
}
