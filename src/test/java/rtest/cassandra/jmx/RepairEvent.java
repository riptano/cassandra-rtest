package rtest.cassandra.jmx;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class RepairEvent
{

    private final String sourceMessage;

    public final String getSourceMessage()
    {
        return sourceMessage;
    }

    public static final class RepairCompleted extends RepairEvent
    {
        public static final Pattern REPAIR_COMPLETE_PATTERN = Pattern
                .compile("Repair command #(\\d+) finished", Pattern.CASE_INSENSITIVE);
        private final int commandNumber;

        public RepairCompleted(final String sourceMessage, final String repairCommand)
        {
            super(sourceMessage);
            this.commandNumber = Integer.parseInt(repairCommand);
        }

        public int getCommandNumber()
        {
            return commandNumber;
        }
    }

    public static final class RepairSuccess extends RepairEvent
    {
        private static final Pattern REPAIR_SUCCESS_PATTERN = Pattern.compile("Repair completed successfully");

        public RepairSuccess(final String sourceMessage)
        {
            super(sourceMessage);
        }
    }

    public static final class PreviewSuccess extends RepairEvent
    {
        private static final Pattern PREVIEW_SUCCESS_PATTERN = Pattern.compile("Repair preview completed successfully");

        public PreviewSuccess(final String sourceMessage)
        {
            super(sourceMessage);
        }
    }

    public static final class PreviewDataIsInSync extends RepairEvent
    {
        private static final Pattern PREVIEW_DATA_IN_SYNC_PATTERN = Pattern.compile("Previewed data was in sync");

        protected PreviewDataIsInSync(final String sourceMessage)
        {
            super(sourceMessage);
        }
    }

    public static final class RepairSessionFinished extends RepairEvent
    {
        private static final Pattern REPAIR_SESSION_FINISHED = Pattern
                .compile("Repair session ([^ ]+) for range ([0-9\\[\\(\\]\\), -]+) finished");
        private final String sessionId;
        private final String ranges;

        protected RepairSessionFinished(final String sourceMessage, final String sessionId, final String ranges)
        {
            super(sourceMessage);
            this.sessionId = sessionId;
            this.ranges = ranges;
        }

        public String getSessionId()
        {
            return sessionId;
        }

        public String getRanges()
        {
            return ranges;
        }
    }

    public static final class RepairPreviewDetail extends RepairEvent
    {
        private static final Pattern REPAIR_PREVIEW_DETAIL = Pattern
                .compile("(/\\d+.\\d+.\\d+.\\d+:\\d+) -> (/\\d+.\\d+.\\d+.\\d+:\\d+): "
                        + "\\d+ ranges, \\d+ sstables, [\\d[;,].\\w]+ bytes");

        private final String srcEndpoint;
        private final String dstEndpoint;

        protected RepairPreviewDetail(final String sourceMessage, final String srcEndpoint, final String dstEndpoint)
        {
            super(sourceMessage);
            this.srcEndpoint = srcEndpoint;
            this.dstEndpoint = dstEndpoint;
        }

        public String getSrcEndpoint()
        {
            return srcEndpoint;
        }

        public String getDstEndpoint()
        {
            return dstEndpoint;
        }
    }

    public static final class Unknown extends RepairEvent
    {
        public Unknown(final String sourceMessage)
        {
            super(sourceMessage);
        }
    }

    protected RepairEvent(final String sourceMessage)
    {
        this.sourceMessage = sourceMessage;
        // Uncomment to debug repair/preview events
        // System.err.println("Repair event message: " + sourceMessage);
    }

    public static RepairEvent parseMessage(final String message)
    {

        Matcher matcher = RepairCompleted.REPAIR_COMPLETE_PATTERN.matcher(message);
        if (matcher.find())
        {
            String repairCommand = matcher.group(1);
            return new RepairCompleted(message, repairCommand);
        }

        matcher = RepairSuccess.REPAIR_SUCCESS_PATTERN.matcher(message);
        if (matcher.find())
        {
            return new RepairSuccess(message);
        }

        matcher = PreviewSuccess.PREVIEW_SUCCESS_PATTERN.matcher(message);
        if (matcher.find())
        {
            return new PreviewSuccess(message);
        }

        matcher = PreviewDataIsInSync.PREVIEW_DATA_IN_SYNC_PATTERN.matcher(message);
        if (matcher.find())
        {
            return new PreviewDataIsInSync(message);
        }

        matcher = RepairSessionFinished.REPAIR_SESSION_FINISHED.matcher(message);
        if (matcher.find())
        {
            String sessionId = matcher.group(1);
            String ranges = matcher.group(2);
            return new RepairSessionFinished(message, sessionId, ranges);
        }

        matcher = RepairPreviewDetail.REPAIR_PREVIEW_DETAIL.matcher(message);
        if (matcher.find())
        {
            String src = matcher.group(1);
            String dst = matcher.group(2);
            return new RepairPreviewDetail(message, src, dst);
        }

        return new Unknown(message);
    }

}
