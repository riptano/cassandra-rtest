package rtest.minireaper;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class RepairEvent {

  public static class RepairCompleted extends RepairEvent {
    public static final Pattern REPAIR_COMPLETE_PATTERN = Pattern
        .compile("Repair command #(\\d+) finished", Pattern.CASE_INSENSITIVE);
    public final int commandNumber;

    public RepairCompleted(String sourceMessage, String repairCommand) {
      super(sourceMessage);
      this.commandNumber = Integer.parseInt(repairCommand);
    }
  }

  public static class RepairSuccess extends RepairEvent {
    private static final Pattern REPAIR_SUCCESS_PATTERN = Pattern.compile("Repair completed successfully");

    public RepairSuccess(String sourceMessage) {
      super(sourceMessage);
    }
  }

  public static class PreviewSuccess extends RepairEvent {
    private static final Pattern PREVIEW_SUCCESS_PATTERN = Pattern.compile("Repair preview completed successfully");

    public PreviewSuccess(String sourceMessage) {
      super(sourceMessage);
    }
  }

  public static class PreviewDataIsInSync extends RepairEvent {
    private static final Pattern PREVIEW_DATA_IN_SYNC_PATTERN = Pattern.compile("Previewed data was in sync");

    protected PreviewDataIsInSync(String sourceMessage) {
      super(sourceMessage);
    }
  }

  public static class RepairSessionFinished extends RepairEvent {
    private static final Pattern REPAIR_SESSION_FINISHED = Pattern
        .compile("Repair session ([^ ]+) for range ([0-9\\[\\(\\]\\), -]+) finished");
    public final String sessionId;
    public final String ranges;

    protected RepairSessionFinished(String sourceMessage, String sessionId, String ranges) {
      super(sourceMessage);
      this.sessionId = sessionId;
      this.ranges = ranges;
    }
  }

  public static class RepairPreviewDetail extends RepairEvent {
    private static final Pattern REPAIR_PREVIEW_DETAIL = Pattern
        .compile("(/\\d+.\\d+.\\d+.\\d+:\\d+) -> (/\\d+.\\d+.\\d+.\\d+:\\d+): " +
            "\\d+ ranges, \\d+ sstables, [\\d[;,].\\w]+ bytes");

    public final String srcEndpoint;
    public final String dstEndpoint;

    protected RepairPreviewDetail(String sourceMessage, String srcEndpoint, String dstEndpoint) {
      super(sourceMessage);
      this.srcEndpoint = srcEndpoint;
      this.dstEndpoint = dstEndpoint;
    }
  }

  public static class Unknown extends RepairEvent {
    public Unknown(String sourceMessage) {
      super(sourceMessage);
    }
  }

  public final String sourceMessage;

  protected RepairEvent(String sourceMessage) {
    this.sourceMessage = sourceMessage;
    // Uncomment to debug repair/preview events
    // System.err.println("Repair event message: " + sourceMessage);
  }

  public static RepairEvent parseMessage(String message) {

    Matcher matcher = RepairCompleted.REPAIR_COMPLETE_PATTERN.matcher(message);
    if (matcher.find()) {
      String repairCommand = matcher.group(1);
      return new RepairCompleted(message, repairCommand);
    }

    matcher = RepairSuccess.REPAIR_SUCCESS_PATTERN.matcher(message);
    if (matcher.find()) {
      return new RepairSuccess(message);
    }

    matcher = PreviewSuccess.PREVIEW_SUCCESS_PATTERN.matcher(message);
    if (matcher.find()) {
      return new PreviewSuccess(message);
    }

    matcher = PreviewDataIsInSync.PREVIEW_DATA_IN_SYNC_PATTERN.matcher(message);
    if (matcher.find()) {
      return new PreviewDataIsInSync(message);
    }

    matcher = RepairSessionFinished.REPAIR_SESSION_FINISHED.matcher(message);
    if (matcher.find()) {
      String sessionId = matcher.group(1);
      String ranges = matcher.group(2);
      return new RepairSessionFinished(message, sessionId, ranges);
    }

    matcher = RepairPreviewDetail.REPAIR_PREVIEW_DETAIL.matcher(message);
    if (matcher.find()) {
      String src = matcher.group(1);
      String dst = matcher.group(2);
      return new RepairPreviewDetail(message, src, dst);
    }

    return new Unknown(message);
  }

}
