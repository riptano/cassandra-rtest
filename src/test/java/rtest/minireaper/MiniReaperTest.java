package rtest.minireaper;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

public class MiniReaperTest {

  @Test
  public void testCountMentionedEndpoints() {
    List<RepairEvent> events = ImmutableList.of(
        RepairEvent.parseMessage("    /127.0.0.3:7000 -> /127.0.0.1:7000: 14 ranges, 2 sstables, 9.473MiB bytes"),
        RepairEvent.parseMessage("    /127.0.0.1:7000 -> /127.0.0.3:7000: 14 ranges, 0 sstables, 0.000KiB bytes"))
        .stream()
        .map(event -> (RepairEvent.RepairPreviewDetail) event)
        .collect(toList());
    assertEquals(2, MiniReaper.countEndpointsInPreview(events.stream()));

    events = ImmutableList.of(
        RepairEvent.parseMessage("    /127.0.0.3:7000 -> /127.0.0.1:7000: 22 ranges, 9 sstables, 15.608MiB bytes"),
        RepairEvent.parseMessage("    /127.0.0.2:7000 -> /127.0.0.1:7000: 7 ranges, 2 sstables, 4.821MiB bytes"),
        RepairEvent.parseMessage("    /127.0.0.1:7000 -> /127.0.0.2:7000: 7 ranges, 0 sstables, 0.000KiB bytes"),
        RepairEvent.parseMessage("    /127.0.0.1:7000 -> /127.0.0.3:7000: 22 ranges, 0 sstables, 0.000KiB bytes"))
        .stream()
        .map(event -> (RepairEvent.RepairPreviewDetail) event)
        .collect(toList());
    assertEquals(3, MiniReaper.countEndpointsInPreview(events.stream()));
  }

}





