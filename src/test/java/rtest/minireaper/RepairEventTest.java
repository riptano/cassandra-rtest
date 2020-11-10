package rtest.minireaper;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RepairEventTest {

  @Test
  public void testParseRepairSessionMessage() {
    String message = "Repair session 4562b4e0-3302-11eb-a1c4-65f1afb6b689 for range " +
        "[(1229782938247303458,1434746761288520698], " +
        "(409927646082434494,614891469123651734], " +
        "(-614891469123651711,-409927646082434471], " +
        "(12,204963823041217252], " +
        "(614891469123651735,819855292164868975], " +
        "(-1434746761288520675,-1229782938247303435], " +
        "(819855292164868976,1024819115206086216], " +
        "(-409927646082434470,-204963823041217230], " +
        "(204963823041217253,409927646082434493], " +
        "(-819855292164868952,-614891469123651712], " +
        "(1024819115206086217,1229782938247303457], " +
        "(-1229782938247303434,-1024819115206086194]," +
        "(-1024819115206086193,-819855292164868953], " +
        "(-204963823041217229,11]] finished";

    RepairEvent event = RepairEvent.parseMessage(message);
    assertTrue(event instanceof RepairEvent.RepairSessionFinished);

    RepairEvent.RepairSessionFinished sessionFinishedEvent = (RepairEvent.RepairSessionFinished) event;
    assertEquals(
        "4562b4e0-3302-11eb-a1c4-65f1afb6b689",
        sessionFinishedEvent.sessionId
    );
  }

  @Test
  public void testParseRepairPreviewDetail() {
    String message = "    /127.0.0.3:7000 -> /127.0.0.1:7000: 14 ranges, 2 sstables, 9.473MiB bytes";
    RepairEvent event = RepairEvent.parseMessage(message);
    assertTrue(event instanceof RepairEvent.RepairPreviewDetail);

    RepairEvent.RepairPreviewDetail detailEvent = (RepairEvent.RepairPreviewDetail) event;
    assertEquals("/127.0.0.3:7000", detailEvent.srcEndpoint);
    assertEquals("/127.0.0.1:7000", detailEvent.dstEndpoint);
  }

}
