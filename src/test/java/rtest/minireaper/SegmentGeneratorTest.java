package rtest.minireaper;


import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.math.BigInteger;
import java.util.List;

import static org.junit.Assert.*;

public class SegmentGeneratorTest {

  @Test
  public void testStepSize() {
    BigInteger step = SegmentGenerator.calculateStep(
        SegmentGenerator.sortRingTokens(ImmutableMap.of(
            "1000", "127.0.0.1:7000",
            "2000", "127.0.0.2:7000",
            "3000", "127.0.0.3:7000"
        )),
        5
    );
    assertEquals(BigInteger.valueOf(200), step);
  }

  @Test
  public void testMurmurTokens() {
    String partitioner = "Murmur3Partitioner";
    ImmutableMap<String, String> ringTokens = ImmutableMap.of(
        "-9223372036854775808", "127.0.0.1:7000",
        "-3074457345618258603", "127.0.0.2:7000",
        "3074457345618258602", "127.0.0.3:7000"
    );

    List<Segment> segments = SegmentGenerator.generate(partitioner, ringTokens, 2);

    assertEquals(
        new BigInteger("-9223372036854775808"),
        segments.get(0).min
    );

    assertEquals(
        SegmentGenerator.maxToken(partitioner),
        segments.get(segments.size() - 1).max
    );

    assertEquals(
        6,
        segments.size()

    );
  }

  @Test
  public void testIsAroundToken() {

    BigInteger node1Token = new BigInteger("-9223372036854775808");
    BigInteger node2Token = new BigInteger("-3074457345618258603");
    BigInteger node3Token = new BigInteger("3074457345618258602");

    BigInteger step = node2Token.subtract(node1Token).divide(BigInteger.valueOf(2L));
    assertEquals(
        new BigInteger("3074457345618258602"),
        step
    );

    BigInteger intervalStart = node2Token.subtract(step);
    assertEquals(
        new BigInteger("-6148914691236517205"),
        intervalStart
    );

    BigInteger intervalEnd = node2Token.add(step);
    assertEquals(
        new BigInteger("-1"),
        intervalEnd
    );

    assertFalse(SegmentGenerator.isAroundToken(new Segment("-9223372036854775808", "-8608480567731124088"), intervalStart, intervalEnd));
    assertFalse(SegmentGenerator.isAroundToken(new Segment("-9223372036854775808", "-8608480567731124088"), intervalStart, intervalEnd));
    assertFalse(SegmentGenerator.isAroundToken(new Segment("-8608480567731124087", "-7993589098607472367"), intervalStart, intervalEnd));
    assertFalse(SegmentGenerator.isAroundToken(new Segment("-7993589098607472366", "-7378697629483820646"), intervalStart, intervalEnd));
    assertFalse(SegmentGenerator.isAroundToken(new Segment("-7378697629483820645", "-6763806160360168925"), intervalStart, intervalEnd));
    assertFalse(SegmentGenerator.isAroundToken(new Segment("-6763806160360168924", "-6148914691236517204"), intervalStart, intervalEnd));

    assertTrue(SegmentGenerator.isAroundToken(new Segment("-6148914691236517203", "-5534023222112865483"), intervalStart, intervalEnd));
    assertTrue(SegmentGenerator.isAroundToken(new Segment("-5534023222112865482", "-4919131752989213762"), intervalStart, intervalEnd));
    assertTrue(SegmentGenerator.isAroundToken(new Segment("-4919131752989213761", "-4304240283865562041"), intervalStart, intervalEnd));
    assertTrue(SegmentGenerator.isAroundToken(new Segment("-4304240283865562040", "-3689348814741910320"), intervalStart, intervalEnd));
    assertTrue(SegmentGenerator.isAroundToken(new Segment("-3689348814741910319", "-3074457345618258604"), intervalStart, intervalEnd));
    assertTrue(SegmentGenerator.isAroundToken(new Segment("-3074457345618258603", "-2459565876494606883"), intervalStart, intervalEnd));
    assertTrue(SegmentGenerator.isAroundToken(new Segment("-2459565876494606882", "-1844674407370955162"), intervalStart, intervalEnd));
    assertTrue(SegmentGenerator.isAroundToken(new Segment("-1844674407370955161", "-1229782938247303441"), intervalStart, intervalEnd));
    assertTrue(SegmentGenerator.isAroundToken(new Segment("-1229782938247303440", "-614891469123651720"), intervalStart, intervalEnd));

    assertFalse(SegmentGenerator.isAroundToken(new Segment("-614891469123651719", "1"), intervalStart, intervalEnd));
    assertFalse(SegmentGenerator.isAroundToken(new Segment("2", "614891469123651722"), intervalStart, intervalEnd));
    assertFalse(SegmentGenerator.isAroundToken(new Segment("614891469123651723", "1229782938247303443"), intervalStart, intervalEnd));
    assertFalse(SegmentGenerator.isAroundToken(new Segment("1229782938247303444", "1844674407370955164"), intervalStart, intervalEnd));
    assertFalse(SegmentGenerator.isAroundToken(new Segment("1844674407370955165", "2459565876494606885"), intervalStart, intervalEnd));
    assertFalse(SegmentGenerator.isAroundToken(new Segment("2459565876494606886", "3074457345618258601"), intervalStart, intervalEnd));
    assertFalse(SegmentGenerator.isAroundToken(new Segment("3074457345618258602", "3689348814741910322"), intervalStart, intervalEnd));
    assertFalse(SegmentGenerator.isAroundToken(new Segment("3689348814741910323", "4304240283865562043"), intervalStart, intervalEnd));
  }

}
