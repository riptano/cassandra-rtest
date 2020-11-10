package rtest.minireaper;

import java.math.BigInteger;
import java.util.List;
import java.util.stream.Collectors;

public class Segment {

  public final BigInteger min;
  public final BigInteger max;

  public Segment(BigInteger min, BigInteger max) {
    this.min = min;
    this.max = max;
  }

  public Segment(String min, String max) {
    this.min = new BigInteger(min);
    this.max = new BigInteger(max);
  }

  @Override
  public String toString() {
    return String.format("%s:%s", min.toString(), max.toString());
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Segment
        && min.equals(((Segment) obj).min)
        && max.equals(((Segment) obj).max);
  }

  public static String commaSeparatedRanges(List<Segment> ranges) {
    return ranges.stream().map(Segment::toString).collect(Collectors.joining(","));
  }

}
