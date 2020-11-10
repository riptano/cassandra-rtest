package rtest.minireaper;

import java.math.BigInteger;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

public class Segment {

  public final BigInteger min;
  public final BigInteger max;

  public Segment(Pair<BigInteger, BigInteger> range) {
    this(range.getLeft(), range.getRight());
  }

  public Segment(String min, String max) {
    this(new BigInteger(min), new BigInteger(max));
  }

  private Segment(BigInteger min, BigInteger max) {
    this.min = min;
    this.max = max;
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

  public boolean encloses(Segment otherRange) {
      if (!isWrapping()) {
        return !otherRange.isWrapping()
            && greaterThanOrEqual(otherRange.min, min)
            && lowerThanOrEqual(otherRange.max, max);
      } else {
        return (!otherRange.isWrapping()
            && (greaterThanOrEqual(otherRange.min, min)
            || lowerThanOrEqual(otherRange.max, max)))
            || (greaterThanOrEqual(otherRange.min, min)
            && lowerThanOrEqual(otherRange.max, max));
      }
  }

  static boolean lowerThanOrEqual(BigInteger big0, BigInteger big1) {
    return big0.compareTo(big1) <= 0;
  }

  static boolean greaterThanOrEqual(BigInteger big0, BigInteger big1) {
    return big0.compareTo(big1) >= 0;
  }

  public boolean isWrapping() {
    return max.compareTo(min) < 0;
  }
}
