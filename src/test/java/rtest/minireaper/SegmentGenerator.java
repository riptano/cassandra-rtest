package rtest.minireaper;

import com.beust.jcommander.internal.Lists;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.RandomPartitioner;

import java.math.BigInteger;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

public class SegmentGenerator {

  private static BigInteger TWO = BigInteger.valueOf(2);

  public static BigInteger minToken(String partitionerName) {
    IPartitioner partitioner = partitionerName.contains("Murmur")
        ? Murmur3Partitioner.instance
        : RandomPartitioner.instance;
    // returning +1 because C* excludes the lower bound
    return new BigInteger(partitioner.getMinimumToken().toString()).add(BigInteger.ONE);
  }

  public static BigInteger maxToken(String partitionerName) {
    IPartitioner partitioner = partitionerName.contains("Murmur")
        ? Murmur3Partitioner.instance
        : RandomPartitioner.instance;
    // return -1 because C* excludes upper bounds too, apparently
    return new BigInteger(partitioner.getMaximumToken().toString()).subtract(BigInteger.ONE);
  }

  public static BigInteger calculateStep(List<BigInteger> ringTokens, long steps) {
    return ringTokens.get(1).subtract(ringTokens.get(0)).abs().divide(BigInteger.valueOf(steps));
  }

  public static List<Segment> generateSegmentsSharingTwoReplicas(
      String partitionerName,
      Map<String, String> tokenToEndpointMap,
      long segmentsPerRange
  ) {
    LinkedList<BigInteger> sortedTokens = sortRingTokens(tokenToEndpointMap);

    // we want to generate segments that involve 2 out of the 3 endpoints
    // but we also need to involve node1 (with smallest token) because we know that one is inconsistent

    // we do this by generating all segments and then
    // we filter those that are "around" the midpoint of 2nd and 3rd token.
    // "around" means half the distance to the next token, both ways
    // half-distance from token to the start of interval we want is a 1/4 of the initial range

    // we know this is correct because the stream plan of the preview only says 1st nad 3rd node need to exchange data

    BigInteger step = sortedTokens.get(1)
        .subtract(sortedTokens.get(0))
        .divide(BigInteger.valueOf(4L));
    BigInteger intervalStart = sortedTokens.get(1).add(step);
    BigInteger intervalEnd = sortedTokens.get(2).subtract(step);

    return generate(partitionerName, sortedTokens, segmentsPerRange).stream()
        .filter(segment -> isAroundToken(segment, intervalStart, intervalEnd))
        .collect(toList());
  }

  public static List<Segment> generateSegmentsSharingThreeReplicas(
      String partitionerName,
      Map<String, String> tokenToEndpointMap,
      long segmentsPerRange
  ) {
    LinkedList<BigInteger> sortedTokens = sortRingTokens(tokenToEndpointMap);

    // here we want to generate a few segments that include all 3 replicas
    // but we don't want the entire ring.
    // again, we want to involve node 1 (smallest token) because it's inconsistent

    // we take similar approach to segments with 2 shared replicas,
    // but we continue across the 3rd token, which causes the remaining node to be involved

    BigInteger step = sortedTokens.get(1)
        .subtract(sortedTokens.get(0))
        .divide(BigInteger.valueOf(4L));

    BigInteger intervalStart = sortedTokens.get(1).add(step);
    BigInteger intervalEnd = sortedTokens.get(2).add(step);

    return generate(partitionerName, sortedTokens, segmentsPerRange).stream()
        .filter(segment -> isAroundToken(segment, intervalStart, intervalEnd))
        .collect(toList());
  }

  public static boolean isAroundToken(Segment segment, BigInteger intervalStart, BigInteger intervalEnd) {
    return segment.min.compareTo(intervalStart) > 0 && segment.max.compareTo(intervalEnd) < 0;
  }

  public static List<Segment> generate(
      String partitionerName,
      Map<String, String> tokenToEndpointMap,
      long segmentsPerRange
  ) {
    return generate(partitionerName, sortRingTokens(tokenToEndpointMap), segmentsPerRange);
  }

  private static List<Segment> generate(
      String partitionerName,
      LinkedList<BigInteger> ringTokens,
      long segmentsPerRange
  ) {
    List<Segment> segments = Lists.newArrayList();
    BigInteger partitionersMaxToken = maxToken(partitionerName);

    // we'll do a naiive way of generating the segments
    // we start with the smallest token in the ring, then add steps until we come to the end
    // we still need to take care to not create a segment spanning two endpoints

    BigInteger step = calculateStep(ringTokens, segmentsPerRange);

    BigInteger segmentStart = ringTokens.poll();
    BigInteger segmentEnd = segmentStart;

    // main loop goes until we would generate a segment that exceeds the partitioner range
    while (segmentEnd.compareTo(partitionersMaxToken) <= 0) {

      BigInteger possibleSegmentEnd = segmentStart.add(step);

      // when generating a new segment, we might end up in 3 situations

      // first is the new segment would cross an existing token range
      if (ringTokens.size() > 0 && possibleSegmentEnd.compareTo(ringTokens.peekFirst()) >= 0) {
        // in that case we need to generate segment a token one smaller than next ring token
        segmentEnd = ringTokens.peekFirst().subtract(BigInteger.ONE);
        segments.add(new Segment(segmentStart, segmentEnd));
        // next iteration we start from the next ring token we almost exceeded
        segmentStart = ringTokens.poll();
        continue;
      }

      // the second case is we ran out of ring tokens
      if (ringTokens.size() == 0) {
        // in this case we move by step until we exceed the partitioner
        segmentEnd = segmentStart.add(step);
        // check if we're still fitting in the partitioner
        if (segmentEnd.compareTo(partitionersMaxToken) > 0) {
          segmentEnd = partitionersMaxToken;
          segments.add(new Segment(segmentStart, segmentEnd));
          break;
        }
        segments.add(new Segment(segmentStart, segmentEnd));
        segmentStart = segmentEnd.add(BigInteger.ONE);
        continue;
      }

      // the final case is the simples, we are just splitting a range within the same initial interval
      segmentEnd = segmentStart.add(step);
      segments.add(new Segment(segmentStart, segmentEnd));
      segmentStart = segmentEnd.add(BigInteger.ONE);
    }

    // after the loop, we need to check for a reminder coverage
    if (segmentEnd.compareTo(partitionersMaxToken) < 0) {
      segmentStart = segmentEnd.add(BigInteger.ONE);
      segmentEnd = partitionersMaxToken;
      segments.add(new Segment(segmentStart, segmentEnd));
    }

    return segments;
  }

  public static LinkedList<BigInteger> sortRingTokens(Map<String, String> tokenToEndpointMap) {
    return Lists.newLinkedList(tokenToEndpointMap.keySet()
        .stream()
        .map(BigInteger::new)
        .sorted(BigInteger::compareTo)
        .collect(toList()));
  }

  public static List<Segment> takeEven(List<Segment> allSegments) {
    return IntStream.range(0, allSegments.size())
        .filter(i -> i % 2 == 0)
        .mapToObj(allSegments::get)
        .collect(toList());
  }

  public static List<Segment> takeOdd(List<Segment> allSegments) {
    return IntStream.range(0, allSegments.size())
        .filter(i -> i % 2 == 1)
        .mapToObj(allSegments::get)
        .collect(toList());
  }

}
