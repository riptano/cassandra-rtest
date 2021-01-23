package rtest.cassandra.jmx;

import java.math.BigInteger;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

public final class Segment
{

    private final BigInteger min;
    private final BigInteger max;

    public Segment(final Pair<BigInteger, BigInteger> range)
    {
        this(range.getLeft(), range.getRight());
    }

    public Segment(final String minBound, final String maxBound)
    {
        this(new BigInteger(minBound), new BigInteger(maxBound));
    }

    private Segment(final BigInteger minBound, final BigInteger maxBound)
    {
        this.min = minBound;
        this.max = maxBound;
    }

    @Override
    public String toString()
    {
        return String.format("%s:%s", min.toString(), max.toString());
    }

    @Override
    public boolean equals(final Object obj)
    {
        return obj instanceof Segment
                && min.equals(((Segment) obj).min)
                && max.equals(((Segment) obj).max);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(min, max);
    }

    public static String commaSeparatedRanges(final List<Segment> ranges)
    {
        return ranges.stream().map(Segment::toString).collect(Collectors.joining(","));
    }

    public boolean encloses(final Segment otherRange)
    {
        if (!isWrapping())
        {
            return !otherRange.isWrapping()
                    && greaterThanOrEqual(otherRange.min, min)
                    && lowerThanOrEqual(otherRange.max, max);
        }
        else
        {
            return (!otherRange.isWrapping()
                    && (greaterThanOrEqual(otherRange.min, min)
                            || lowerThanOrEqual(otherRange.max, max)))
                    || (greaterThanOrEqual(otherRange.min, min)
                            && lowerThanOrEqual(otherRange.max, max));
        }
    }

    static boolean lowerThanOrEqual(final BigInteger big0, final BigInteger big1)
    {
        return big0.compareTo(big1) <= 0;
    }

    static boolean greaterThanOrEqual(final BigInteger big0, final BigInteger big1)
    {
        return big0.compareTo(big1) >= 0;
    }

    public boolean isWrapping()
    {
        return max.compareTo(min) < 0;
    }
}
