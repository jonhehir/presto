package com.facebook.presto.operator.aggregation;

import com.facebook.airlift.stats.cardinality.HyperLogLog;
import com.facebook.presto.operator.scalar.MathFunctions;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slice;
import org.apache.commons.math3.distribution.LaplaceDistribution;

public class PrivateHyperLogLogEstimator
{
    private HyperLogLog hll;
    private byte[] deltas;
    private int numberOfBuckets;
    private byte baseline;
    private int overflows;
    private int[] overflowBuckets;
    private byte[] overflowValues;
    byte indexBitLength;
    private static final int BITS_PER_BUCKET = 4;
    private static final int BUCKET_MASK = (1 << BITS_PER_BUCKET) - 1;
    private static final int MAX_DELTA = (1 << BITS_PER_BUCKET) - 1;

    public PrivateHyperLogLogEstimator(HyperLogLog hll)
    {
        hll.makeDense();
        deserialize(hll.serialize());
    }

    private void deserialize(Slice serialized)
    {
        BasicSliceInput input = serialized.getInput();

        byte formatTag = input.readByte();
        // assert formatTag == Format.DENSE_V2.getTag()

        indexBitLength = input.readByte();
        numberOfBuckets = 1 << indexBitLength; // see Utils::numberOfBuckets()

        baseline = input.readByte();
        deltas = new byte[numberOfBuckets / 2];
        input.readBytes(deltas);

        // ignore overflows for now
        int overflows = input.readUnsignedShort();

        //checkArgument(overflows <= numberOfBuckets, "Overflow entries is greater than actual number of buckets (possibly corrupt input)");

        int[] overflowBuckets = new int[overflows];
        byte[] overflowValues = new byte[overflows];

        for (int i = 0; i < overflows; i++) {
            overflowBuckets[i] = input.readUnsignedShort();
            //checkArgument(overflowBuckets[i] <= numberOfBuckets, "Overflow bucket index is out of range");
        }

        for (int i = 0; i < overflows; i++) {
            overflowValues[i] = input.readByte();
            //checkArgument(overflowValues[i] > 0, "Overflow bucket value must be > 0");
        }
//
//        baselineCount = 0;
//        for (int i = 0; i < numberOfBuckets; i++) {
//            if (getDelta(i) == 0) {
//                baselineCount++;
//            }
//        }
//
//        checkArgument(!input.isReadable(), "input is too big");
    }

    public long cardinality(double epsilon)
    {
        double sum = 0;
        for (int i = 0; i < numberOfBuckets; i++) {
            int value = getBucketValue(i);
            sum += 1.0 / (1L << value);
        }

        // sensitivity calculation
        double max_value = 1.0;
        int hash_value_length = Long.SIZE - indexBitLength;
        double min_value = 1.0 / (1L << hash_value_length);
        double sensitivity = max_value - min_value;

        // Add laplace noise (can use MathFunctions.inverseLaplaceCdf once landed)
        LaplaceDistribution dist = new LaplaceDistribution(null, 0, sensitivity/epsilon);
        sum += dist.inverseCumulativeProbability(MathFunctions.secure_random());

        double estimate = 0.673 * numberOfBuckets * numberOfBuckets / sum;
        //estimate = correctBias(estimate);

        return Math.round(estimate);
    }

    private static int bucketToSlot(int bucket)
    {
        return bucket >> 1;
    }

    private static int shiftForBucket(int bucket)
    {
        return ((~bucket) & 1) << 2;
    }

    private int getBucketValue(int bucket)
    {
        int slot = bucketToSlot(bucket);
        int delta = (deltas[slot] >> shiftForBucket(bucket)) & BUCKET_MASK;
        if (delta == MAX_DELTA) {
            delta += getOverflow(bucket);
        }
        return delta + baseline;
    }

    private int getOverflow(int bucket)
    {
        for (int i = 0; i < overflows; i++) {
            if (overflowBuckets[i] == bucket) {
                return overflowValues[i];
            }
        }
        return 0;
    }
}
