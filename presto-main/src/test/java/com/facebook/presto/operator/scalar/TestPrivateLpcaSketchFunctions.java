package com.facebook.presto.operator.scalar;
/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.facebook.airlift.stats.cardinality.HyperLogLog;
import com.google.common.io.BaseEncoding;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;

public class TestPrivateLpcaSketchFunctions
        extends AbstractTestFunctions
{
    private TestPrivateLpcaSketchFunctions() {}

    private static final int NUMBER_OF_BUCKETS = 4096;

    @Test
    public void testPrivatizeNullSketch()
    {
        functionAssertions.assertFunction("privatize_hll(NULL, NULL, NULL) IS NULL", BOOLEAN, true);
        functionAssertions.assertFunction("privatize_hll(NULL, 1, 1) IS NULL", BOOLEAN, true);
        functionAssertions.assertFunction("privatize_hll(NULL, NULL, 1) IS NULL", BOOLEAN, true);
        functionAssertions.assertFunction("privatize_hll(NULL, 1, NULL) IS NULL", BOOLEAN, true);
    }

    @Test
    public void testPrivatizeNonNullNullEpsilons()
    {
        String hllProjection = getHyperLogLogProjection(1);
        functionAssertions.assertFunction("privatize_hll(" + hllProjection + ", 1, NULL) IS NULL", BOOLEAN, true);
        functionAssertions.assertFunction("privatize_hll(" + hllProjection + ", NULL, 1) IS NULL", BOOLEAN, true);
    }

    @Test
    public void testCardinality()
    {
        long uniqueElements = 100_000;
        String between = getBetweenProjection(70_000, 130_000);
        String hllProjection = getHyperLogLogProjection(uniqueElements);

        // assertFunctionWithError assumes a deterministic result, so we'll do this instead!
        functionAssertions.assertFunction("private_cardinality(" + hllProjection + ", 10, 1) " + between, BOOLEAN, true);
        functionAssertions.assertFunction("cardinality(privatize_hll(" + hllProjection + ", 10, 1)) " + between, BOOLEAN, true);
    }

    @Test
    public void testCardinalityRoundTrip()
    {
        // Note: The cardinality in this test won't be exactly the same in the round-trip because we're comparing two different (random) sketches
        // So we test that the diffrence between them is approximately 0
        String hllProjection = getHyperLogLogProjection(200_000);
        String between = getBetweenProjection(-50_000, 50_000);
        String lpcaProjection = "privatize_hll(" + hllProjection + ", 10, 1)";

        String diff = "cardinality(" + lpcaProjection + ") - cardinality(CAST(CAST(" + lpcaProjection + " AS VARBINARY) AS PrivateLpcaSketch))";

        functionAssertions.assertFunction(diff + " " + between, BOOLEAN, true);
    }

    @Test
    public void testMergeNull()
    {
        String hllProjection = getHyperLogLogProjection(1);
        functionAssertions.assertFunction("merge_hll(NULL, " + hllProjection + ") IS NULL", BOOLEAN, true);
        functionAssertions.assertFunction("merge_hll(privatize_hll(" + hllProjection + ", 10, 1), NULL) IS NULL", BOOLEAN, true);
    }

    @Test
    public void testMerge()
    {
        long uniqueElements1 = 250_000;
        long uniqueElements2 = 200_000;
        String between = getBetweenProjection(400_000, 500_000);
        String hllProjection1 = getHyperLogLogProjection(uniqueElements1);
        String hllProjection2 = getHyperLogLogProjection(uniqueElements2, uniqueElements1);
        String mergeProjection = "merge_hll(privatize_hll(" + hllProjection1 + ", 10, 1), " + hllProjection2 + ")";

        functionAssertions.assertFunction("cardinality(" + mergeProjection + ") " + between, BOOLEAN, true);
    }

    private String getHyperLogLogProjection(long uniqueElements)
    {
        return getHyperLogLogProjection(uniqueElements, 0);
    }

    private String getHyperLogLogProjection(long uniqueElements, long start)
    {
        HyperLogLog hll = HyperLogLog.newInstance(NUMBER_OF_BUCKETS);
        for (long i = start; i < uniqueElements + start; i++) {
            hll.add(i);
        }
        byte[] serialized = hll.serialize().getBytes();
        return "CAST(X'" + BaseEncoding.base16().lowerCase().encode(serialized) + "' AS HyperLogLog)";
    }

    private String getBetweenProjection(long lower, long upper)
    {
        return "BETWEEN " + lower + " AND " + upper;
    }
}
