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
package com.facebook.presto.operator.scalar;

import com.facebook.airlift.stats.cardinality.HyperLogLog;
import com.facebook.airlift.stats.cardinality.PrivateLpcaSketch;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.Failures.checkCondition;

public final class PrivateLpcaSketchFunctions
{
    private PrivateLpcaSketchFunctions() {}

    @ScalarFunction(visibility = SqlFunctionVisibility.EXPERIMENTAL)
    @Description("compute the cardinality of a PrivateLpcaSketch instance")
    @SqlType(StandardTypes.BIGINT)
    public static long cardinality(@SqlType(StandardTypes.PRIVATE_LPCA_SKETCH) Slice serializedSketch)
    {
        return (new PrivateLpcaSketch(serializedSketch)).cardinality();
    }

    @ScalarFunction(deterministic = false, visibility = SqlFunctionVisibility.EXPERIMENTAL)
    @Description("merge data from a HyperLogLog into an existing PrivateLpcaSketch")
    @SqlType(StandardTypes.PRIVATE_LPCA_SKETCH)
    public static Slice mergeHll(@SqlType(StandardTypes.PRIVATE_LPCA_SKETCH) Slice serializedSketch, @SqlType(StandardTypes.HYPER_LOG_LOG) Slice serializedHll)
    {
        HyperLogLog hll = HyperLogLog.newInstance(serializedHll);
        PrivateLpcaSketch sketch = new PrivateLpcaSketch(serializedSketch);
        checkCondition(hll.getNumberOfBuckets() == sketch.getNumberOfBuckets(), INVALID_FUNCTION_ARGUMENT, "sketches must have same number of buckets");

        sketch.update(hll);
        return sketch.serialize();
    }

    @ScalarFunction(deterministic = false, visibility = SqlFunctionVisibility.EXPERIMENTAL)
    @Description("converts a non-private HyperLogLog to a private LPCA sketch")
    @SqlType(StandardTypes.PRIVATE_LPCA_SKETCH)
    public static Slice privatizeHll(@SqlType(StandardTypes.HYPER_LOG_LOG) Slice serializedHll,
            @SqlType(StandardTypes.DOUBLE) double epsilonThreshold,
            @SqlType(StandardTypes.DOUBLE) double epsilonRandomizedResponse)
    {
        checkCondition(epsilonThreshold > 0, INVALID_FUNCTION_ARGUMENT, "epsilons must be positive");
        checkCondition(epsilonRandomizedResponse > 0, INVALID_FUNCTION_ARGUMENT, "epsilons must be positive");

        HyperLogLog hll = HyperLogLog.newInstance(serializedHll);
        PrivateLpcaSketch sketch = new PrivateLpcaSketch(hll, epsilonThreshold, epsilonRandomizedResponse);
        return sketch.serialize();
    }
}
