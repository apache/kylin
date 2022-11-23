package org.apache.kylin.metadata.cube.planner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.google.common.collect.Lists;

public class CostBasePlannerUtilsTest {

    private Map<Integer, Integer> getMap(int dimensionCount) {
        Map<Integer, Integer> result = new HashMap<>();
        for (int i = 0; i < dimensionCount; i++) {
            result.put(i, i);
        }
        return result;
    }

    @Test
    public void testConvertDimensionsToCuboId() {
        int maxCountDimension = 12;
        List<Integer> dimensionIds = Lists.newArrayList(4, 8, 11);
        BigInteger result = CostBasePlannerUtils.convertDimensionsToCuboId(dimensionIds, maxCountDimension, getMap(12));
        long expected = 0;
        expected = 1 << (12 - 1 - 4) | 1 << (12 - 1 - 8) | 1 << (12 - 1 - 11);
        BigInteger expectedInteger = BigInteger.valueOf(expected);
        assertEquals(expectedInteger, result);
    }
}