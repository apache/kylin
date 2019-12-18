package org.apache.kylin.engine.spark.metadata.cube;

import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public final class BitUtils {
    private static final Logger log = LoggerFactory.getLogger(BitUtils.class);

    public static List<Integer> tailor(List<Integer> complete, long cuboidId) {

        int bitCount = Long.bitCount(cuboidId);

        Integer[] ret = new Integer[bitCount];

        int next = 0;
        int size = complete.size();
        for (int i = 0; i < size; i++) {
            int shift = size - i - 1;
            if ((cuboidId & (1L << shift)) != 0) {
                ret[next++] = complete.get(i);
            }
        }

        return Arrays.asList(ret);
    }
}
