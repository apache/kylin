package org.apache.kylin.storage.util;

import net.sf.ehcache.pool.sizeof.ReflectionSizeOf;

/**
 */
public final class SizeOfUtil {

    private SizeOfUtil() {
    }

    private static final ReflectionSizeOf DEFAULT_SIZE_OF = new ReflectionSizeOf();

    public static final long deepSizeOf(Object obj) {
        return DEFAULT_SIZE_OF.deepSizeOf(Integer.MAX_VALUE, true, obj).getCalculated();
    }

    public static final long sizeOf(Object obj) {
        return DEFAULT_SIZE_OF.sizeOf(obj);
    }
}
