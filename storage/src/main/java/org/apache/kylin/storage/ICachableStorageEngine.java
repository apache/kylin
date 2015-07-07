package org.apache.kylin.storage;

import com.google.common.collect.Range;

/**
 */
public interface ICachableStorageEngine extends IStorageQuery {
    /**
     *
     * being dynamic => getVolatilePeriod() return not null
     * being dynamic => partition column of its realization not null
     *
     * @return true for static storage like cubes
     *          false for dynamic storage like II
     */
    boolean isDynamic();

    /**
     * volatile period is the period of time in which the returned data is not stable
     * e.g. inverted index's last several minutes' data is dynamic as time goes by.
     * data in this period cannot be cached
     *
     * This method should not be called before ITupleIterator.close() is called
     *
     * @return null if the underlying storage guarantees the data is static
     */
    Range<Long> getVolatilePeriod();

    /**
     * get the uuid for the realization assigned to this storage engine
     * @return
     */
    String getStorageUUID();
}
