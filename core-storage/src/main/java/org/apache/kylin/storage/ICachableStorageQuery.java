/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.storage;

import com.google.common.collect.Range;

/**
 */
public interface ICachableStorageQuery extends IStorageQuery {
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
     * get the uuid for the realization that serves this query
     */
    String getStorageUUID();
}
