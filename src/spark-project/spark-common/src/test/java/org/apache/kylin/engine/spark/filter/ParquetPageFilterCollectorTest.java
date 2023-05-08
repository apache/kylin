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

package org.apache.kylin.engine.spark.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class ParquetPageFilterCollectorTest {

    @Test
    public void testParquetPageFilterCollector() {
        String queryId = "test_query_id";
        long totalPages = 10L;
        long filteredPages = 5L;
        long afterFilterPages = 3L;

        ParquetPageFilterCollector.addQueryMetrics(queryId, totalPages, filteredPages, afterFilterPages);
        assertNotNull(ParquetPageFilterCollector.queryTotalParquetPages.getIfPresent(queryId));
        assertEquals(ParquetPageFilterCollector.queryTotalParquetPages.getIfPresent(queryId).get(), totalPages);
        assertNotNull(ParquetPageFilterCollector.queryFilteredParquetPages.getIfPresent(queryId));
        assertEquals(ParquetPageFilterCollector.queryFilteredParquetPages.getIfPresent(queryId).get(), filteredPages);
        assertNotNull(ParquetPageFilterCollector.queryAfterFilterParquetPages.getIfPresent(queryId));
        assertEquals(ParquetPageFilterCollector.queryAfterFilterParquetPages.getIfPresent(queryId).get(), afterFilterPages);

        ParquetPageFilterCollector.logParquetPages(queryId);
        assertNull(ParquetPageFilterCollector.queryTotalParquetPages.getIfPresent(queryId));
        assertNull(ParquetPageFilterCollector.queryFilteredParquetPages.getIfPresent(queryId));
        assertNull(ParquetPageFilterCollector.queryAfterFilterParquetPages.getIfPresent(queryId));
    }
}
