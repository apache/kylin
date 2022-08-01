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
package org.apache.kylin.metadata.model;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.Assert;
import org.junit.Test;

import lombok.val;

public class SegmentRangeTest extends NLocalFileMetadataTestCase {

    @Test
    public void testKafkaOffsetRangeContains() {
        val rangeToMerge = new SegmentRange.KafkaOffsetPartitionedSegmentRange(1613957130000L, 1613957150000L,
                createKafkaPartitionsOffset(3, 300L), createKafkaPartitionsOffset(3, 500L));
        val seg1 = new SegmentRange.KafkaOffsetPartitionedSegmentRange(1613957130000L, 1613957140000L,
                createKafkaPartitionsOffset(3, 300L), createKafkaPartitionsOffset(3, 400L));
        val seg2 = new SegmentRange.KafkaOffsetPartitionedSegmentRange(1613957140000L, 1613957150000L,
                createKafkaPartitionsOffset(3, 400L), createKafkaPartitionsOffset(3, 500L));
        Assert.assertTrue(rangeToMerge.contains(seg1));
        Assert.assertTrue(rangeToMerge.contains(seg2));
    }

    @Test
    public void testKafkaOffsetRangeEquals() {
        val seg1 = new SegmentRange.KafkaOffsetPartitionedSegmentRange(1613957130000L, 1613957150000L,
                createKafkaPartitionsOffset(3, 300L), createKafkaPartitionsOffset(3, 500L));
        val seg2 = new SegmentRange.KafkaOffsetPartitionedSegmentRange(1613957130000L, 1613957150000L,
                createKafkaPartitionsOffset(3, 300L), createKafkaPartitionsOffset(3, 500L));
        Assert.assertEquals(seg1, seg2);
    }

    @Test
    public void testKafkaOffsetRangeCompareTo() {
        val seg1 = new SegmentRange.KafkaOffsetPartitionedSegmentRange(1613957130000L, 1613957140000L,
                createKafkaPartitionsOffset(3, 300L), createKafkaPartitionsOffset(3, 400L));
        val seg2 = new SegmentRange.KafkaOffsetPartitionedSegmentRange(1613957140000L, 1613957150000L,
                createKafkaPartitionsOffset(3, 400L), createKafkaPartitionsOffset(3, 500L));
        Assert.assertTrue(seg1.compareTo(seg2) < 0);
        Assert.assertTrue(seg2.compareTo(seg1) > 0);
        Assert.assertTrue(seg1.compareTo(seg1) == 0);
    }
}
