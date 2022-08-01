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

import org.apache.kylin.metadata.model.PartitionDesc;
import org.junit.Assert;
import org.junit.Test;

public class NPartitionDescTest {
    private static final String DATE_COL = "dateCol";
    private static final String DATE_FORMAT = "dateFormat";
    private static final String TIME_FORMAT = "timeFormat";
    private static final String TIME_COL = "timeCol";
    PartitionDesc.PartitionType partitionType = PartitionDesc.PartitionType.APPEND;

    @Test
    public void testEquals() {
        PartitionDesc partitionDesc1 = new PartitionDesc();
        Assert.assertEquals(partitionDesc1, partitionDesc1);
        Assert.assertNotEquals(partitionDesc1, new Integer(1));
        Assert.assertNotEquals(partitionDesc1, null);

        partitionDesc1.setCubePartitionType(partitionType);
        partitionDesc1.setPartitionDateColumn(DATE_COL);
        partitionDesc1.setPartitionDateFormat(DATE_FORMAT);

        PartitionDesc partitionDesc2 = new PartitionDesc();
        partitionDesc2.setCubePartitionType(partitionType);
        partitionDesc2.setPartitionDateColumn(DATE_COL);
        partitionDesc2.setPartitionDateFormat(DATE_FORMAT);
        Assert.assertEquals(partitionDesc1, partitionDesc2);
        Assert.assertEquals(partitionDesc1.hashCode(), partitionDesc2.hashCode());
        partitionDesc2.setPartitionDateFormat("new_date");
        Assert.assertNotEquals(partitionDesc1, partitionDesc2);

        PartitionDesc partitionDesc3 = PartitionDesc.getCopyOf(partitionDesc1);
        Assert.assertEquals(partitionDesc1.hashCode(), partitionDesc3.hashCode());
    }
}
