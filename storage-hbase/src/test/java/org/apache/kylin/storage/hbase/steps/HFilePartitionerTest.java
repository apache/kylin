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

package org.apache.kylin.storage.hbase.steps;

import java.util.ArrayList;

import org.apache.kylin.common.util.BytesUtil;
import org.junit.Assert;
import org.junit.Test;

public class HFilePartitionerTest {

    @Test
    public void testPartitioner() {
        String[] splitKyes = new String[] {
                "\\x00\\x0A\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x5F\\x03\\x00\\x7F\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF",
                "\\x00\\x02\\x00\\x01\\x00\\x7F\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF",
                "\\x00\\x0A\\x00\\x01\\x00\\x00\\x00\\x00\\x00\\x00\\x58\\xF3\\x00\\x7F\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF",
                "\\x00\\x02\\x00\\x02\\x00\\x7F\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF",
                "\\x00\\x0A\\x00\\x02\\x00\\x00\\x00\\x00\\x00\\x00\\x58\\x5C\\x00\\x7F\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF",
                "\\x00\\x0A\\x00\\x02\\x00\\x00\\x00\\x00\\x00\\x00\\x7C\\x50\\x00\\x7F\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF",
                "\\x00\\x02\\x00\\x03\\x00\\x7F\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF",
                "\\x00\\x0A\\x00\\x03\\x00\\x00\\x00\\x00\\x00\\x00\\x5B\\xF3\\x00\\x7F\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF",
                "\\x00\\x02\\x00\\x04\\x00\\x7F\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF",
                "\\x00\\x0A\\x00\\x04\\x00\\x00\\x00\\x00\\x00\\x00\\x5B\\xFC\\x00\\x7F\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF", };

        ArrayList<RowKeyWritable> keys = new ArrayList();
        for (int i = 0; i < splitKyes.length; i++) {
            byte[] bytes = BytesUtil.fromHex(splitKyes[i]);
            RowKeyWritable rowKeyWritable = new RowKeyWritable(bytes);
            keys.add(rowKeyWritable);
        }

        SparkCubeHFile.HFilePartitioner partitioner = new SparkCubeHFile.HFilePartitioner(keys);

        String testRowKey = "\\x00\\x11\\x00\\x02\\x00\\x00\\x00\\x00\\x00\\x00\\x40\\xAC\\x0B\\x37\\xF9\\x05\\x04\\x02\\x00\\x02\\x46\\x32\\x4D\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x04";

        int partition = partitioner.getPartition(new RowKeyWritable(BytesUtil.fromHex(testRowKey)));

        Assert.assertEquals(4, partition);

        testRowKey = "\\x00\\x0D\\x00\\x04\\x00\\x00\\x00\\x00\\x00\\x00\\x40\\x00\\x0B\\x39\\x6C\\x02\\x46\\x32\\x4D\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x04";
        partition = partitioner.getPartition(new RowKeyWritable(BytesUtil.fromHex(testRowKey)));

        Assert.assertEquals(9, partition);

    }

}
