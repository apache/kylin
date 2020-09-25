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

package org.apache.kylin.storage.hbase.common.coprocessor;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.kylin.common.util.Bytes;
import org.junit.Test;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

/**
 * @author yangli9
 * 
 */
public class RowProjectorTest {

    byte[] mask = new byte[] { (byte) 0xff, 0x00, 0x00, (byte) 0xff };
    CoprocessorProjector sample = new CoprocessorProjector(mask, true);

    @Test
    public void testSerialize() {

        byte[] bytes = CoprocessorProjector.serialize(sample);
        CoprocessorProjector copy = CoprocessorProjector.deserialize(bytes);

        assertTrue(Arrays.equals(sample.groupByMask, copy.groupByMask));
    }

    @Test
    public void testProject() {
        byte[] bytes1 = new byte[] { -1, -2, -3, -4 };
        byte[] bytes2 = new byte[] { 1, 2, 3, 4 };
        byte[] bytes3 = new byte[] { 1, 99, 100, 4 };
        byte[] bytes4 = new byte[] { 1, 1, 1, 5 };

        AggrKey rowKey = sample.getAggrKey(newCellWithRowKey(bytes1));
        AggrKey rowKey2 = sample.getAggrKey(newCellWithRowKey(bytes2));
        assertTrue(rowKey == rowKey2); // no extra object creation
        assertTrue(Bytes.equals(rowKey.get(), rowKey.offset(), rowKey.length(), bytes2, 0, bytes2.length));

        rowKey2 = rowKey.copy(); // explicit object creation
        assertTrue(rowKey != rowKey2);

        rowKey = sample.getAggrKey(newCellWithRowKey(bytes1));
        assertTrue(rowKey.hashCode() != rowKey2.hashCode());
        assertTrue(rowKey.equals(rowKey2) == false);
        assertTrue(rowKey.compareTo(rowKey2) > 0); // unsigned compare

        rowKey = sample.getAggrKey(newCellWithRowKey(bytes3));
        assertTrue(rowKey.hashCode() == rowKey2.hashCode());
        assertTrue(rowKey.equals(rowKey2) == true);
        assertTrue(rowKey.compareTo(rowKey2) == 0);

        rowKey = sample.getAggrKey(newCellWithRowKey(bytes4));
        assertTrue(rowKey.hashCode() != rowKey2.hashCode());
        assertTrue(rowKey.equals(rowKey2) == false);
        assertTrue(rowKey.compareTo(rowKey2) > 0);
    }

    private List<Cell> newCellWithRowKey(byte[] rowkey) {
        ArrayList<Cell> list = Lists.newArrayList();
        list.add(new KeyValue(rowkey, null, null, null));
        return list;
    }
}
