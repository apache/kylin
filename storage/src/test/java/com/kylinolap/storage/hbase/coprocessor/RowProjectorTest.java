/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.storage.hbase.coprocessor;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * @author yangli9
 */
public class RowProjectorTest {

    byte[] mask = new byte[]{0x00, 0x01, 0x7f, (byte) 0xff};
    RowProjector sample = new RowProjector(mask);

    @Test
    public void testSerialize() {

        byte[] bytes = RowProjector.serialize(sample);
        RowProjector copy = RowProjector.deserialize(bytes);

        assertTrue(Arrays.equals(sample.rowKeyBitMask, copy.rowKeyBitMask));
    }

    @Test
    public void testProject() {
        ImmutableBytesWritable rowKey =
                sample.getRowKey(newCellWithRowKey(new byte[]{(byte) 0xff, (byte) 0xff, (byte) 0xff,
                        (byte) 0xff}));
        assertTrue(Bytes.equals(mask, 0, mask.length, rowKey.get(), rowKey.getOffset(), rowKey.getLength()));
    }

    private List<Cell> newCellWithRowKey(byte[] rowkey) {
        ArrayList<Cell> list = Lists.newArrayList();
        list.add(new KeyValue(rowkey, null, null, null));
        return list;
    }
}
