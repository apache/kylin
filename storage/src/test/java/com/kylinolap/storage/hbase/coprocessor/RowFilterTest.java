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
import com.kylinolap.storage.hbase.coprocessor.RowFilter.ColumnFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author yangli9
 */
public class RowFilterTest {

    public static List<byte[]> rowkeys = Lists.newArrayList();

    static {
        // A (1B), B (2B), C (3B)
        rowkeys.add(new byte[]{0x00, 0x02, 0x01, 0x03, 0x03, 0x01});
        rowkeys.add(new byte[]{0x01, 0x02, 0x01, 0x03, 0x03, 0x02}); // hit
        rowkeys.add(new byte[]{0x01, 0x02, 0x02, 0x03, 0x03, 0x02}); // hit
        rowkeys.add(new byte[]{0x01, 0x02, 0x02, 0x03, 0x03, 0x02}); // hit
        rowkeys.add(new byte[]{0x02, 0x02, 0x03, 0x03, 0x03, 0x03}); // hit
        rowkeys.add(new byte[]{0x02, 0x02, 0x03, 0x03, 0x03, 0x03}); // hit
        rowkeys.add(new byte[]{0x02, 0x02, 0x04, 0x03, 0x03, 0x04});
    }

    @Test
    public void testFilter() {
        RowFilter sample = newSampleRowFilter();
        for (int i = 0; i < rowkeys.size(); i++) {
            ImmutableBytesWritable rowkey = new ImmutableBytesWritable(rowkeys.get(i));
            boolean match = sample.evaluate(rowkey);
            if (i >= 1 && i <= 5) {
                assertTrue(match);
            } else {
                assertFalse(match);
            }
        }
    }

    @Test
    public void testSerialize() {
        RowFilter sample = newSampleRowFilter();

        byte[] bytes = RowFilter.serialize(sample);
        RowFilter copy = RowFilter.deserialize(bytes);

        assertTrue(copy.equals(sample));
    }

    private RowFilter newSampleRowFilter() {
        ColumnFilter[][] filter = new ColumnFilter[4][];
        filter[0] = new ColumnFilter[]{ //
                newFilterA(new byte[]{0x01}, new byte[]{0x01}) //
        };
        filter[1] = new ColumnFilter[]{ //
                newFilterB(new byte[]{0x02, 0x02}, new byte[]{0x02, 0x03}), //
                newFilterC(new byte[]{0x03, 0x03, 0x03}, new byte[]{0x03, 0x03, 0x04}) //
        };

        filter[2] = null;
        filter[3] = new ColumnFilter[0];
        return new RowFilter(filter);
    }

    private ColumnFilter newFilterA(byte[] start, byte[] end) {
        return new ColumnFilter(0, 1, start, end);
    }

    private ColumnFilter newFilterB(byte[] start, byte[] end) {
        return new ColumnFilter(1, 2, start, end);
    }

    private ColumnFilter newFilterC(byte[] start, byte[] end) {
        return new ColumnFilter(3, 3, start, end);
    }
}
