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

package org.apache.kylin.invertedindex.invertedindex;

import com.google.common.base.Function;
import com.google.common.collect.*;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.dict.Dictionary;
import org.apache.kylin.dict.DictionaryGenerator;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.index.*;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.invertedindex.model.IIKeyValueCodec;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.kylin.invertedindex.model.IIRow;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class InvertedIndexLocalTest extends LocalFileMetadataTestCase {

    IIInstance ii;
    TableRecordInfo info;
    List<String> lines;
    private Dictionary<?>[] dictionaryMap;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        this.ii = IIManager.getInstance(getTestConfig()).getII("test_kylin_ii_left_join");

        File file = new File(LOCALMETA_TEST_DATA, "data/flatten_data_for_ii.csv");
        FileInputStream in = new FileInputStream(file);
        this.lines = IOUtils.readLines(in, "UTF-8");
        in.close();

        dictionaryMap = buildDictionary(Lists.transform(lines, new Function<String, List<String>>() {
            @Nullable
            @Override
            public List<String> apply(@Nullable String input) {
                return Lists.newArrayList(input.split(","));
            }
        }), ii.getDescriptor());
        this.info = new TableRecordInfo(ii.getDescriptor(), dictionaryMap);
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testCompressedValueContainer() {
        // create container
        CompressedValueContainer container = new CompressedValueContainer(info.getDigest(), 0, 500);
        Dictionary<String> dict = info.dict(0);

        byte[] buf = new byte[dict.getSizeOfId()];
        ImmutableBytesWritable bytes = new ImmutableBytesWritable(buf);

        for (int v = dict.getMinId(); v <= dict.getMaxId(); v++) {
            BytesUtil.writeUnsigned(v, buf, 0, dict.getSizeOfId());
            container.append(bytes);
        }
        BytesUtil.writeUnsigned(Dictionary.NULL_ID[dict.getSizeOfId()], buf, 0, dict.getSizeOfId());
        container.append(bytes);
        container.closeForChange();

        // copy by serialization
        ImmutableBytesWritable copy = container.toBytes();
        CompressedValueContainer container2 = new CompressedValueContainer(info.getDigest(), 0, 500);
        container2.fromBytes(copy);

        // check the copy
        int i = 0;
        for (int v = dict.getMinId(); v <= dict.getMaxId(); v++) {
            container2.getValueAt(i++, bytes);
            int value = BytesUtil.readUnsigned(bytes.get(), bytes.getOffset(), bytes.getLength());
            assertEquals(v, value);
        }
        container2.getValueAt(i++, bytes);
        int value = BytesUtil.readUnsigned(bytes.get(), bytes.getOffset(), bytes.getLength());
        assertEquals(Dictionary.NULL_ID[dict.getSizeOfId()], value);
        assertEquals(container, container2);
    }

    @Test
    public void testCodec() throws IOException {
        List<TableRecord> records = loadRecordsSorted();
        System.out.println(records.size() + " records");
        List<Slice> slices = buildTimeSlices(records);
        System.out.println(slices.size() + " slices");

        IIKeyValueCodec codec = new IIKeyValueCodec(info.getDigest());
        List<IIRow> kvs = encodeKVs(codec, slices);
        System.out.println(kvs.size() + " KV pairs");

        List<Slice> slicesCopy = decodeKVs(codec, kvs);
        assertEquals(slices.size(), slicesCopy.size());
        for (int i = 0; i < slices.size(); i++) {
            assertEquals(slices.get(i), slicesCopy.get(i));
        }

        List<TableRecord> recordsCopy = iterateRecords(slicesCopy);
        assertEquals(new HashSet<TableRecord>(records), new HashSet<TableRecord>(recordsCopy));
        dump(recordsCopy);
    }

    private Dictionary<?>[] buildDictionary(List<List<String>> table, IIDesc desc) {
        SetMultimap<TblColRef, String> valueMap = HashMultimap.create();
        Set<TblColRef> dimensionColumns = Sets.newHashSet();
        for (int i = 0; i < desc.listAllColumns().size(); i++) {
            if (!desc.isMetricsCol(i)) {
                dimensionColumns.add(desc.listAllColumns().get(i));
            }
        }
        for (List<String> row : table) {
            for (int i = 0; i < row.size(); i++) {
                String cell = row.get(i);
                valueMap.put(desc.listAllColumns().get(i), cell);
            }
        }
        Dictionary<?>[] result = new Dictionary<?>[desc.listAllColumns().size()];
        for (TblColRef tblColRef : valueMap.keys()) {
            result[desc.findColumn(tblColRef)] = DictionaryGenerator.buildDictionaryFromValueList(tblColRef.getType(), Collections2.transform(valueMap.get(tblColRef), new Function<String, byte[]>() {
                @Nullable
                @Override
                public byte[] apply(String input) {
                    return input.getBytes();
                }
            }));
        }
        return result;
    }

    private List<TableRecord> loadRecordsSorted() throws IOException {
        List<TableRecord> records = Lists.newArrayList();
        for (String line : lines) {
            String[] fields = line.split(",");
            TableRecord rec = info.createTableRecord();
            for (int col = 0; col < fields.length; col++) {
                rec.setValueString(col, fields[col]);
            }
            records.add(rec);
        }

        Collections.sort(records, new Comparator<TableRecord>() {
            @Override
            public int compare(TableRecord a, TableRecord b) {
                long x = a.getTimestamp() - b.getTimestamp();
                if (x > 0)
                    return 1;
                else if (x == 0)
                    return 0;
                else
                    return -1;
            }
        });

        return records;
    }

    private List<Slice> buildTimeSlices(List<TableRecord> records) throws IOException {
        ShardingSliceBuilder builder = new ShardingSliceBuilder(info);
        List<Slice> slices = Lists.newArrayList();
        for (TableRecord rec : records) {
            //here assume there less records than slice size for each shard
            Slice slice = builder.append(rec);
            if (slice != null) {
                slice.setLocalDictionaries(dictionaryMap);
                slices.add(slice);
            }
        }
        List<Slice> finals = builder.close();
        for (Slice slice : finals) {
            slice.setLocalDictionaries(dictionaryMap);
        }
        slices.addAll(finals);

        Collections.sort(slices);
        return slices;
    }

    private List<IIRow> encodeKVs(IIKeyValueCodec codec, List<Slice> slices) {

        List<IIRow> kvs = Lists.newArrayList();
        for (Slice slice : slices) {
            kvs.addAll(codec.encodeKeyValue(slice));
        }
        return kvs;
    }

    private List<Slice> decodeKVs(IIKeyValueCodec codec, List<IIRow> kvs) {
        List<Slice> slices = Lists.newArrayList();
        for (Slice slice : codec.decodeKeyValue(kvs)) {
            slices.add(slice);
        }
        return slices;
    }

    private List<TableRecord> iterateRecords(List<Slice> slices) {
        List<TableRecord> records = Lists.newArrayList();
        for (Slice slice : slices) {
            for (RawTableRecord rec : slice) {
                records.add(new TableRecord((RawTableRecord) rec.clone(), info));
            }
        }
        return records;
    }

    private void dump(Iterable<TableRecord> records) {
        for (TableRecord rec : records) {
            System.out.println(rec.toString());
        }
    }

}
