package com.kylinolap.cube.invertedindex;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.kylinolap.common.util.BytesUtil;
import com.kylinolap.common.util.LocalFileMetadataTestCase;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.dict.Dictionary;

public class InvertedIndexLocalTest extends LocalFileMetadataTestCase {

    CubeInstance cube;
    TableRecordInfo info;

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        this.cube = CubeManager.getInstance(getTestConfig()).getCube("test_kylin_cube_ii");
        this.info = new TableRecordInfo(cube.getFirstSegment());
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }
    
    @Test
    public void testBitMapContainer() {
        // create container
        BitMapContainer container = new BitMapContainer(info, 0);
        Dictionary<String> dict = info.dict(0);
        for (int v = dict.getMinId(); v <= dict.getMaxId(); v++) {
            container.append(v);
        }
        container.append(Dictionary.NULL_ID[dict.getSizeOfId()]);
        container.closeForChange();

        // copy by serialization
        List<ImmutableBytesWritable> bytes = container.toBytes();
        BitMapContainer container2 = new BitMapContainer(info, 0);
        container2.fromBytes(bytes);

        // check the copy
        int i = 0;
        for (int v = dict.getMinId(); v <= dict.getMaxId(); v++) {
            int value = container2.getValueIntAt(i++);
            assertEquals(v, value);
        }
        assertEquals(Dictionary.NULL_ID[dict.getSizeOfId()], container2.getValueIntAt(i++));
        assertEquals(container, container2);
    }

    @Test
    public void testCompressedValueContainer() {
        // create container
        CompressedValueContainer container = new CompressedValueContainer(info, 0, 500);
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
        CompressedValueContainer container2 = new CompressedValueContainer(info, 0, 500);
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

        IIKeyValueCodec codec = new IIKeyValueCodec(info);
        List<Pair<ImmutableBytesWritable, ImmutableBytesWritable>> kvs = encodeKVs(codec, slices);
        System.out.println(kvs.size() + " KV pairs");

        List<Slice> slicesCopy = decodeKVs(codec, kvs);
        assertEquals(slices, slicesCopy);

        List<TableRecord> recordsCopy = iterateRecords(slicesCopy);
        assertEquals(new HashSet<TableRecord>(records), new HashSet<TableRecord>(recordsCopy));
        dump(recordsCopy);
    }

    private List<TableRecord> loadRecordsSorted() throws IOException {
        File file = new File(LOCALMETA_TEST_DATA, "data/TEST_KYLIN_FACT.csv");
        FileInputStream in = new FileInputStream(file);
        List<String> lines = IOUtils.readLines(in, "UTF-8");
        in.close();

        List<TableRecord> records = Lists.newArrayList();
        for (String line : lines) {
            String[] fields = line.split(",");
            TableRecord rec = new TableRecord(info);
            for (int col = 0; col < fields.length; col++) {
                rec.setValueString(col, fields[col]);
            }
            records.add(rec);
        }

        Collections.sort(records, new Comparator<TableRecord>() {
            @Override
            public int compare(TableRecord a, TableRecord b) {
                return (int) (a.getTimestamp() - b.getTimestamp());
            }
        });

        return records;
    }

    private List<Slice> buildTimeSlices(List<TableRecord> records) throws IOException {
        ShardingSliceBuilder builder = new ShardingSliceBuilder(info);
        List<Slice> slices = Lists.newArrayList();
        for (TableRecord rec : records) {
            Slice slice = builder.append(rec);
            if (slice != null)
                slices.add(slice);
        }
        List<Slice> finals = builder.close();
        slices.addAll(finals);
        
        Collections.sort(slices);
        return slices;
    }

    private List<Pair<ImmutableBytesWritable, ImmutableBytesWritable>> encodeKVs(IIKeyValueCodec codec, List<Slice> slices) {

        List<Pair<ImmutableBytesWritable, ImmutableBytesWritable>> kvs = Lists.newArrayList();
        for (Slice slice : slices) {
            kvs.addAll(codec.encodeKeyValue(slice));
        }
        return kvs;
    }

    private List<Slice> decodeKVs(IIKeyValueCodec codec, List<Pair<ImmutableBytesWritable, ImmutableBytesWritable>> kvs) {
        List<Slice> slices = Lists.newArrayList();
        for (Slice slice : codec.decodeKeyValue(kvs)) {
            slices.add(slice);
        }
        return slices;
    }

    private List<TableRecord> iterateRecords(List<Slice> slices) {
        List<TableRecord> records = Lists.newArrayList();
        for (Slice slice : slices) {
            for (TableRecordBytes rec : slice) {
                records.add((TableRecord) rec.clone());
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
