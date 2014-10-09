package com.kylinolap.cube.invertedindex;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
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
            int value = container2.getValueAt(i++);
            assertEquals(v, value);
        }
        assertEquals(Dictionary.NULL_ID[dict.getSizeOfId()], container2.getValueAt(i++));
        assertEquals(container, container2);
    }

    @Test
    public void testCompressedValueContainer() {
        // create container
        CompressedValueContainer container = new CompressedValueContainer(info, 0, 500);
        Dictionary<String> dict = info.dict(0);
        for (int v = dict.getMinId(); v <= dict.getMaxId(); v++) {
            container.append(v);
        }
        container.append(Dictionary.NULL_ID[dict.getSizeOfId()]);
        container.closeForChange();

        // copy by serialization
        ImmutableBytesWritable bytes = container.toBytes();
        CompressedValueContainer container2 = new CompressedValueContainer(info, 0, 500);
        container2.fromBytes(bytes);

        // check the copy
        int i = 0;
        for (int v = dict.getMinId(); v <= dict.getMaxId(); v++) {
            int value = container2.getValueAt(i++);
            assertEquals(v, value);
        }
        assertEquals(Dictionary.NULL_ID[dict.getSizeOfId()], container2.getValueAt(i++));
        assertEquals(container, container2);
    }

    @Test
    public void testCodec() throws IOException {
        List<TableRecord> records = loadRecordsSorted();
        System.out.println(records.size() + " records");
        List<TimeSlice> slices = buildTimeSlices(records);
        System.out.println(slices.size() + " slices");

        IIKeyValueCodec codec = new IIKeyValueCodec(info);
        List<Pair<ImmutableBytesWritable, ImmutableBytesWritable>> kvs = encodeKVs(codec, slices);
        System.out.println(kvs.size() + " KV pairs");

        List<TimeSlice> slicesCopy = decodeKVs(codec, kvs);
        assertEquals(slices, slicesCopy);

        List<TableRecord> recordsCopy = iterateRecords(slicesCopy);
        assertEquals(records, recordsCopy);
        dump(recordsCopy);
    }

    private List<TableRecord> loadRecordsSorted() throws IOException {
        File file = new File(this.testDataFolder, "data/TEST_KYLIN_FACT.csv");
        FileInputStream in = new FileInputStream(file);
        List<String> lines = IOUtils.readLines(in, "UTF-8");
        in.close();

        List<TableRecord> records = Lists.newArrayList();
        for (String line : lines) {
            String[] fields = line.split(",");
            TableRecord rec = new TableRecord(info);
            for (int col = 0; col < fields.length; col++) {
                rec.setValue(col, Bytes.toBytes(fields[col]));
            }
            records.add(rec);
        }

        Collections.sort(records, new Comparator<TableRecord>() {
            @Override
            public int compare(TableRecord a, TableRecord b) {
                return a.getValueID(1) - b.getValueID(1); // the second column is CAL_DT
            }
        });

        return records;
    }

    private List<TimeSlice> buildTimeSlices(List<TableRecord> records) throws IOException {
        TimeSliceBuilder builder = new TimeSliceBuilder(info);
        List<TimeSlice> slices = Lists.newArrayList();
        for (TableRecord rec : records) {
            TimeSlice slice = builder.append(rec);
            if (slice != null)
                slices.add(slice);
        }
        TimeSlice slice = builder.close();
        if (slice != null)
            slices.add(slice);
        return slices;
    }

    private List<Pair<ImmutableBytesWritable, ImmutableBytesWritable>> encodeKVs(IIKeyValueCodec codec,
            List<TimeSlice> slices) {

        List<Pair<ImmutableBytesWritable, ImmutableBytesWritable>> kvs = Lists.newArrayList();
        for (TimeSlice slice : slices) {
            kvs.addAll(codec.encodeKeyValue(slice));
        }
        return kvs;
    }

    private List<TimeSlice> decodeKVs(IIKeyValueCodec codec,
            List<Pair<ImmutableBytesWritable, ImmutableBytesWritable>> kvs) {
        List<TimeSlice> slices = Lists.newArrayList();
        for (TimeSlice slice : codec.decodeKeyValue(kvs)) {
            slices.add(slice);
        }
        return slices;
    }

    private List<TableRecord> iterateRecords(List<TimeSlice> slices) {
        List<TableRecord> records = Lists.newArrayList();
        for (TimeSlice slice : slices) {
            for (TableRecord rec : slice) {
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
