package org.apache.kylin.job.hadoop.invertedindex;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.MutationSerialization;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.kylin.common.util.FIFOIterable;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.index.Slice;
import org.apache.kylin.invertedindex.index.TableRecordInfo;
import org.apache.kylin.invertedindex.index.TableRecordInfoDigest;
import org.apache.kylin.invertedindex.model.*;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.cube.FactDistinctIIColumnsMapper;
import org.apache.kylin.streaming.Stream;
import org.apache.kylin.streaming.StringStreamParser;
import org.apache.kylin.streaming.invertedindex.SliceBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

/**
 * Created by Hongbin Ma(Binmahone) on 3/26/15.
 */
public class II2CubeTest extends LocalFileMetadataTestCase {

    String iiName = "test_kylin_ii_inner_join";
    IIInstance ii;
    IIDesc iiDesc;
    String cubeName = "test_kylin_cube_with_slr_empty";

    List<IIRow> iiRows;

    final String[] inputs = new String[]{ //
            "FP-non GTC,0,15,145970,0,28,Toys,2008-10-08 07:18:40,USER_Y,Toys & Hobbies,Models & Kits,Automotive,0,Ebay,USER_S,15,Professional-Other,2012-08-16,2012-08-11,0,2012-08-16,145970,10000329,26.8551,0", //
            "ABIN,0,-99,43479,0,21,Photo,2012-09-11 20:26:04,USER_Y,Cameras & Photo,Film Photography,Other,0,Ebay,USER_S,-99,Not Applicable,2012-08-16,2012-08-11,0,2012-08-16,43479,10000807,26.2474,0", //
            "ABIN,0,16,80053,0,12,Computers,2012-06-19 21:15:09,USER_Y,Computers/Tablets & Networking,MonitorProjectors & Accs,Monitors,0,Ebay,USER_S,16,Consumer-Other,2012-08-16,2012-08-11,0,2012-08-16,80053,10000261,94.2273,0"};

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        this.ii = IIManager.getInstance(getTestConfig()).getII(iiName);
        this.iiDesc = ii.getDescriptor();

        List<Stream> streams = Lists.transform(Arrays.asList(inputs), new Function<String, Stream>() {
            @Nullable
            @Override
            public Stream apply(String input) {
                return new Stream(System.currentTimeMillis(), input.getBytes());
            }
        });

        iiRows = Lists.newArrayList();
        final Slice slice = new SliceBuilder(iiDesc, (short) 0).buildSlice(streams, StringStreamParser.instance);
        IIKeyValueCodec codec = new IIKeyValueCodec(slice.getInfo());
        for (IIRow iiRow : codec.encodeKeyValue(slice)) {
            iiRows.add(iiRow);
        }

    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    /**
     * simulate stream building into slices, and encode the slice into IIRows.
     * Then reconstruct the IIRows to slice.
     */
    @Test
    public void basicTest() {
        Queue<IIRow> buffer = Lists.newLinkedList();
        FIFOIterable bufferIterable = new FIFOIterable(buffer);
        TableRecordInfo info = new TableRecordInfo(iiDesc);
        TableRecordInfoDigest digest = info.getDigest();
        KeyValueCodec codec = new IIKeyValueCodecWithState(digest);
        Iterator<Slice> slices = codec.decodeKeyValue(bufferIterable).iterator();

        Assert.assertTrue(!slices.hasNext());
        Assert.assertEquals(iiRows.size(), digest.getColumnCount());

        for (int i = 0; i < digest.getColumnCount(); ++i) {
            buffer.add(iiRows.get(i));

            if (i != digest.getColumnCount() - 1) {
                Assert.assertTrue(!slices.hasNext());
            } else {
                Assert.assertTrue(slices.hasNext());
            }
        }

        Slice newSlice = slices.next();
        Assert.assertEquals(newSlice.getLocalDictionaries().get(0).getSize(), 2);
    }

    @Test
    public void factDistinctIIColumnsMapperTest() throws IOException {
        MapDriver<ImmutableBytesWritable, Result, LongWritable, Text> mapDriver;
        FactDistinctIIColumnsMapper mapper = new FactDistinctIIColumnsMapper();
        mapDriver = MapDriver.newMapDriver(mapper);

        mapDriver.getConfiguration().set(BatchConstants.CFG_II_NAME, iiName);
        mapDriver.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
        mapDriver.getConfiguration().setStrings("io.serializations", mapDriver.getConfiguration().get("io.serializations"), MutationSerialization.class.getName(), ResultSerialization.class.getName());
        mapDriver.addAll(Lists.newArrayList(Collections2.transform(iiRows, new Function<IIRow, Pair<ImmutableBytesWritable, Result>>() {
            @Nullable
            @Override
            public Pair<ImmutableBytesWritable, Result> apply(@Nullable IIRow input) {
                return new Pair<ImmutableBytesWritable, Result>(new ImmutableBytesWritable(new byte[]{1}), Result.create(input.makeCells()));
            }
        })));

        List<Pair<LongWritable, Text>> result = mapDriver.run();
        Set<String> lstgNames = Sets.newHashSet("FP-non GTC", "ABIN");
        for (Pair<LongWritable, Text> pair : result) {
            Assert.assertEquals(pair.getFirst().get(), 6);
            Assert.assertTrue(lstgNames.contains(pair.getSecond().toString()));
        }
    }

}
