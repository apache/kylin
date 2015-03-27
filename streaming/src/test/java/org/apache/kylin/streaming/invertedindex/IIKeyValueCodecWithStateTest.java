package org.apache.kylin.streaming.invertedindex;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import javax.annotation.Nullable;

import org.apache.kylin.common.util.FIFOIterable;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.index.Slice;
import org.apache.kylin.invertedindex.index.TableRecordInfo;
import org.apache.kylin.invertedindex.index.TableRecordInfoDigest;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.invertedindex.model.IIKeyValueCodecWithState;
import org.apache.kylin.invertedindex.model.IIRow;
import org.apache.kylin.invertedindex.model.KeyValueCodec;
import org.apache.kylin.streaming.Stream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;

/**
 * Created by Hongbin Ma(Binmahone) on 3/26/15.
 */
public class IIKeyValueCodecWithStateTest extends LocalFileMetadataTestCase {

    IIInstance ii;
    IIDesc iiDesc;
    List<IIRow> iiRowList = Lists.newArrayList();

    final String[] inputs = new String[] { //
    "FP-non GTC,0,15,145970,0,28,Toys,2008-10-08 07:18:40,USER_Y,Toys & Hobbies,Models & Kits,Automotive,0,Ebay,USER_S,15,Professional-Other,2012-08-16,2012-08-11,0,2012-08-16,145970,10000329,26.8551,0", //
            "ABIN,0,-99,43479,0,21,Photo,2012-09-11 20:26:04,USER_Y,Cameras & Photo,Film Photography,Other,0,Ebay,USER_S,-99,Not Applicable,2012-08-16,2012-08-11,0,2012-08-16,43479,10000807,26.2474,0", //
            "ABIN,0,16,80053,0,12,Computers,2012-06-19 21:15:09,USER_Y,Computers/Tablets & Networking,MonitorProjectors & Accs,Monitors,0,Ebay,USER_S,16,Consumer-Other,2012-08-16,2012-08-11,0,2012-08-16,80053,10000261,94.2273,0" };

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        this.ii = IIManager.getInstance(getTestConfig()).getII("test_kylin_ii_inner_join");
        this.iiDesc = ii.getDescriptor();

        Collection<?> streams = Collections2.transform(Arrays.asList(inputs), new Function<String, Stream>() {
            @Nullable
            @Override
            public Stream apply(String input) {
                return new Stream(0, input.getBytes());
            }
        });
        LinkedBlockingQueue q = new LinkedBlockingQueue();
        q.addAll(streams);
        q.put(new Stream(-1, null));//a stop sign for builder

        ToyIIStreamBuilder builder = new ToyIIStreamBuilder(q, iiDesc, 0, iiRowList);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        Future<?> future = executorService.submit(builder);
        future.get();
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
        Assert.assertEquals(iiRowList.size(), digest.getColumnCount());

        for (int i = 0; i < digest.getColumnCount(); ++i) {
            buffer.add(iiRowList.get(i));

            if (i != digest.getColumnCount() - 1) {
                Assert.assertTrue(!slices.hasNext());
            } else {
                Assert.assertTrue(slices.hasNext());
            }
        }

        Slice newSlice = slices.next();
        Assert.assertEquals(newSlice.getLocalDictionaries().get(0).getSize(), 2);
    }
}
