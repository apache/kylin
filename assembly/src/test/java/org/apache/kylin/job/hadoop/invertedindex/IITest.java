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

package org.apache.kylin.job.hadoop.invertedindex;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import javax.annotation.Nullable;


import com.google.common.base.Function;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.kylin.common.util.FIFOIterable;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StreamingBatch;
import org.apache.kylin.common.util.StreamingMessage;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.invertedindex.index.Slice;
import org.apache.kylin.invertedindex.index.TableRecordInfo;
import org.apache.kylin.invertedindex.index.TableRecordInfoDigest;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.invertedindex.model.IIKeyValueCodec;
import org.apache.kylin.invertedindex.model.IIKeyValueCodecWithState;
import org.apache.kylin.invertedindex.model.IIRow;
import org.apache.kylin.invertedindex.model.KeyValueCodec;
import org.apache.kylin.invertedindex.index.SliceBuilder;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.kafka.StreamingParser;
import org.apache.kylin.source.kafka.StringStreamingParser;
import org.apache.kylin.storage.hbase.common.coprocessor.CoprocessorFilter;
import org.apache.kylin.storage.hbase.common.coprocessor.CoprocessorProjector;
import org.apache.kylin.storage.hbase.common.coprocessor.CoprocessorRowType;
import org.apache.kylin.storage.hbase.common.coprocessor.FilterDecorator;
import org.apache.kylin.storage.hbase.ii.coprocessor.endpoint.ClearTextDictionary;
import org.apache.kylin.storage.hbase.ii.coprocessor.endpoint.EndpointAggregators;
import org.apache.kylin.storage.hbase.ii.coprocessor.endpoint.IIEndpoint;
import org.apache.kylin.storage.hbase.ii.coprocessor.endpoint.generated.IIProtos;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 */
public class IITest extends LocalFileMetadataTestCase {

    String iiName = "test_kylin_ii_inner_join";
    IIInstance ii;
    IIDesc iiDesc;

    List<IIRow> iiRows;

    final String[] inputData = new String[] { //
    "FP-non GTC,0,15,145970,0,28,Toys,2008-10-08 07:18:40,USER_Y,Toys & Hobbies,Models & Kits,Automotive,0,Ebay,USER_S,15,Professional-Other,2012-08-16,2012-08-11,0,2012-08-16,145970,10000329,26.8551,0", //
            "ABIN,0,-99,43479,0,21,Photo,2012-09-11 20:26:04,USER_Y,Cameras & Photo,Film Photography,Other,0,Ebay,USER_S,-99,Not Applicable,2012-08-16,2012-08-11,0,2012-08-16,43479,10000807,26.2474,0", //
            "ABIN,0,16,80053,0,12,Computers,2012-06-19 21:15:09,USER_Y,Computers/Tablets & Networking,MonitorProjectors & Accs,Monitors,0,Ebay,USER_S,16,Consumer-Other,2012-08-16,2012-08-11,0,2012-08-16,80053,10000261,94.2273,0" };

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        this.ii = IIManager.getInstance(getTestConfig()).getII(iiName);
        this.iiDesc = ii.getDescriptor();

        List<MessageAndOffset> messages = Lists.transform(Arrays.asList(inputData), new Function<String, MessageAndOffset>() {
            @Nullable
            @Override
            public MessageAndOffset apply(String input) {
                return new MessageAndOffset(new Message(input.getBytes()), System.currentTimeMillis());
            }
        });

        final StreamingParser parser = StringStreamingParser.instance;
        final List<StreamingMessage> streamingMessages = Lists.transform(messages, new Function<MessageAndOffset, StreamingMessage>() {
            @Nullable
            @Override
            public StreamingMessage apply(@Nullable MessageAndOffset input) {
                return parser.parse(input);
            }
        });
        StreamingBatch batch = new StreamingBatch(streamingMessages, Pair.newPair(0L, System.currentTimeMillis()));

        iiRows = Lists.newArrayList();
        final Slice slice = new SliceBuilder(iiDesc, (short) 0).buildSlice((batch));
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
        Assert.assertEquals(newSlice.getLocalDictionaries()[0].getSize(), 2);
    }

    @Test
    public void IIEndpointTest() {
        TableRecordInfo info = new TableRecordInfo(ii.getDescriptor());
        if (ii.getFirstSegment() == null) {
            IISegment segment = IIManager.getInstance(getTestConfig()).buildSegment(ii, 0, System.currentTimeMillis());
            ii.getSegments().add(segment);
        }
        CoprocessorRowType type = CoprocessorRowType.fromTableRecordInfo(info, ii.getFirstSegment().getColumns());
        CoprocessorProjector projector = CoprocessorProjector.makeForEndpoint(info, Collections.singletonList(ii.getDescriptor().findColumnRef("default.test_kylin_fact", "lstg_format_name")));

        FunctionDesc f1 = new FunctionDesc();
        f1.setExpression("SUM");
        ParameterDesc p1 = new ParameterDesc();
        p1.setType("column");
        p1.setValue("PRICE");
        f1.setParameter(p1);
        f1.setReturnType("decimal(19,4)");

        TblColRef column = ii.getDescriptor().findColumnRef("default.test_kylin_fact", "cal_dt");
        CompareTupleFilter compareFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.GTE);
        ColumnTupleFilter columnFilter = new ColumnTupleFilter(column);
        compareFilter.addChild(columnFilter);
        ConstantTupleFilter constantFilter = null;
        constantFilter = new ConstantTupleFilter(("2012-08-16"));
        compareFilter.addChild(constantFilter);

        EndpointAggregators aggregators = EndpointAggregators.fromFunctions(info, Collections.singletonList(f1));
        CoprocessorFilter filter = CoprocessorFilter.fromFilter(new ClearTextDictionary(info), compareFilter, FilterDecorator.FilterConstantsTreatment.AS_IT_IS);

        final Iterator<IIRow> iiRowIterator = iiRows.iterator();

        IIEndpoint endpoint = new IIEndpoint();
        IIProtos.IIResponseInternal response = endpoint.getResponse(new RegionScanner() {
            @Override
            public HRegionInfo getRegionInfo() {
                throw new NotImplementedException();
            }

            @Override
            public boolean isFilterDone() throws IOException {
                throw new NotImplementedException();
            }

            @Override
            public boolean reseek(byte[] row) throws IOException {
                throw new NotImplementedException();
            }

            @Override
            public long getMaxResultSize() {
                throw new NotImplementedException();

            }

            @Override
            public long getMvccReadPoint() {
                throw new NotImplementedException();
            }

            @Override
            public boolean nextRaw(List<Cell> result) throws IOException {
                if (iiRowIterator.hasNext()) {
                    IIRow iiRow = iiRowIterator.next();
                    result.addAll(iiRow.makeCells());
                    return true;
                } else {
                    return false;
                }
            }

            @Override
            public boolean nextRaw(List<Cell> result, int limit) throws IOException {
                throw new NotImplementedException();
            }

            @Override
            public boolean next(List<Cell> results) throws IOException {
                throw new NotImplementedException();
            }

            @Override
            public boolean next(List<Cell> result, int limit) throws IOException {
                throw new NotImplementedException();
            }

            @Override
            public void close() throws IOException {
                throw new NotImplementedException();
            }
        }, type, projector, aggregators, filter);

        Assert.assertEquals(2, response.getRowsList().size());
        System.out.println(response.getRowsList().size());
        Set<String> answers = Sets.newHashSet("120.4747", "26.8551");
        for (IIProtos.IIResponseInternal.IIRow responseRow : response.getRowsList()) {
            ByteBuffer bf = responseRow.getMeasures().asReadOnlyByteBuffer();
            List<Object> metrics = aggregators.deserializeMetricValues(bf);
            Assert.assertTrue(answers.contains(metrics.get(0)));
            answers.remove(metrics.get(0));
        }
    }

}
