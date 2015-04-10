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

package org.apache.kylin.storage.hbase.coprocessor.endpoint;

import com.google.common.collect.Lists;
import org.apache.hadoop.io.LongWritable;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.index.TableRecordInfo;
import org.apache.kylin.metadata.measure.MeasureAggregator;
import org.apache.kylin.metadata.measure.fixedlen.FixedLenMeasureCodec;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.hbase.coprocessor.CoprocessorProjector;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Created by Hongbin Ma(Binmahone) on 11/27/14.
 *
 * ii test
 */
@Ignore("need to mock up TableRecordInfo")
public class EndpointAggregationTest extends LocalFileMetadataTestCase {

    @Before
    public void setup() throws IOException {
        this.createTestMetadata();
    }

    @After
    public void cleanUp() {
        cleanupTestMetadata();
    }

    private List<FunctionDesc> buildAggregations() {
        List<FunctionDesc> functions = new ArrayList<FunctionDesc>();

        FunctionDesc f1 = new FunctionDesc();
        f1.setExpression("SUM");
        ParameterDesc p1 = new ParameterDesc();
        p1.setType("column");
        p1.setValue("PRICE");
        f1.setParameter(p1);
        f1.setReturnType("decimal");
        functions.add(f1);

        FunctionDesc f2 = new FunctionDesc();
        f2.setExpression("MIN");
        ParameterDesc p2 = new ParameterDesc();
        p2.setType("column");
        p2.setValue("PRICE");
        f2.setParameter(p2);
        f2.setReturnType("decimal");
        functions.add(f2);

        return functions;
    }

    @Test
    public void testSerializeAggregator() {
        final IIInstance ii = IIManager.getInstance(getTestConfig()).getII("test_kylin_ii_left_join");
        final TableRecordInfo tableRecordInfo = new TableRecordInfo(ii.getFirstSegment());
        final EndpointAggregators endpointAggregators = EndpointAggregators.fromFunctions(tableRecordInfo, buildAggregations());
        byte[] x = EndpointAggregators.serialize(endpointAggregators);
        final EndpointAggregators result = EndpointAggregators.deserialize(x);
        assertArrayEquals(endpointAggregators.dataTypes, result.dataTypes);
        assertArrayEquals(endpointAggregators.funcNames, result.funcNames);
        assertArrayEquals(endpointAggregators.metricValues, result.metricValues);
        assertEquals(endpointAggregators.rawTableRecord.getBytes().length, result.rawTableRecord.getBytes().length);
    }

    private byte[] randomBytes(final int length) {
        byte[] result = new byte[length];
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            random.nextBytes(result);
        }
        return result;
    }

    private List<byte[]> mockData(TableRecordInfo tableRecordInfo) {
        ArrayList<byte[]> result = Lists.newArrayList();
        final int priceColumnIndex = 23;
        final int groupByColumnIndex = 0;
        TblColRef column = tableRecordInfo.getDescriptor().listAllColumns().get(priceColumnIndex);
        FixedLenMeasureCodec codec = FixedLenMeasureCodec.get(column.getType());

        byte[] data = randomBytes(tableRecordInfo.getDigest().getByteFormLen());
        byte[] groupOne = randomBytes(tableRecordInfo.getDigest().length(groupByColumnIndex));
        codec.write(codec.valueOf("199.99"), data, tableRecordInfo.getDigest().offset(priceColumnIndex));
        System.arraycopy(groupOne, 0, data, tableRecordInfo.getDigest().offset(groupByColumnIndex), groupOne.length);
        result.add(data);

        data = randomBytes(tableRecordInfo.getDigest().getByteFormLen());
        codec.write(codec.valueOf("2.09"), data, tableRecordInfo.getDigest().offset(priceColumnIndex));
        System.arraycopy(groupOne, 0, data, tableRecordInfo.getDigest().offset(groupByColumnIndex), groupOne.length);
        result.add(data);

        byte[] groupTwo = randomBytes(tableRecordInfo.getDigest().length(groupByColumnIndex));
        data = randomBytes(tableRecordInfo.getDigest().getByteFormLen());
        System.arraycopy(groupTwo, 0, data, tableRecordInfo.getDigest().offset(groupByColumnIndex), groupTwo.length);
        codec.write(codec.valueOf("100"), data, tableRecordInfo.getDigest().offset(priceColumnIndex));
        result.add(data);

        return result;
    }

    @Test
    public void basicTest() {
        final IIInstance ii = IIManager.getInstance(getTestConfig()).getII("test_kylin_ii_left_join");
        final TableRecordInfo tableRecordInfo = new TableRecordInfo(ii.getFirstSegment());
        final EndpointAggregators aggregators = EndpointAggregators.fromFunctions(tableRecordInfo, buildAggregations());
        final EndpointAggregationCache aggCache = new EndpointAggregationCache(aggregators);
        final Collection<TblColRef> dims = new HashSet<>();
        final TblColRef groupByColumn = ii.getDescriptor().findColumnRef("DEFAULT.TEST_KYLIN_FACT", "LSTG_FORMAT_NAME");
        dims.add(groupByColumn);
        CoprocessorProjector projector = CoprocessorProjector.makeForEndpoint(tableRecordInfo, dims);
        List<byte[]> rawData = mockData(tableRecordInfo);
        for (int i = 0; i < rawData.size(); ++i) {
            byte[] data = rawData.get(i);
            CoprocessorProjector.AggrKey aggKey = projector.getAggrKey(data);
            MeasureAggregator[] bufs = aggCache.getBuffer(aggKey);
            aggregators.aggregate(bufs, data);
            aggCache.checkMemoryUsage();
        }
        long sumTotal = 0;
        long minTotal = 0;
        for (Map.Entry<CoprocessorProjector.AggrKey, MeasureAggregator[]> entry : aggCache.getAllEntries()) {
            sumTotal += ((LongWritable) entry.getValue()[0].getState()).get();
            minTotal += ((LongWritable) entry.getValue()[1].getState()).get();

        }
        assertEquals(3020800, sumTotal);
        assertEquals(1020900, minTotal);
    }

}
