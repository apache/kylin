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

import org.apache.hadoop.io.LongWritable;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.index.TableRecord;
import org.apache.kylin.invertedindex.index.TableRecordInfo;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.measure.MeasureAggregator;
import org.apache.kylin.metadata.model.*;
import org.apache.kylin.storage.hbase.coprocessor.AggrKey;
import org.apache.kylin.storage.hbase.coprocessor.CoprocessorFilter;
import org.apache.kylin.storage.hbase.coprocessor.CoprocessorProjector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 * Created by Hongbin Ma(Binmahone) on 11/27/14.
 *
 * ii test
 */
public class EndpointAggregationTest extends LocalFileMetadataTestCase {
    IIInstance ii;
    TableRecordInfo tableRecordInfo;

    CoprocessorProjector projector;
    EndpointAggregators aggregators;
    CoprocessorFilter filter;

    EndpointAggregationCache aggCache;
    List<TableRecord> tableData;

    TableDesc factTableDesc;

    @Before
    public void setup() throws IOException {
        this.createTestMetadata();
        this.ii = IIManager.getInstance(getTestConfig()).getII("test_kylin_ii");
        this.tableRecordInfo = new TableRecordInfo(ii.getFirstSegment());
        factTableDesc = MetadataManager.getInstance(getTestConfig()).getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        TblColRef formatName = this.ii.getDescriptor().findColumnRef("DEFAULT.TEST_KYLIN_FACT", "LSTG_FORMAT_NAME");
        TblColRef siteId = this.ii.getDescriptor().findColumnRef("DEFAULT.TEST_KYLIN_FACT", "LSTG_SITE_ID");

        Collection<TblColRef> dims = new HashSet<>();
        dims.add(formatName);
        projector = CoprocessorProjector.makeForEndpoint(tableRecordInfo, dims);
        aggregators = EndpointAggregators.fromFunctions(tableRecordInfo, buildAggregations());

        CompareTupleFilter rawFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        rawFilter.addChild(new ColumnTupleFilter(siteId));
        rawFilter.addChild(new ConstantTupleFilter("0"));
        filter = CoprocessorFilter.fromFilter(this.ii.getFirstSegment(), rawFilter);

        aggCache = new EndpointAggregationCache(aggregators);
        tableData = mockTable();
    }

    @After
    public void cleanUp() {
        cleanupTestMetadata();
    }

    private List<TableRecord> mockTable() {

        ColumnDesc[] factTableColumns = factTableDesc.getColumns();
        int[] factTableColumnIndex = new int[factTableColumns.length];
        for (int i = 0; i < factTableColumnIndex.length; ++i) {
            factTableColumnIndex[i] = tableRecordInfo.findColumn(new TblColRef(factTableColumns[i]));
        }

        TableRecord temp1 = tableRecordInfo.createTableRecord();
        temp1.setValueString(factTableColumnIndex[0], "10000000239");
        temp1.setValueString(factTableColumnIndex[1], "2012-03-22");
        temp1.setValueString(factTableColumnIndex[2], "Auction");
        temp1.setValueString(factTableColumnIndex[3], "80135");
        temp1.setValueString(factTableColumnIndex[4], "0");
        temp1.setValueString(factTableColumnIndex[5], "14");
        temp1.setValueString(factTableColumnIndex[6], "199.99");
        temp1.setValueString(factTableColumnIndex[7], "1");
        temp1.setValueString(factTableColumnIndex[8], "10000005");

        TableRecord temp2 = tableRecordInfo.createTableRecord();
        temp2.setValueString(factTableColumnIndex[0], "10000000244");
        temp2.setValueString(factTableColumnIndex[1], "2012-11-11");
        temp2.setValueString(factTableColumnIndex[2], "Auction");
        temp2.setValueString(factTableColumnIndex[3], "16509");
        temp2.setValueString(factTableColumnIndex[4], "101");
        temp2.setValueString(factTableColumnIndex[5], "12");
        temp2.setValueString(factTableColumnIndex[6], "2.09");
        temp2.setValueString(factTableColumnIndex[7], "1");
        temp2.setValueString(factTableColumnIndex[8], "10000004");

        TableRecord temp3 = tableRecordInfo.createTableRecord();
        temp3.setValueString(factTableColumnIndex[0], "10000000259");
        temp3.setValueString(factTableColumnIndex[1], "2012-07-12");
        temp3.setValueString(factTableColumnIndex[2], "Others");
        temp3.setValueString(factTableColumnIndex[3], "15687");
        temp3.setValueString(factTableColumnIndex[4], "0");
        temp3.setValueString(factTableColumnIndex[5], "14");
        temp3.setValueString(factTableColumnIndex[6], "100");
        temp3.setValueString(factTableColumnIndex[7], "1");
        temp3.setValueString(factTableColumnIndex[8], "10000020");

        List<TableRecord> ret = new ArrayList<TableRecord>();
        ret.add(temp1);
        ret.add(temp2);
        ret.add(temp3);
        return ret;
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
    public void testSerializeAggreagtor() {
        EndpointAggregators endpointAggregators = EndpointAggregators.fromFunctions(tableRecordInfo, buildAggregations());
        byte[] x = EndpointAggregators.serialize(endpointAggregators);
        EndpointAggregators.deserialize(x);
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void basicTest() {

        for (int i = 0; i < tableData.size(); ++i) {
            byte[] data = tableData.get(i).getBytes();
            AggrKey aggKey = projector.getAggrKey(data);
            MeasureAggregator[] bufs = aggCache.getBuffer(aggKey);
            aggregators.aggregate(bufs, data);
            aggCache.checkMemoryUsage();
        }

        assertEquals(aggCache.getAllEntries().size(), 2);

        long sumTotal = 0;
        long minTotal = 0;
        for (Map.Entry<AggrKey, MeasureAggregator[]> entry : aggCache.getAllEntries()) {
            sumTotal += ((LongWritable) entry.getValue()[0].getState()).get();
            minTotal += ((LongWritable) entry.getValue()[1].getState()).get();

        }
        assertEquals(3020800, sumTotal);
        assertEquals(1020900, minTotal);

    }

}
