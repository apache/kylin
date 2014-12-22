package com.kylinolap.storage.hbase.coprocessor.endpoint;

import static org.junit.Assert.assertEquals;

import com.kylinolap.common.util.LocalFileMetadataTestCase;
import com.kylinolap.invertedindex.IIInstance;
import com.kylinolap.invertedindex.IIManager;
import com.kylinolap.invertedindex.index.TableRecord;
import com.kylinolap.invertedindex.index.TableRecordInfo;
import com.kylinolap.metadata.measure.MeasureAggregator;
import com.kylinolap.metadata.model.FunctionDesc;
import com.kylinolap.metadata.model.ParameterDesc;
import com.kylinolap.metadata.model.TblColRef;
import com.kylinolap.storage.filter.ColumnTupleFilter;
import com.kylinolap.storage.filter.CompareTupleFilter;
import com.kylinolap.storage.filter.ConstantTupleFilter;
import com.kylinolap.storage.filter.TupleFilter;
import com.kylinolap.storage.hbase.coprocessor.CoprocessorFilter;
import com.kylinolap.storage.hbase.coprocessor.CoprocessorProjector;
import org.apache.hadoop.io.LongWritable;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

/**
 * Created by Hongbin Ma(Binmahone) on 11/27/14.
 */
@Ignore
public class EndpoindAggregationTest extends LocalFileMetadataTestCase {
    IIInstance ii;
    TableRecordInfo tableRecordInfo;

    CoprocessorProjector projector;
    EndpointAggregators aggregators;
    CoprocessorFilter filter;

    EndpointAggregationCache aggCache;

    List<TableRecord> tableData;


    @Before
    public void setup() throws IOException {
        this.createTestMetadata();
        this.ii = IIManager.getInstance(getTestConfig()).getII("test_kylin_ii");
        this.tableRecordInfo = new TableRecordInfo(ii.getFirstSegment());
        TblColRef formatName = this.ii.getDescriptor().findColumnRef("TEST_KYLIN_FACT", "LSTG_FORMAT_NAME");
        TblColRef siteId = this.ii.getDescriptor().findColumnRef("TEST_KYLIN_FACT", "LSTG_SITE_ID");

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

        TableRecord temp1 = (TableRecord) tableRecordInfo.createTableRecord();
        temp1.setValueString(0, "10000000252");
        temp1.setValueString(1, "2012-03-22");
        temp1.setValueString(2, "Auction");
        temp1.setValueString(3, "80135");
        temp1.setValueString(4, "0");
        temp1.setValueString(5, "14");
        temp1.setValueString(6, "199.99");
        temp1.setValueString(7, "1");
        temp1.setValueString(8, "10000005");

        TableRecord temp2 = (TableRecord) tableRecordInfo.createTableRecord();
        temp2.setValueString(0, "10000000242");
        temp2.setValueString(1, "2012-11-11");
        temp2.setValueString(2, "Auction");
        temp2.setValueString(3, "16509");
        temp2.setValueString(4, "101");
        temp2.setValueString(5, "12");
        temp2.setValueString(6, "2.09");
        temp2.setValueString(7, "1");
        temp2.setValueString(8, "10000004");

        TableRecord temp3 = (TableRecord) tableRecordInfo.createTableRecord();
        temp3.setValueString(0, "10000000258");
        temp3.setValueString(1, "2012-07-12");
        temp3.setValueString(2, "Others");
        temp3.setValueString(3, "15687");
        temp3.setValueString(4, "0");
        temp3.setValueString(5, "14");
        temp3.setValueString(6, "100");
        temp3.setValueString(7, "1");
        temp3.setValueString(8, "10000020");

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
        EndpointAggregators d = EndpointAggregators.deserialize(x);
    }

    @Test
    public void basicTest() {

        for (int i = 0; i < tableData.size(); ++i) {
            byte[] data = tableData.get(i).getBytes();
            CoprocessorProjector.AggrKey aggKey = projector.getAggrKey(data);
            MeasureAggregator[] bufs = aggCache.getBuffer(aggKey);
            aggregators.aggregate(bufs, data);
            aggCache.checkMemoryUsage();
        }

        assertEquals(aggCache.getAllEntries().size(), 2);

        long sumTotal = 0;
        long minTotal = 0;
        for (Map.Entry<CoprocessorProjector.AggrKey, MeasureAggregator[]> entry : aggCache.getAllEntries()) {
            sumTotal += ((LongWritable) entry.getValue()[0].getState()).get();
            minTotal += ((LongWritable) entry.getValue()[1].getState()).get();

        }
        assertEquals(3020800,sumTotal);
        assertEquals(1020900,minTotal);

    }

}
