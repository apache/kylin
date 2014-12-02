package com.kylinolap.storage.hbase.coprocessor.endpoint;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.kylinolap.common.util.BytesUtil;
import com.kylinolap.common.util.LocalFileMetadataTestCase;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.invertedindex.TableRecordInfo;
import com.kylinolap.cube.measure.MeasureAggregator;
import com.kylinolap.metadata.model.realization.FunctionDesc;
import com.kylinolap.metadata.model.realization.ParameterDesc;
import com.kylinolap.metadata.model.realization.TblColRef;
import com.kylinolap.storage.filter.ColumnTupleFilter;
import com.kylinolap.storage.filter.CompareTupleFilter;
import com.kylinolap.storage.filter.ConstantTupleFilter;
import com.kylinolap.storage.filter.TupleFilter;
import com.kylinolap.storage.hbase.coprocessor.CoprocessorFilter;
import com.kylinolap.storage.hbase.coprocessor.CoprocessorProjector;

/**
 * Created by Hongbin Ma(Binmahone) on 11/27/14.
 */
@Ignore
public class EndpoindAggregationTest extends LocalFileMetadataTestCase {
    CubeInstance cube;
    TableRecordInfo tableRecordInfo;

    CoprocessorProjector projector;
    EndpointAggregators aggregators;
    CoprocessorFilter filter;

    EndpointAggregationCache aggCache;

    byte[][] data;


    @Before
    public void setup() throws IOException {
        this.createTestMetadata();
        this.cube = CubeManager.getInstance(getTestConfig()).getCube("test_kylin_cube_ii");
        this.tableRecordInfo = new TableRecordInfo(cube.getFirstSegment());
        TblColRef formatName = this.cube.getDescriptor().findColumnRef("test_kylin_fact", "LSTG_FORMAT_NAME");
        TblColRef siteId = this.cube.getDescriptor().findColumnRef("test_kylin_fact", "LSTG_SITE_ID");

        Collection<TblColRef> dims = new HashSet<>();
        dims.add(formatName);
        projector = CoprocessorProjector.makeForEndpoint(tableRecordInfo, dims);
        aggregators = EndpointAggregators.fromFunctions(buildAggregations(), tableRecordInfo);

        CompareTupleFilter rawFilter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        rawFilter.addChild(new ColumnTupleFilter(formatName));
        rawFilter.addChild(new ConstantTupleFilter("Others"));
        filter = CoprocessorFilter.fromFilter(this.cube.getFirstSegment(), rawFilter);

        aggCache = new EndpointAggregationCache(aggregators);

        /**
         [0,2012-10-08,Auction,16145,3,12,85.0913662952116,0,10000036]
         [0,2012-10-08,FP-non GTC,20865,0,-99,44.721564542590485,0,10000328]
         [0,2012-10-08,Others,75665,0,-99,15.583138983388148,0,10000809]
         [0,2012-10-08,Others,2023,0,15,57.67495843017063,0,10000149]
         [0,2012-10-13,FP-GTC,50508,0,5,96.04012848965151,0,10000559]
         */
        String[] dataInHex = new String[] {
                "\\x00\\x0B\\x37\\xAF\\x01\\x21\\x02\\x03\\x0B\\xCF\\x0D\\x77\\xD6\\xC9\\x8F\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x24",
                "\\x00\\x0B\\x37\\xAF\\x03\\x25\\x00\\x00\\x06\\x34\\xD4\\x42\\x93\\x9B\\xAC\\xC0\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x01\\x48",
                "\\x00\\x0B\\x37\\xAF\\x04\\x58\\x00\\x00\\x02\\x29\\x9F\\xD2\\xCB\\xE7\\x67\\x80\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x03\\x29",
                "\\x00\\x0B\\x37\\xAF\\x04\\x0F\\x00\\x06\\x08\\x01\\x06\\xB0\\xF0\\xA8\\x4C\\x80\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x95",
                "\\x00\\x0B\\x37\\xB4\\x02\\x45\\x00\\x01\\x0D\\x54\\x07\\xE1\\x54\\x15\\xE4\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x02\\x2F",
        };
        data = new byte[dataInHex.length][];
        for (int i = 0; i < dataInHex.length; ++i) {
            data[i] = BytesUtil.fromReadableText(dataInHex[i]);
        }
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
        functions.add(f1);

        FunctionDesc f2 = new FunctionDesc();
        f2.setExpression("MIN");
        ParameterDesc p2 = new ParameterDesc();
        p1.setType("column");
        p1.setValue("PRICE");
        f2.setParameter(p2);
        functions.add(f2);

        return functions;
    }

    @Test
    public void basicTest() {

        for (int i = 0; i < data.length; ++i) {
            CoprocessorProjector.AggrKey aggKey = projector.getAggrKey(data[i]);
            MeasureAggregator[] bufs = aggCache.getBuffer(aggKey);
            aggregators.aggregate(bufs, data[i]);
            aggCache.checkMemoryUsage();
        }

        assertEquals(aggCache.getAllEntries().size(), 4);
        for (Map.Entry<CoprocessorProjector.AggrKey, MeasureAggregator[]> entry : aggCache.getAllEntries()) {

        }

    }
}
