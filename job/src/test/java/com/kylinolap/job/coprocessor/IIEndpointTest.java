package com.kylinolap.job.coprocessor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.kylinolap.common.util.BytesUtil;
import com.kylinolap.common.util.HBaseMetadataTestCase;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.invertedindex.IIKeyValueCodec;
import com.kylinolap.job.hadoop.invertedindex.IIBulkLoadJob;
import com.kylinolap.metadata.MetadataManager;
import com.kylinolap.metadata.model.ColumnDesc;
import com.kylinolap.metadata.model.TableDesc;
import com.kylinolap.metadata.model.invertedindex.InvertedIndexDesc;
import com.kylinolap.metadata.model.realization.FunctionDesc;
import com.kylinolap.metadata.model.realization.ParameterDesc;
import com.kylinolap.metadata.model.realization.TblColRef;
import com.kylinolap.storage.StorageContext;
import com.kylinolap.storage.filter.ColumnTupleFilter;
import com.kylinolap.storage.filter.CompareTupleFilter;
import com.kylinolap.storage.filter.ConstantTupleFilter;
import com.kylinolap.storage.filter.TupleFilter;
import com.kylinolap.storage.hbase.coprocessor.endpoint.EndpointTupleIterator;
import com.kylinolap.storage.hbase.coprocessor.endpoint.IIEndpoint;
import com.kylinolap.storage.tuple.ITuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * Created by Hongbin Ma(Binmahone) on 11/14/14.
 */
public class IIEndpointTest extends HBaseMetadataTestCase {

    private static final TableName TEST_TABLE = TableName.valueOf("test_III");
    private static final byte[] TEST_COLUMN = Bytes.toBytes("f");

    private static HBaseTestingUtility TEST_UTIL = null;
    private static Configuration CONF = null;
    private static HConnection hconn;

    private CubeInstance cube;
    private CubeSegment seg;

    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        TEST_UTIL = new HBaseTestingUtility();
        CONF = TEST_UTIL.getConfiguration();

        //add endpoint coprocessor
        CONF.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
                IIEndpoint.class.getName());

        //create table and bulk load data
        TEST_UTIL.startMiniCluster();

        //simulate bulk load
        mockCubeHtable();

        hconn = HConnectionManager.createConnection(CONF);
    }

    private static void mockCubeHtable() throws Exception {
        org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(CONF);
        fs.copyFromLocalFile(false, new Path("../examples/test_case_cube/II_hfile/"), new Path("/tmp/test_III_hfile"));

        int sharding = 4;

        HTableDescriptor tableDesc = new HTableDescriptor(TEST_TABLE);
        HColumnDescriptor cf = new HColumnDescriptor(InvertedIndexDesc.HBASE_FAMILY);
        cf.setMaxVersions(1);
        tableDesc.addFamily(cf);

        if (User.isHBaseSecurityEnabled(CONF)) {
            // add coprocessor for bulk load
            tableDesc.addCoprocessor("org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint");
        }

        byte[][] splitKeys = getSplits(sharding);
        if (splitKeys.length == 0)
            splitKeys = null;

        TEST_UTIL.createTable(tableDesc.getTableName(), TEST_COLUMN, splitKeys);

        HBaseAdmin hBaseAdmin = new HBaseAdmin(CONF);
        TableName[] tables = hBaseAdmin.listTableNames();

        String temp = "-cubename \"test_kylin_cube_ii\"  -input \"/tmp/test_III_hfile\"  -htablename \"test_III\"";
        ToolRunner.run(CONF, new IIBulkLoadJob(), temp.split("\\s+"));
    }

    @Before
    public void setup() throws Exception {
        this.createTestMetadata();
        this.cube = CubeManager.getInstance(getTestConfig()).getCube("test_kylin_cube_ii");
        this.seg = cube.getFirstSegment();
    }


    //one region for one shard
    private static byte[][] getSplits(int shard) {
        byte[][] result = new byte[shard - 1][];
        for (int i = 1; i < shard; ++i) {
            byte[] split = new byte[IIKeyValueCodec.SHARD_LEN];
            BytesUtil.writeUnsigned(i, split, 0, IIKeyValueCodec.SHARD_LEN);
            result[i - 1] = split;
        }
        return result;
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        TEST_UTIL.shutdownMiniCluster();
    }


    private TupleFilter mockEQFiter(String value, TblColRef tblColRef) throws IOException {
        CompareTupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        filter.addChild(new ColumnTupleFilter(tblColRef));
        filter.addChild(new ConstantTupleFilter(value));
        return filter;
    }

    private TupleFilter mockNEQFiter(String value, TblColRef tblColRef) throws IOException {
        CompareTupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.NEQ);
        filter.addChild(new ColumnTupleFilter(tblColRef));
        filter.addChild(new ConstantTupleFilter(value));
        return filter;
    }

    private List<FunctionDesc> buildAggregations(ColumnDesc columnDesc) {
        List<FunctionDesc> functions = new ArrayList<FunctionDesc>();

        FunctionDesc f1 = new FunctionDesc();
        f1.setExpression("SUM");
        ParameterDesc p1 = new ParameterDesc();
        p1.setType("column");
        p1.setValue("PRICE");
        p1.setColRefs(ImmutableList.of(new TblColRef(columnDesc)));
        f1.setParameter(p1);
        f1.setReturnType("decimal");
        functions.add(f1);

        FunctionDesc f2 = new FunctionDesc();
        f2.setExpression("MIN");
        ParameterDesc p2 = new ParameterDesc();
        p2.setType("column");
        p2.setValue("PRICE");
        p2.setColRefs(ImmutableList.of(new TblColRef(columnDesc)));
        f2.setParameter(p2);
        f2.setReturnType("decimal");

        functions.add(f2);

        return functions;
    }

    @Test
    public void testEndpoint() throws Throwable {

        String tableName = seg.getStorageLocationIdentifier();
        HTableInterface table = hconn.getTable(tableName);

        TableDesc tableDesc = MetadataManager.getInstance(getTestConfig()).getTableDesc("test_kylin_fact");
        ColumnDesc columnDesc = tableDesc.findColumnByName("LSTG_FORMAT_NAME");
        TblColRef lfn = new TblColRef(columnDesc);
        TupleFilter filterA = mockEQFiter("ABIN", lfn);
        TupleFilter filterB = mockNEQFiter("ABIN", lfn);

        ColumnDesc priceColumn = tableDesc.findColumnByName("PRICE");
        List<FunctionDesc> measures = buildAggregations(priceColumn);
        Collection<TblColRef> groupby = ImmutableSet.of(lfn);
        StorageContext context = new StorageContext();
        EndpointTupleIterator iterator = new EndpointTupleIterator(seg, tableDesc, filterA, groupby, measures, context, table);

        int counterA = 0;
        while (iterator.hasNext()) {
            ITuple tuple = iterator.next();
            System.out.println(tuple);
            counterA++;
        }
        System.out.println(counterA);


//        IIProtos.IIRequest requestA = prepareRequest(filterA);
//        IIProtos.IIRequest requestB = prepareRequest(filterB);
//
//        int resultA = getResults(requestA, table).size();
//        int resultB = getResults(requestB, table).size();
//
//
//        assertEquals(resultA + resultB, 10000);
    }
}
