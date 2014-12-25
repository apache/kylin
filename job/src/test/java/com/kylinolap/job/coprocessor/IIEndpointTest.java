package com.kylinolap.job.coprocessor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.kylinolap.common.util.BytesUtil;
import com.kylinolap.common.util.HBaseMetadataTestCase;
import com.kylinolap.invertedindex.IIInstance;
import com.kylinolap.invertedindex.IIManager;
import com.kylinolap.invertedindex.IISegment;
import com.kylinolap.invertedindex.model.IIDesc;
import com.kylinolap.invertedindex.model.IIKeyValueCodec;
import com.kylinolap.job.hadoop.invertedindex.IIBulkLoadJob;
import com.kylinolap.metadata.MetadataManager;
import com.kylinolap.metadata.model.*;
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
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.junit.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;


/**
 * Created by Hongbin Ma(Binmahone) on 11/14/14.
 */
public class IIEndpointTest extends HBaseMetadataTestCase {

    private static final TableName TEST_TABLE = TableName.valueOf("test_III");
    private static final byte[] TEST_COLUMN = Bytes.toBytes("f");

    private static HBaseTestingUtility TEST_UTIL = null;
    private static Configuration CONF = null;
    private static HConnection hconn;

    private static StorageContext context = new StorageContext();

    private IIInstance ii;
    private IISegment seg;
    private TableDesc tableDesc;
    String tableName;

    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        staticCreateTestMetadata(SANDBOX_TEST_DATA);

        TEST_UTIL = new HBaseTestingUtility();
        CONF = TEST_UTIL.getConfiguration();

        //add endpoint coprocessor
        CONF.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
                IIEndpoint.class.getName());

        //create table and bulk load data
        TEST_UTIL.startMiniCluster();

        //simulate bulk load
        mockIIHtable();

        hconn = HConnectionManager.createConnection(CONF);
    }

    @AfterClass
    public static void cleanUpAfterClass() throws Exception {
        staticCleanupTestMetadata();

        hconn.close();
        TEST_UTIL.shutdownMiniCluster();
    }

    private static void mockIIHtable() throws Exception {
        org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(CONF);
        fs.copyFromLocalFile(false, new Path("../examples/test_case_cube/II_hfile/"), new Path("/tmp/test_III_hfile"));

        int sharding = 4;

        HTableDescriptor tableDesc = new HTableDescriptor(TEST_TABLE);
        HColumnDescriptor cf = new HColumnDescriptor(IIDesc.HBASE_FAMILY);
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

        String temp = "-iiname \"test_kylin_ii\"  -input \"/tmp/test_III_hfile\"  -htablename \"test_III\"";
        ToolRunner.run(CONF, new IIBulkLoadJob(), temp.split("\\s+"));
    }

    @Before
    public void setup() throws Exception {

        this.ii = IIManager.getInstance(getTestConfig()).getII("test_kylin_ii");
        this.seg = ii.getFirstSegment();
        this.tableDesc = MetadataManager.getInstance(getTestConfig()).getTableDesc("test_kylin_fact");

        this.tableName = seg.getStorageLocationIdentifier();

    }

    @After
    public void cleanup() throws IOException {
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


    private TupleFilter mockEQFiter(TblColRef tblColRef, String value) throws IOException {
        CompareTupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        filter.addChild(new ColumnTupleFilter(tblColRef));
        filter.addChild(new ConstantTupleFilter(value));
        return filter;
    }

    private TupleFilter mockNEQFiter(TblColRef tblColRef, String value) throws IOException {
        CompareTupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.NEQ);
        filter.addChild(new ColumnTupleFilter(tblColRef));
        filter.addChild(new ConstantTupleFilter(value));
        return filter;
    }

    private TupleFilter mockGTFiter(TblColRef tblColRef, String value) throws IOException {
        CompareTupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.GT);
        filter.addChild(new ColumnTupleFilter(tblColRef));
        filter.addChild(new ConstantTupleFilter(value));
        return filter;
    }

    private List<FunctionDesc> buildAggregations(ColumnDesc priceColumn) {
        List<FunctionDesc> functions = new ArrayList<FunctionDesc>();

        FunctionDesc f1 = new FunctionDesc();
        f1.setExpression("MAX");
        ParameterDesc p1 = new ParameterDesc();
        p1.setType("column");
        p1.setValue("PRICE");
        p1.setColRefs(ImmutableList.of(new TblColRef(priceColumn)));
        f1.setParameter(p1);
        f1.setReturnType("decimal");
        functions.add(f1);

        FunctionDesc f2 = new FunctionDesc();
        f2.setExpression("MIN");
        ParameterDesc p2 = new ParameterDesc();
        p2.setType("column");
        p2.setValue("PRICE");
        p2.setColRefs(ImmutableList.of(new TblColRef(priceColumn)));
        f2.setParameter(p2);
        f2.setReturnType("decimal");
        functions.add(f2);

        FunctionDesc f3 = new FunctionDesc();
        f3.setExpression("COUNT");
        ParameterDesc p3 = new ParameterDesc();
        p3.setType("constant");
        p3.setValue("1");
        f3.setParameter(p3);
        f3.setReturnType("bigint");
        functions.add(f3);

        return functions;
    }


    @Test
    public void testSimpleCount() throws Throwable {
        EndpointTupleIterator iterator = new EndpointTupleIterator(seg, tableDesc.getColumns(), null, null, null, context, hconn);

        int count = 0;
        while (iterator.hasNext()) {
            ITuple tuple = iterator.next();
            System.out.println(tuple);
            count++;
        }
        assertEquals(count, 10000);
        iterator.close();
    }

    private int filteredCount(TupleFilter tupleFilter) throws Throwable {

        EndpointTupleIterator iterator = new EndpointTupleIterator(seg, tableDesc.getColumns(), tupleFilter, null, null, context, hconn);

        int count = 0;
        while (iterator.hasNext()) {
            ITuple tuple = iterator.next();
            System.out.println(tuple);
            count++;
        }

        iterator.close();
        return count;
    }

    @Ignore
    @Test
    public void testFilterOnMetric() throws Throwable {
        ColumnDesc priceDesc = tableDesc.findColumnByName("PRICE");
        TblColRef priceColumn = new TblColRef(priceDesc);
        TupleFilter tupleFilter = mockGTFiter(priceColumn, "50");

        ColumnDesc columnDesc = tableDesc.findColumnByName("LSTG_FORMAT_NAME");
        TblColRef lfn = new TblColRef(columnDesc);
        Collection<TblColRef> groupby = ImmutableSet.of(lfn);

        List<FunctionDesc> measures = buildAggregations(priceDesc);
        EndpointTupleIterator iterator = new EndpointTupleIterator(seg, tableDesc.getColumns(), tupleFilter, groupby, measures, context, hconn);

        int count = 0;
        while (iterator.hasNext()) {
            ITuple tuple = iterator.next();
            count += (Long) tuple.getValue("COUNT__");
        }

        System.out.printf("Count: " + count);
        iterator.close();
    }

    @Test
    public void testCostlyFilter() throws Throwable {
        ColumnDesc sellerDessc = tableDesc.findColumnByName("SELLER_ID");
        TblColRef sellerColumn = new TblColRef(sellerDessc);
        TupleFilter tupleFilter = mockGTFiter(sellerColumn, "10000570");

        TblColRef lfn = new TblColRef(sellerDessc);
        Collection<TblColRef> groupby = ImmutableSet.of(lfn);

        List<FunctionDesc> measures = buildAggregations(sellerDessc);
        EndpointTupleIterator iterator = new EndpointTupleIterator(seg, tableDesc.getColumns(), tupleFilter, groupby, measures, context, hconn);

        int count = 0;
        while (iterator.hasNext()) {
            ITuple tuple = iterator.next();
            count += (Long) tuple.getValue("COUNT__");
        }

        System.out.println("Count: " + count);
        iterator.close();
    }


    @Test
    public void testFilteredCount() throws Throwable {
        ColumnDesc lstgDesc = tableDesc.findColumnByName("LSTG_FORMAT_NAME");
        TblColRef lstg = new TblColRef(lstgDesc);

        TupleFilter filterA = mockEQFiter(lstg, "ABIN");
        TupleFilter filterB = mockNEQFiter(lstg, "ABIN");

        int countA = filteredCount(filterA);
        int countB = filteredCount(filterB);
        assertEquals(countA + countB, 10000);
    }

    private int filteredGrouByCount(TupleFilter tupleFilter) throws Throwable {

        ColumnDesc columnDesc = tableDesc.findColumnByName("LSTG_FORMAT_NAME");
        TblColRef lfn = new TblColRef(columnDesc);
        Collection<TblColRef> groupby = ImmutableSet.of(lfn);

        ColumnDesc priceColumn = tableDesc.findColumnByName("PRICE");
        List<FunctionDesc> measures = buildAggregations(priceColumn);

        EndpointTupleIterator iterator = new EndpointTupleIterator(seg, tableDesc.getColumns(), tupleFilter, groupby, measures, context, hconn);

        int count = 0;
        while (iterator.hasNext()) {
            ITuple tuple = iterator.next();
            count += (Long) tuple.getValue("COUNT__");
        }

        iterator.close();
        return count;
    }

    @Test
    public void testFilterGroupByCount() throws Throwable {


        ColumnDesc columnDesc = tableDesc.findColumnByName("LSTG_FORMAT_NAME");
        TblColRef lfn = new TblColRef(columnDesc);
        TupleFilter filterA = mockEQFiter(lfn, "ABIN");
        TupleFilter filterB = mockNEQFiter(lfn, "ABIN");

        int countA = filteredGrouByCount(filterA);
        int countB = filteredGrouByCount(filterB);
        assertEquals(countA + countB, 10000);
    }
}
