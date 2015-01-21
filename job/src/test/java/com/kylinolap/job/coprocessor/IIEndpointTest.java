package com.kylinolap.job.coprocessor;

import org.junit.Ignore;

import com.kylinolap.common.util.HBaseMetadataTestCase;

/**
 * Created by Hongbin Ma(Binmahone) on 11/14/14.
 */
@Ignore("ii not ready")
public class IIEndpointTest extends HBaseMetadataTestCase {
/*
    private static HConnection hconn;

    private static StorageContext context = new StorageContext();

    private IIInstance ii;
    private IISegment seg;
    private TableDesc tableDesc;
    String tableName;

    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        staticCreateTestMetadata(SANDBOX_TEST_DATA);
        hconn = HBaseConnection.get(KylinConfig.getInstanceFromEnv().getStorageUrl());
    }

    @AfterClass
    public static void cleanUpAfterClass() throws Exception {
        staticCleanupTestMetadata();
        hconn.close();
    }

    @Before
    public void setup() throws Exception {

        this.ii = IIManager.getInstance(getTestConfig()).getII("test_kylin_ii");
        this.seg = ii.getFirstSegment();
        this.tableDesc = MetadataManager.getInstance(getTestConfig()).getTableDesc("default.test_kylin_fact");
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
        EndpointTupleIterator iterator = new EndpointTupleIterator(seg, null, null, null, context, hconn);

        int count = 0;
        while (iterator.hasNext()) {
            ITuple tuple = iterator.next();
            System.out.println(tuple);
            count++;
        }
        assertEquals(count, 402);
        iterator.close();
    }

    private int filteredCount(TupleFilter tupleFilter) throws Throwable {

        EndpointTupleIterator iterator = new EndpointTupleIterator(seg, tupleFilter, null, null, context, hconn);

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
        EndpointTupleIterator iterator = new EndpointTupleIterator(seg, tupleFilter, groupby, measures, context, hconn);

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
        EndpointTupleIterator iterator = new EndpointTupleIterator(seg, tupleFilter, groupby, measures, context, hconn);

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
        assertEquals(countA + countB, 402);
    }

    private int filteredGrouByCount(TupleFilter tupleFilter) throws Throwable {

        ColumnDesc columnDesc = tableDesc.findColumnByName("LSTG_FORMAT_NAME");
        TblColRef lfn = new TblColRef(columnDesc);
        Collection<TblColRef> groupby = ImmutableSet.of(lfn);

        ColumnDesc priceColumn = tableDesc.findColumnByName("PRICE");
        List<FunctionDesc> measures = buildAggregations(priceColumn);

        EndpointTupleIterator iterator = new EndpointTupleIterator(seg, tupleFilter, groupby, measures, context, hconn);

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
        assertEquals(countA + countB, 402);
    }
*/
}
