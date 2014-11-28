package com.kylinolap.job.coprocessor;

import com.google.protobuf.ByteString;
import com.kylinolap.common.util.BytesUtil;
import com.kylinolap.common.util.HBaseMetadataTestCase;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.cuboid.Cuboid;
import com.kylinolap.cube.invertedindex.IIKeyValueCodec;
import com.kylinolap.cube.invertedindex.TableRecord;
import com.kylinolap.cube.invertedindex.TableRecordInfo;
import com.kylinolap.cube.invertedindex.TableRecordInfoDigest;
import com.kylinolap.job.hadoop.invertedindex.IIBulkLoadJob;
import com.kylinolap.metadata.MetadataManager;
import com.kylinolap.metadata.model.ColumnDesc;
import com.kylinolap.metadata.model.TableDesc;
import com.kylinolap.metadata.model.invertedindex.InvertedIndexDesc;
import com.kylinolap.metadata.model.realization.TblColRef;
import com.kylinolap.storage.filter.ColumnTupleFilter;
import com.kylinolap.storage.filter.CompareTupleFilter;
import com.kylinolap.storage.filter.ConstantTupleFilter;
import com.kylinolap.storage.filter.TupleFilter;
import com.kylinolap.storage.hbase.coprocessor.endpoint.IIEndpoint;
import com.kylinolap.storage.hbase.coprocessor.endpoint.generated.IIProtos;
import com.kylinolap.storage.hbase.coprocessor.CoprocessorFilter;
import com.kylinolap.storage.hbase.coprocessor.observer.ObserverRowType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    private IIProtos.IIRequest prepareRequest(TupleFilter rootFilter) throws IOException {

        long baseCuboidId = Cuboid.getBaseCuboidId(this.cube.getDescriptor());
        ObserverRowType type = ObserverRowType.fromCuboid(this.seg, Cuboid.findById(cube.getDescriptor(), baseCuboidId));

        CoprocessorFilter filter = CoprocessorFilter.fromFilter(this.seg, rootFilter);

        //SRowProjector projector = SRowProjector.makeForObserver(segment, cuboid, groupBy);
        //SRowAggregators aggrs = SRowAggregators.fromValueDecoders(rowValueDecoders);


        TableRecordInfoDigest recordInfo = new TableRecordInfo(seg);
        IIProtos.IIRequest request = IIProtos.IIRequest.newBuilder().
                setTableInfo(ByteString.copyFrom(TableRecordInfoDigest.serialize(recordInfo))).
                setSRowType(ByteString.copyFrom(ObserverRowType.serialize(type))).
                setSRowFilter(ByteString.copyFrom(CoprocessorFilter.serialize(filter))).
                build();

        return request;
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

    private List<TableRecord> getResults(final IIProtos.IIRequest request, HTableInterface table) throws Throwable {
        Map<byte[], List<TableRecord>> results = table.coprocessorService(IIProtos.RowsService.class,
                null, null,
                new Batch.Call<IIProtos.RowsService, List<TableRecord>>() {
                    public List<TableRecord> call(IIProtos.RowsService counter) throws IOException {
                        ServerRpcController controller = new ServerRpcController();
                        BlockingRpcCallback<IIProtos.IIResponse> rpcCallback =
                                new BlockingRpcCallback<>();
                        counter.getRows(controller, request, rpcCallback);
                        IIProtos.IIResponse response = rpcCallback.get();
                        if (controller.failedOnException()) {
                            throw controller.getFailedOn();
                        }

                        List<TableRecord> records = new ArrayList<>();
                        TableRecordInfoDigest recordInfo = new TableRecordInfo(seg);
                        for (ByteString raw : response.getRowsList()) {
                            TableRecord record = new TableRecord(recordInfo);
                            record.setBytes(raw.toByteArray(), 0, raw.size());
                            System.out.println(record);
                            records.add(record);
                        }
                        return records;
                    }
                });

        List<TableRecord> ret = new ArrayList<TableRecord>();
        for (Map.Entry<byte[], List<TableRecord>> entry : results.entrySet()) {
            System.out.println("result count : " + entry.getValue().size());
            ret.addAll(entry.getValue());
        }
        return ret;
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

        IIProtos.IIRequest requestA = prepareRequest(filterA);
        IIProtos.IIRequest requestB = prepareRequest(filterB);

        int resultA = getResults(requestA, table).size();
        int resultB = getResults(requestB, table).size();


        assertEquals(resultA + resultB, 10000);
    }
}
