package com.kylinolap.job.coprocessor;

import com.google.protobuf.ByteString;
import com.kylinolap.common.util.BytesUtil;
import com.kylinolap.common.util.HBaseMetadataTestCase;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.cube.CubeSegment;
import com.kylinolap.cube.invertedindex.IIKeyValueCodec;
import com.kylinolap.cube.invertedindex.TableRecord;
import com.kylinolap.cube.invertedindex.TableRecordInfo;
import com.kylinolap.cube.invertedindex.TableRecordInfoDigest;
import com.kylinolap.job.hadoop.invertedindex.IIBulkLoadJob;
import com.kylinolap.metadata.model.invertedindex.InvertedIndexDesc;
import com.kylinolap.storage.hbase.endpoint.IIEndpoint;
import com.kylinolap.storage.hbase.endpoint.generated.IIProtos;
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
        //TEST_UTIL.createTable(TEST_TABLE, new byte[][] { TEST_FAMILY });

        org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(CONF);
        fs.copyFromLocalFile(false, new Path("../examples/test_case_cube/II_hfile/"), new Path("/tmp/test_III_hfile"));


//        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/tmp/test_III_hfile"), true);
//        while (iterator.hasNext()) {
//            LocatedFileStatus a = iterator.next();
//            System.out.println(a.getPath());
//        }


        int sharding = 4;

        HTableDescriptor tableDesc = new HTableDescriptor(TEST_TABLE);
        HColumnDescriptor cf = new HColumnDescriptor(InvertedIndexDesc.HBASE_FAMILY);
        cf.setMaxVersions(1);
        //cf.setCompressionType(Compression.Algorithm.LZO);
        //cf.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
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
        //String temp = "-cubename \"test_kylin_cube_ii\"    -htablename \"test_III\"";

        hconn = HConnectionManager.createConnection(CONF);
//        HTableInterface hTableInterface = conn.getTable("tt");
//        ResultScanner scanner = hTableInterface.getScanner(new Scan());
//        Iterator<Result> resultIterator = scanner.iterator();
//        int count = 0;
//        while (resultIterator.hasNext()) {
//            resultIterator.next();
//            count++;
//        }
//
//        System.out.println(count);


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

    @Test
    public void testEndpoint() throws Throwable {
        String tableName = seg.getStorageLocationIdentifier();
        HTableInterface table = hconn.getTable(tableName);
        final TableRecordInfoDigest recordInfo = new TableRecordInfo(seg);
        final IIProtos.IIRequest request = IIProtos.IIRequest.newBuilder().setTableInfo(
                ByteString.copyFrom(TableRecordInfoDigest.serialize(recordInfo))).build();

        Map<byte[], List<TableRecord>> results = table.coprocessorService(IIProtos.RowsService.class,
                null, null,
                new Batch.Call<IIProtos.RowsService, List<TableRecord>>() {
                    public List<TableRecord> call(IIProtos.RowsService counter) throws IOException {
                        ServerRpcController controller = new ServerRpcController();
                        BlockingRpcCallback<IIProtos.IIResponse> rpcCallback =
                                new BlockingRpcCallback<IIProtos.IIResponse>();
                        counter.getRows(controller, request, rpcCallback);
                        IIProtos.IIResponse response = rpcCallback.get();
                        if (controller.failedOnException()) {
                            throw controller.getFailedOn();
                        }

                        List<TableRecord> records = new ArrayList<TableRecord>();
                        for (ByteString raw : response.getRowsList()) {
                            TableRecord record = new TableRecord(recordInfo);
                            record.setBytes(raw.toByteArray(), 0, raw.size());
                            records.add(record);
                        }
                        return records;
                    }
                });

        for (Map.Entry<byte[], List<TableRecord>> entry : results.entrySet()) {
            System.out.println("result count : " + entry.getValue());
        }
    }
}
