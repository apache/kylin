package com.kylinolap.storage.hbase.coprocessor;

import com.kylinolap.storage.hbase.coprocessor.example.ExampleEndpoint;
import com.kylinolap.storage.hbase.coprocessor.example.generated.ExampleProtos;
import com.kylinolap.storage.hbase.coprocessor.example.generated.NodeProtos;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

/**
 * Created by honma on 11/3/14.
 */
@Ignore
public class ExampleEndpointTest {
    private static final TableName TEST_TABLE = TableName.valueOf("testrowcounter");
    private static final byte[] TEST_FAMILY = Bytes.toBytes("f");
    private static final byte[] TEST_COLUMN = Bytes.toBytes("col");

    private static HBaseTestingUtility TEST_UTIL = null;
    private static Configuration CONF = null;

    @BeforeClass
    public static void setupBeforeClass() throws Exception {




        TEST_UTIL = new HBaseTestingUtility();
        CONF = TEST_UTIL.getConfiguration();
        CONF.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
                ExampleEndpoint.class.getName());

        TEST_UTIL.startMiniCluster();
        TEST_UTIL.createTable(TEST_TABLE, new byte[][] { TEST_FAMILY });
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        TEST_UTIL.shutdownMiniCluster();
    }

    @Test
    public void testEndpoint() throws Throwable {

        HConnection connection = HConnectionManager.createConnection(CONF);
        HTableInterface table = connection.getTable(TEST_TABLE);

        // insert some test rows
        for (int i = 0; i < 5; i++) {
            byte[] iBytes = Bytes.toBytes(i);
            Put p = new Put(iBytes);
            p.add(TEST_FAMILY, TEST_COLUMN, iBytes);
            table.put(p);
        }

        final ExampleProtos.CountRequest request = ExampleProtos.CountRequest.getDefaultInstance();
        Map<byte[], Long> results = table.coprocessorService(ExampleProtos.RowCountService.class,
                null, null,
                new Batch.Call<ExampleProtos.RowCountService, Long>() {
                    public Long call(ExampleProtos.RowCountService counter) throws IOException {
                        ServerRpcController controller = new ServerRpcController();
                        BlockingRpcCallback<ExampleProtos.CountResponse> rpcCallback =
                                new BlockingRpcCallback<ExampleProtos.CountResponse>();
                        counter.getRowCount(controller, request, rpcCallback);
                        ExampleProtos.CountResponse response = rpcCallback.get();
                        if (controller.failedOnException()) {
                            throw controller.getFailedOn();
                        }
                        return (response != null && response.hasCount()) ? response.getCount() : 0;
                    }
                });
        // should be one region with results
        assertEquals(1, results.size());
        Iterator<Long> iter = results.values().iterator();
        Long val = iter.next();
        assertNotNull(val);
        assertEquals(5l, val.longValue());
    }


}
