package com.kylinolap.job.coprocessor;

import com.kylinolap.common.util.BytesUtil;
import com.kylinolap.cube.invertedindex.IIKeyValueCodec;
import com.kylinolap.job.hadoop.invertedindex.IICreateHTableJob;
import com.kylinolap.metadata.model.invertedindex.InvertedIndexDesc;
import com.kylinolap.storage.hbase.coprocessor.IIEndpoint;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * Created by Hongbin Ma(Binmahone) on 11/14/14.
 */
public class IIEndpointTest {

    private static final TableName TEST_TABLE = TableName.valueOf("II_cube_htable");
    private static final byte[] TEST_COLUMN = Bytes.toBytes("col");

    private static HBaseTestingUtility TEST_UTIL = null;
    private static Configuration CONF = null;

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

        TEST_UTIL.createTable(tableDesc, splitKeys, CONF);
        HBaseAdmin hBaseAdmin = new HBaseAdmin(CONF);
        hBaseAdmin.listTableNames();
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

    }
}
