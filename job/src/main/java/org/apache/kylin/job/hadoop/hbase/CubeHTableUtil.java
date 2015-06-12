package org.apache.kylin.job.hadoop.hbase;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.security.User;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.job.tools.DeployCoprocessorCLI;
import org.apache.kylin.job.tools.LZOSupportnessChecker;
import org.apache.kylin.metadata.realization.IRealizationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 */
public class CubeHTableUtil {

    private static final Logger logger = LoggerFactory.getLogger(CubeHTableUtil.class);

    public static void createHTable(CubeDesc cubeDesc, String tableName, byte[][] splitKeys) throws IOException {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
        // https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/regionserver/ConstantSizeRegionSplitPolicy.html
        tableDesc.setValue(HTableDescriptor.SPLIT_POLICY, ConstantSizeRegionSplitPolicy.class.getName());
        tableDesc.setValue(IRealizationConstants.HTableTag, kylinConfig.getMetadataUrlPrefix());

        Configuration conf = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(conf);

        try {
            if (User.isHBaseSecurityEnabled(conf)) {
                // add coprocessor for bulk load
                tableDesc.addCoprocessor("org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint");
            }

            for (HBaseColumnFamilyDesc cfDesc : cubeDesc.getHBaseMapping().getColumnFamily()) {
                HColumnDescriptor cf = new HColumnDescriptor(cfDesc.getName());
                cf.setMaxVersions(1);

                if (LZOSupportnessChecker.getSupportness()) {
                    logger.info("hbase will use lzo to compress cube data");
                    cf.setCompressionType(Compression.Algorithm.LZO);
                } else {
                    logger.info("hbase will not use lzo to compress cube data");
                }

                cf.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
                cf.setInMemory(false);
                cf.setBlocksize(4 * 1024 * 1024); // set to 4MB
                tableDesc.addFamily(cf);
            }

            if (admin.tableExists(tableName)) {
                // admin.disableTable(tableName);
                // admin.deleteTable(tableName);
                throw new RuntimeException("HBase table " + tableName + " exists!");
            }

            DeployCoprocessorCLI.deployCoprocessor(tableDesc);

            admin.createTable(tableDesc, splitKeys);
            Preconditions.checkArgument(admin.isTableAvailable(tableName), "table " + tableName + " created, but is not available due to some reasons");
            logger.info("create hbase table " + tableName + " done.");
        } catch (Exception e) {
            logger.error("Failed to create HTable", e);
            throw e;
        } finally {
            admin.close();
        }

    }
}
