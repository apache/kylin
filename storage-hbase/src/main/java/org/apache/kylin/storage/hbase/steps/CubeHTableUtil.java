/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.storage.hbase.steps;

import java.io.IOException;
import java.util.Locale;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.exceptions.TimeoutIOException;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy;
import org.apache.hadoop.hbase.security.User;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinVersion;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.metadata.realization.IRealizationConstants;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.apache.kylin.storage.hbase.util.DeployCoprocessorCLI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Preconditions;

/**
 */
public class CubeHTableUtil {

    private static final Logger logger = LoggerFactory.getLogger(CubeHTableUtil.class);

    public static void createHTable(CubeSegment cubeSegment, byte[][] splitKeys, boolean continueOnExists)
            throws IOException {
        TableName tableName = TableName.valueOf(cubeSegment.getStorageLocationIdentifier());
        CubeInstance cubeInstance = cubeSegment.getCubeInstance();
        CubeDesc cubeDesc = cubeInstance.getDescriptor();
        KylinConfig kylinConfig = cubeDesc.getConfig();

        HTableDescriptor tableDesc = new HTableDescriptor(tableName);
        tableDesc.setCompactionEnabled(false);
        tableDesc.setValue(HTableDescriptor.SPLIT_POLICY, DisabledRegionSplitPolicy.class.getName());
        tableDesc.setValue(IRealizationConstants.HTableTag, kylinConfig.getMetadataUrlPrefix());
        tableDesc.setValue(IRealizationConstants.HTableCreationTime, String.valueOf(System.currentTimeMillis()));

        if (!StringUtils.isEmpty(kylinConfig.getKylinOwner())) {
            //HTableOwner is the team that provides kylin service
            tableDesc.setValue(IRealizationConstants.HTableOwner, kylinConfig.getKylinOwner());
        }

        String commitInfo = KylinVersion.getGitCommitInfo();
        if (!StringUtils.isEmpty(commitInfo)) {
            tableDesc.setValue(IRealizationConstants.HTableGitTag, commitInfo);
        }

        //HTableUser is the cube owner, which will be the "user"
        tableDesc.setValue(IRealizationConstants.HTableUser, cubeInstance.getOwner());

        tableDesc.setValue(IRealizationConstants.HTableSegmentTag, cubeSegment.toString());

        Configuration conf = HBaseConnection.getCurrentHBaseConfiguration();
        Connection conn = HBaseConnection.get(kylinConfig.getStorageUrl());
        Admin admin = conn.getAdmin();

        try {
            if (User.isHBaseSecurityEnabled(conf)) {
                // add coprocessor for bulk load
                tableDesc.addCoprocessor("org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint");
            }

            for (HBaseColumnFamilyDesc cfDesc : cubeDesc.getHbaseMapping().getColumnFamily()) {
                HColumnDescriptor cf = createColumnFamily(kylinConfig, cfDesc.getName(), cfDesc.isMemoryHungry());
                tableDesc.addFamily(cf);
            }

            if (admin.tableExists(tableName)) {
                if (!continueOnExists) {
                    throw new RuntimeException("HBase table " + tableName.toString() + " exists!");
                } else {
                    logger.warn("HBase table " + tableName + " exists when create HTable, continue the process!");
                    if (admin.isTableEnabled(tableName)) {
                        try {
                            admin.disableTable(tableName);
                            logger.warn("Disabled existing enabled HBase table " + tableName.toString());
                        } catch (TableNotEnabledException e) {
                            logger.warn("HBase table " + tableName + " already disabled.", e);
                        }
                    } else {
                        logger.warn("HBase table exists but in disabled state.");
                    }
                    try {
                        admin.deleteTable(tableName);
                        logger.info("Deleted existing HBase table " + tableName.toString());
                    } catch (TableNotFoundException e) {
                        logger.warn("HBase table " + tableName + " already deleted.", e);
                    }
                }
            }

            DeployCoprocessorCLI.deployCoprocessor(tableDesc);

            try {
                admin.createTable(tableDesc, splitKeys);
            } catch (TableExistsException e) {
                if (admin.isTableEnabled(tableName)) {
                    logger.warn(
                            "Duplicate create table request send to HMaster, ignore it and continue the process, table "
                                    + tableName.toString());
                } else {
                    logger.warn(
                            "Duplicate create table request send to HMaster, ignore it and continue enable the table "
                                    + tableName.toString());
                    admin.enableTable(tableName);
                }
            } catch (TimeoutIOException e) {
                if (admin.isTableEnabled(tableName)) {
                    logger.warn("False alerting?? Detect TimeoutIOException when creating HBase table "
                            + tableName.toString(), e);
                } else {
                    throw e;
                }
            }

            int availableRetry = kylinConfig.getHBaseHTableAvailableRetry();
            while (availableRetry > 0) {
                if (!admin.isTableAvailable(tableName)) {
                    logger.warn("Table created but not available, wait for a while...");
                    try {
                        availableRetry--;
                        Thread.sleep(60000);
                    } catch (InterruptedException e) {
                    }
                } else {
                    break;
                }
            }

            Preconditions.checkArgument(admin.isTableAvailable(tableName), "table " + tableName + " created, but is not available due to some reasons");
            logger.info("create hbase table " + tableName + " done.");
        } finally {
            IOUtils.closeQuietly(admin);
        }

    }

    public static void deleteHTable(TableName tableName) throws IOException {
        Admin admin = HBaseConnection.get(KylinConfig.getInstanceFromEnv().getStorageUrl()).getAdmin();
        try {
            if (admin.tableExists(tableName)) {
                logger.info("disabling hbase table " + tableName);
                admin.disableTable(tableName);
                logger.info("deleting hbase table " + tableName);
                admin.deleteTable(tableName);
            }
        } finally {
            IOUtils.closeQuietly(admin);
        }
    }

    /** create a HTable that has the same performance settings as normal cube table, for benchmark purpose */
    public static void createBenchmarkHTable(TableName tableName, String cfName) throws IOException {
        Admin admin = HBaseConnection.get(KylinConfig.getInstanceFromEnv().getStorageUrl()).getAdmin();
        try {
            if (admin.tableExists(tableName)) {
                logger.info("disabling hbase table " + tableName);
                admin.disableTable(tableName);
                logger.info("deleting hbase table " + tableName);
                admin.deleteTable(tableName);
            }

            HTableDescriptor tableDesc = new HTableDescriptor(tableName);
            tableDesc.setValue(HTableDescriptor.SPLIT_POLICY, DisabledRegionSplitPolicy.class.getName());

            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            tableDesc.addFamily(createColumnFamily(kylinConfig, cfName, false));

            logger.info("creating hbase table " + tableName);
            admin.createTable(tableDesc, null);
            Preconditions.checkArgument(admin.isTableAvailable(tableName), "table " + tableName + " created, but is not available due to some reasons");
            logger.info("create hbase table " + tableName + " done.");
        } finally {
            IOUtils.closeQuietly(admin);
        }
    }

    public static HColumnDescriptor createColumnFamily(KylinConfig kylinConfig, String cfName, boolean isMemoryHungry) {
        HColumnDescriptor cf = new HColumnDescriptor(cfName);
        cf.setMaxVersions(1);

        if (isMemoryHungry) {
            cf.setBlocksize(kylinConfig.getHbaseDefaultBlockSize());
        } else {
            cf.setBlocksize(kylinConfig.getHbaseSmallFamilyBlockSize());
        }

        String hbaseDefaultCC = kylinConfig.getHbaseDefaultCompressionCodec().toLowerCase(Locale.ROOT);
        switch (hbaseDefaultCC) {
        case "snappy": {
            logger.info("hbase will use snappy to compress data");
            cf.setCompressionType(Algorithm.SNAPPY);
            break;
        }
        case "lzo": {
            logger.info("hbase will use lzo to compress data");
            cf.setCompressionType(Algorithm.LZO);
            break;
        }
        case "gz":
        case "gzip": {
            logger.info("hbase will use gzip to compress data");
            cf.setCompressionType(Algorithm.GZ);
            break;
        }
        case "lz4": {
            logger.info("hbase will use lz4 to compress data");
            cf.setCompressionType(Algorithm.LZ4);
            break;
        }
        case "none":
        default: {
            logger.info("hbase will not use any compression algorithm to compress data");
            cf.setCompressionType(Algorithm.NONE);
        }
        }

        try {
            String encodingStr = kylinConfig.getHbaseDefaultEncoding();
            DataBlockEncoding encoding = DataBlockEncoding.valueOf(encodingStr);
            cf.setDataBlockEncoding(encoding);
        } catch (Exception e) {
            logger.info("hbase will not use any encoding", e);
            cf.setDataBlockEncoding(DataBlockEncoding.NONE);
        }

        cf.setInMemory(false);
        cf.setBloomFilterType(BloomType.NONE);
        cf.setScope(kylinConfig.getHBaseReplicationScope());
        return cf;
    }

}
