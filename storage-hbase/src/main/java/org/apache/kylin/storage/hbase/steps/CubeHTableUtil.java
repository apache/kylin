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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy;
import org.apache.hadoop.hbase.security.User;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseColumnFamilyDesc;
import org.apache.kylin.metadata.realization.IRealizationConstants;
import org.apache.kylin.storage.hbase.util.DeployCoprocessorCLI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 */
public class CubeHTableUtil {

    private static final Logger logger = LoggerFactory.getLogger(CubeHTableUtil.class);

    public static void createHTable(CubeSegment cubeSegment, byte[][] splitKeys) throws IOException {
        String tableName = cubeSegment.getStorageLocationIdentifier();
        CubeInstance cubeInstance = cubeSegment.getCubeInstance();
        CubeDesc cubeDesc = cubeInstance.getDescriptor();
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(cubeSegment.getStorageLocationIdentifier()));
        // https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/regionserver/ConstantSizeRegionSplitPolicy.html
        tableDesc.setValue(HTableDescriptor.SPLIT_POLICY, DisabledRegionSplitPolicy.class.getName());
        tableDesc.setValue(IRealizationConstants.HTableTag, kylinConfig.getMetadataUrlPrefix());
        tableDesc.setValue(IRealizationConstants.HTableCreationTime, String.valueOf(System.currentTimeMillis()));

        if (!StringUtils.isEmpty(kylinConfig.getKylinOwner())) {
            //HTableOwner is the team that provides kylin service
            tableDesc.setValue(IRealizationConstants.HTableOwner, kylinConfig.getKylinOwner());
        }

        //HTableUser is the cube owner, which will be the "user"
        tableDesc.setValue(IRealizationConstants.HTableUser, cubeInstance.getOwner());

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

                String hbaseDefaultCC = kylinConfig.getHbaseDefaultCompressionCodec().toLowerCase();

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
                default: {
                    logger.info("hbase will not user any compression codec to compress data");
                }
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
