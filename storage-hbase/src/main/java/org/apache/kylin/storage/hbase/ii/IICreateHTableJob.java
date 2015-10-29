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

package org.apache.kylin.storage.hbase.ii;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy;
import org.apache.hadoop.hbase.security.User;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.storage.hbase.util.IIDeployCoprocessorCLI;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.invertedindex.model.IIKeyValueCodec;
import org.apache.kylin.metadata.realization.IRealizationConstants;

/**
 * @author George Song (ysong1)
 */
public class IICreateHTableJob extends AbstractHadoopJob {

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_II_NAME);
            options.addOption(OPTION_HTABLE_NAME);
            parseOptions(options, args);

            String tableName = getOptionValue(OPTION_HTABLE_NAME);
            String iiName = getOptionValue(OPTION_II_NAME);

            KylinConfig config = KylinConfig.getInstanceFromEnv();
            IIManager iiManager = IIManager.getInstance(config);
            IIInstance ii = iiManager.getII(iiName);
            int sharding = ii.getDescriptor().getSharding();

            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor cf = new HColumnDescriptor(IIDesc.HBASE_FAMILY);
            cf.setMaxVersions(1);

            String hbaseDefaultCC = config.getHbaseDefaultCompressionCodec().toLowerCase();

            switch (hbaseDefaultCC) {
            case "snappy": {
                logger.info("hbase will use snappy to compress data");
                cf.setCompressionType(Compression.Algorithm.SNAPPY);
                break;
            }
            case "lzo": {
                logger.info("hbase will use lzo to compress data");
                cf.setCompressionType(Compression.Algorithm.LZO);
                break;
            }
            case "gz":
            case "gzip": {
                logger.info("hbase will use gzip to compress data");
                cf.setCompressionType(Compression.Algorithm.GZ);
                break;
            }
            case "lz4": {
                logger.info("hbase will use lz4 to compress data");
                cf.setCompressionType(Compression.Algorithm.LZ4);
                break;
            }
            default: {
                logger.info("hbase will not user any compression codec to compress data");
            }
            }

            cf.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
            tableDesc.addFamily(cf);
            tableDesc.setValue(IRealizationConstants.HTableTag, config.getMetadataUrlPrefix());
            tableDesc.setValue(IRealizationConstants.HTableCreationTime, String.valueOf(System.currentTimeMillis()));
            tableDesc.setValue(HTableDescriptor.SPLIT_POLICY, DisabledRegionSplitPolicy.class.getName());

            Configuration conf = HBaseConfiguration.create(getConf());
            if (User.isHBaseSecurityEnabled(conf)) {
                // add coprocessor for bulk load
                tableDesc.addCoprocessor("org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint");
            }

            IIDeployCoprocessorCLI.deployCoprocessor(tableDesc);

            // drop the table first
            HBaseAdmin admin = new HBaseAdmin(conf);
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }

            // create table
            byte[][] splitKeys = getSplits(sharding);
            if (splitKeys.length == 0)
                splitKeys = null;
            admin.createTable(tableDesc, splitKeys);
            if (splitKeys != null) {
                for (int i = 0; i < splitKeys.length; i++) {
                    System.out.println("split key " + i + ": " + BytesUtil.toHex(splitKeys[i]));
                }
            }
            System.out.println("create hbase table " + tableName + " done.");
            admin.close();

            return 0;
        } catch (Exception e) {
            printUsage(options);
            throw e;
        }
    }

    //one region for one shard
    private byte[][] getSplits(int shard) {
        byte[][] result = new byte[shard - 1][];
        for (int i = 1; i < shard; ++i) {
            byte[] split = new byte[IIKeyValueCodec.SHARD_LEN];
            BytesUtil.writeUnsigned(i, split, 0, IIKeyValueCodec.SHARD_LEN);
            result[i - 1] = split;
        }
        return result;
    }

}
