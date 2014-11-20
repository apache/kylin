/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kylinolap.job.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.ConstantSizeRegionSplitPolicy;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.HadoopUtil;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.job.hadoop.AbstractHadoopJob;
import com.kylinolap.job.tools.DeployCoprocessorCLI;
import com.kylinolap.job.tools.LZOSupportnessChecker;
import com.kylinolap.metadata.model.cube.CubeDesc;
import com.kylinolap.metadata.model.cube.HBaseColumnFamilyDesc;

/**
 * @author George Song (ysong1)
 */

public class CreateHTableJob extends AbstractHadoopJob {

    protected static final Logger log = LoggerFactory.getLogger(CreateHTableJob.class);

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_PARTITION_FILE_PATH);
        options.addOption(OPTION_HTABLE_NAME);
        parseOptions(options, args);

        Path partitionFilePath = new Path(getOptionValue(OPTION_PARTITION_FILE_PATH));

        String cubeName = getOptionValue(OPTION_CUBE_NAME).toUpperCase();
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        CubeManager cubeMgr = CubeManager.getInstance(config);
        CubeInstance cube = cubeMgr.getCube(cubeName);
        CubeDesc cubeDesc = cube.getDescriptor();

        String tableName = getOptionValue(OPTION_HTABLE_NAME).toUpperCase();
        HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
        // https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/regionserver/ConstantSizeRegionSplitPolicy.html
        tableDesc.setValue(HTableDescriptor.SPLIT_POLICY, ConstantSizeRegionSplitPolicy.class.getName());
        tableDesc.setValue(CubeManager.getHtableMetadataKey(),config.getMetadataUrlPrefix());

        Configuration conf = HBaseConfiguration.create(getConf());
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
                    log.info("hbase will use lzo to compress data");
                    cf.setCompressionType(Algorithm.LZO);
                } else {
                    log.info("hbase will not use lzo to compress data");
                }

                cf.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
                cf.setInMemory(false);
                cf.setBlocksize(4 * 1024 * 1024); // set to 4MB
                tableDesc.addFamily(cf);
            }

            byte[][] splitKeys = getSplits(conf, partitionFilePath);

            if (admin.tableExists(tableName)) {
                // admin.disableTable(tableName);
                // admin.deleteTable(tableName);
                throw new RuntimeException("HBase table " + tableName + " exists!");
            }

            try {
                initHTableCoprocessor(tableDesc);
                log.info("hbase table " + tableName + " deployed with coprocessor.");

            } catch (Exception ex) {
                log.error("Error deploying coprocessor on " + tableName, ex);
                log.error("Will try creating the table without coprocessor.");
            }

            admin.createTable(tableDesc, splitKeys);
            log.info("create hbase table " + tableName + " done.");

            return 0;
        } catch (Exception e) {
            printUsage(options);
            e.printStackTrace(System.err);
            log.error(e.getLocalizedMessage(), e);
            return 2;
        } finally {
            admin.close();
        }
    }

    private void initHTableCoprocessor(HTableDescriptor desc) throws IOException {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        Configuration hconf = HadoopUtil.getDefaultConfiguration();
        FileSystem fileSystem = FileSystem.get(hconf);

        String localCoprocessorJar = kylinConfig.getCoprocessorLocalJar();
        Path hdfsCoprocessorJar = DeployCoprocessorCLI.uploadCoprocessorJar(localCoprocessorJar, fileSystem, null);

        DeployCoprocessorCLI.setCoprocessorOnHTable(desc, hdfsCoprocessorJar);
    }

    @SuppressWarnings("deprecation")
    public byte[][] getSplits(Configuration conf, Path path) throws Exception {
        List<byte[]> rowkeyList = new ArrayList<byte[]>();
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(path.getFileSystem(conf), path, conf);
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            while (reader.next(key, value)) {
                byte[] tmp = ((Text) key).copyBytes();
                if (rowkeyList.contains(tmp) == false) {
                    rowkeyList.add(tmp);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        } finally {
            IOUtils.closeStream(reader);
        }

        byte[][] retValue = rowkeyList.toArray(new byte[rowkeyList.size()][]);
        if (retValue.length == 0) {
            throw new IllegalStateException("Split number is 0, no records in cube??");
        }

        return retValue;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CreateHTableJob(), args);
        System.exit(exitCode);
    }
}
