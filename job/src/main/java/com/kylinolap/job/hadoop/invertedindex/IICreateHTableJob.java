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
package com.kylinolap.job.hadoop.invertedindex;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.ToolRunner;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.common.util.BytesUtil;
import com.kylinolap.common.util.HadoopUtil;
import com.kylinolap.job.hadoop.AbstractHadoopJob;
import com.kylinolap.metadata.model.invertedindex.InvertedIndexDesc;

/**
 * @author George Song (ysong1)
 * 
 */
public class IICreateHTableJob extends AbstractHadoopJob {

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_CUBE_NAME);
            options.addOption(OPTION_PARTITION_FILE_PATH);
            options.addOption(OPTION_HTABLE_NAME);
            parseOptions(options, args);

            Path partitionFilePath = new Path(getOptionValue(OPTION_PARTITION_FILE_PATH));
            String tableName = getOptionValue(OPTION_HTABLE_NAME);

            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor cf = new HColumnDescriptor(InvertedIndexDesc.HBASE_FAMILY);
            cf.setMaxVersions(1);
            cf.setCompressionType(Algorithm.LZO);
            cf.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
            tableDesc.addFamily(cf);

            Configuration conf = HBaseConfiguration.create(getConf());
            if (User.isHBaseSecurityEnabled(conf)) {
                // add coprocessor for bulk load
                tableDesc.addCoprocessor("org.apache.hadoop.hbase.security.access.SecureBulkLoadEndpoint");
            }

            // drop the table first
            HBaseAdmin admin = new HBaseAdmin(conf);
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }

            // create table
            byte[][] splitKeys = getSplits(conf, partitionFilePath);
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
            e.printStackTrace(System.err);
            log.error(e.getLocalizedMessage(), e);
            return 2;
        }
    }

    public byte[][] getSplits(Configuration conf, Path path) throws Exception {
        List<byte[]> rowkeyList = new ArrayList<byte[]>();
        Reader reader = new Reader(conf, SequenceFile.Reader.file(path));
        Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);

        try {
            while (reader.next(key, value)) {
                byte[] keyBytes = BytesUtil.toBytes(key);
                rowkeyList.add(keyBytes);
                System.out.println("key split: " + Bytes.toStringBinary(keyBytes));
            }
        } finally {
            IOUtils.closeStream(reader);
        }

        System.out.println("Total " + rowkeyList.size() + " split point, " + (rowkeyList.size() + 1)
                + " regions.");

        return rowkeyList.toArray(new byte[rowkeyList.size()][]);
    }

    public static void main(String[] args) throws Exception {
        IICreateHTableJob job = new IICreateHTableJob();
        job.setConf(HadoopUtil.newHBaseConfiguration(KylinConfig.getInstanceFromEnv().getStorageUrl()));
        int exitCode = ToolRunner.run(job, args);
        System.exit(exitCode);
    }
}
