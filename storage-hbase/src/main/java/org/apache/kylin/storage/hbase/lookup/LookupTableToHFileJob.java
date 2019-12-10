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

package org.apache.kylin.storage.hbase.lookup;

import java.io.IOException;
import java.util.Locale;
import java.util.Random;

import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinVersion;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.kv.RowConstants;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.dict.lookup.ExtTableSnapshotInfo;
import org.apache.kylin.dict.lookup.ExtTableSnapshotInfoManager;
import org.apache.kylin.engine.mr.IMRInput.IMRTableInputFormat;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.IRealizationConstants;
import org.apache.kylin.source.IReadableTable;
import org.apache.kylin.source.IReadableTable.TableSignature;
import org.apache.kylin.source.SourceManager;
import org.apache.kylin.storage.hbase.HBaseConnection;
import org.apache.kylin.storage.hbase.steps.CubeHTableUtil;
import org.apache.kylin.storage.hbase.steps.HFileOutputFormat3;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LookupTableToHFileJob extends AbstractHadoopJob {
    protected static final Logger logger = LoggerFactory.getLogger(LookupTableToHFileJob.class);

    private static String ALPHA_NUM = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final Random ran = new Random();
    private static int HBASE_TABLE_LENGTH = 10;

    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_JOB_NAME);
            options.addOption(OPTION_CUBE_NAME);
            options.addOption(OPTION_OUTPUT_PATH);
            options.addOption(OPTION_TABLE_NAME);
            options.addOption(OPTION_CUBING_JOB_ID);
            options.addOption(OPTION_LOOKUP_SNAPSHOT_ID);
            parseOptions(options, args);

            Path output = new Path(getOptionValue(OPTION_OUTPUT_PATH));
            String cubeName = getOptionValue(OPTION_CUBE_NAME).toUpperCase(Locale.ROOT);
            String tableName = getOptionValue(OPTION_TABLE_NAME);
            String lookupSnapshotID = getOptionValue(OPTION_LOOKUP_SNAPSHOT_ID);
            String jobId = getOptionValue(OPTION_CUBING_JOB_ID);

            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            CubeManager cubeMgr = CubeManager.getInstance(kylinConfig);
            CubeInstance cube = cubeMgr.getCube(cubeName);

            TableDesc tableDesc = TableMetadataManager.getInstance(kylinConfig).getTableDesc(tableName,
                    cube.getProject());

            ExtTableSnapshotInfoManager extSnapshotInfoManager = ExtTableSnapshotInfoManager.getInstance(kylinConfig);
            removeSnapshotIfExist(extSnapshotInfoManager, kylinConfig, tableName, lookupSnapshotID);

            IReadableTable sourceTable = SourceManager.createReadableTable(tableDesc, jobId);

            logger.info("create HTable for source table snapshot:{}", tableName);
            Pair<String, Integer> hTableNameAndShard = createHTable(tableName, sourceTable, kylinConfig);
            String[] keyColumns = getLookupKeyColumns(cube, tableName);
            ExtTableSnapshotInfo snapshot = createSnapshotResource(extSnapshotInfoManager, tableName, lookupSnapshotID,
                    keyColumns, hTableNameAndShard.getFirst(), hTableNameAndShard.getSecond(), sourceTable);
            logger.info("created snapshot information at:{}", snapshot.getResourcePath());

            job = Job.getInstance(HBaseConfiguration.create(getConf()), getOptionValue(OPTION_JOB_NAME));

            setJobClasspath(job, cube.getConfig());
            // For separate HBase cluster, note the output is a qualified HDFS path if "kylin.storage.hbase.cluster-fs" is configured, ref HBaseMRSteps.getHFilePath()
            HBaseConnection.addHBaseClusterNNHAConfiguration(job.getConfiguration());

            FileOutputFormat.setOutputPath(job, output);

            IMRTableInputFormat tableInputFormat = MRUtil.getTableInputFormat(tableDesc, jobId);
            tableInputFormat.configureJob(job);
            job.setMapperClass(LookupTableToHFileMapper.class);

            // set job configuration
            job.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
            job.getConfiguration().set(BatchConstants.CFG_TABLE_NAME, tableName);
            // set block replication to 3 for hfiles
            job.getConfiguration().set(DFSConfigKeys.DFS_REPLICATION_KEY, "3");
            job.getConfiguration().set(BatchConstants.CFG_SHARD_NUM, String.valueOf(hTableNameAndShard.getSecond()));
            // add metadata to distributed cache
            attachCubeMetadata(cube, job.getConfiguration());

            Connection conn = getHBaseConnection(kylinConfig);
            HTable htable = (HTable) conn.getTable(TableName.valueOf(hTableNameAndShard.getFirst()));

            // Automatic config !
            HFileOutputFormat3.configureIncrementalLoad(job, htable, htable.getRegionLocator());

            job.setReducerClass(KVSortReducerWithDupKeyCheck.class);

            this.deletePath(job.getConfiguration(), output);

            return waitForCompletion(job);
        } finally {
            if (job != null)
                cleanupTempConfFile(job.getConfiguration());
        }
    }

    private void removeSnapshotIfExist(ExtTableSnapshotInfoManager extSnapshotInfoManager, KylinConfig kylinConfig,
            String tableName, String lookupSnapshotID) throws IOException {
        ExtTableSnapshotInfo snapshotInfo = null;
        try {
            snapshotInfo = extSnapshotInfoManager.getSnapshot(tableName, lookupSnapshotID);
        } catch (Exception e) {
            // swallow the exception, means not snapshot exist of this snapshot id
        }
        if (snapshotInfo == null) {
            return;
        }
        logger.info("the table:{} snapshot:{} exist, remove it", tableName, lookupSnapshotID);
        extSnapshotInfoManager.removeSnapshot(tableName, lookupSnapshotID);
        String hTableName = snapshotInfo.getStorageLocationIdentifier();
        logger.info("remove related HBase table:{} for snapshot:{}", hTableName, lookupSnapshotID);
        HBaseConnection.deleteTable(kylinConfig.getStorageUrl(), hTableName);
    }

    private String[] getLookupKeyColumns(CubeInstance cube, String tableName) {
        CubeDesc cubeDesc = cube.getDescriptor();
        DataModelDesc modelDesc = cubeDesc.getModel();
        TableRef lookupTableRef = null;
        for (TableRef tableRef : modelDesc.getLookupTables()) {
            if (tableRef.getTableIdentity().equalsIgnoreCase(tableName)) {
                lookupTableRef = tableRef;
                break;
            }
        }
        if (lookupTableRef == null) {
            throw new IllegalStateException("cannot find table in model:" + tableName);
        }
        JoinDesc joinDesc = modelDesc.getJoinByPKSide(lookupTableRef);
        TblColRef[] keyColRefs = joinDesc.getPrimaryKeyColumns();
        String[] result = new String[keyColRefs.length];
        for (int i = 0; i < keyColRefs.length; i++) {
            result[i] = keyColRefs[i].getName();
        }
        return result;
    }

    /**
     *
     * @param sourceTableName
     * @param sourceTable
     * @param kylinConfig
     * @return Pair of HTableName and shard number
     * @throws IOException
     */
    private Pair<String, Integer> createHTable(String sourceTableName, IReadableTable sourceTable,
            KylinConfig kylinConfig) throws IOException {
        TableSignature signature = sourceTable.getSignature();
        int shardNum = calculateShardNum(kylinConfig, signature.getSize());
        Connection conn = getHBaseConnection(kylinConfig);
        Admin admin = conn.getAdmin();
        String hTableName = genHTableName(kylinConfig, admin, sourceTableName);

        TableName tableName = TableName.valueOf(hTableName);
        HTableDescriptor hTableDesc = new HTableDescriptor(tableName);
        hTableDesc.setCompactionEnabled(false);
        hTableDesc.setValue(HTableDescriptor.SPLIT_POLICY, DisabledRegionSplitPolicy.class.getName());
        hTableDesc.setValue(IRealizationConstants.HTableTag, kylinConfig.getMetadataUrlPrefix());
        hTableDesc.setValue(IRealizationConstants.HTableCreationTime, String.valueOf(System.currentTimeMillis()));
        String commitInfo = KylinVersion.getGitCommitInfo();
        if (!StringUtils.isEmpty(commitInfo)) {
            hTableDesc.setValue(IRealizationConstants.HTableGitTag, commitInfo);
        }

        HColumnDescriptor cf = CubeHTableUtil.createColumnFamily(kylinConfig, HBaseLookupRowEncoder.CF_STRING, false);
        hTableDesc.addFamily(cf);

        try {
            if (shardNum > 1) {
                admin.createTable(hTableDesc, getSplitsByShardNum(shardNum));
            } else {
                admin.createTable(hTableDesc);
            }
        } finally {
            IOUtils.closeQuietly(admin);
        }
        return new Pair<>(hTableName, shardNum);
    }

    private int calculateShardNum(KylinConfig kylinConfig, long dataSize) {
        long shardSize = (long) (kylinConfig.getExtTableSnapshotShardingMB()) * 1024 * 1024;
        return dataSize < shardSize ? 1 : (int) (Math.ceil((double) dataSize / shardSize));
    }

    private byte[][] getSplitsByShardNum(int shardNum) {
        byte[][] result = new byte[shardNum - 1][];
        for (int i = 1; i < shardNum; ++i) {
            byte[] split = new byte[RowConstants.ROWKEY_SHARDID_LEN];
            BytesUtil.writeUnsigned(i, split, 0, RowConstants.ROWKEY_SHARDID_LEN);
            result[i - 1] = split;
        }
        return result;
    }

    private ExtTableSnapshotInfo createSnapshotResource(ExtTableSnapshotInfoManager extSnapshotInfoManager,
            String tableName, String snapshotID, String[] keyColumns, String hTableName, int shardNum,
            IReadableTable sourceTable) throws IOException {
        return extSnapshotInfoManager.createSnapshot(sourceTable.getSignature(), tableName, snapshotID, keyColumns,
                shardNum, ExtTableSnapshotInfo.STORAGE_TYPE_HBASE, hTableName);
    }

    private String genHTableName(KylinConfig kylinConfig, Admin admin, String tableName) throws IOException {
        String namePrefix = kylinConfig.getHBaseTableNamePrefix()
                + IRealizationConstants.LookupHbaseStorageLocationPrefix + tableName + "_";
        String namespace = kylinConfig.getHBaseStorageNameSpace();
        String hTableName;
        do {
            StringBuilder sb = new StringBuilder();
            if ((namespace.equals("default") || namespace.equals("")) == false) {
                sb.append(namespace).append(":");
            }
            sb.append(namePrefix);
            for (int i = 0; i < HBASE_TABLE_LENGTH; i++) {
                sb.append(ALPHA_NUM.charAt(ran.nextInt(ALPHA_NUM.length())));
            }
            hTableName = sb.toString();
        } while (hTableExists(admin, hTableName));

        return hTableName;
    }

    private boolean hTableExists(Admin admin, String hTableName) throws IOException {
        return admin.tableExists(TableName.valueOf(hTableName));
    }

    private Connection getHBaseConnection(KylinConfig kylinConfig) throws IOException {
        return HBaseConnection.get(kylinConfig.getStorageUrl());
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new LookupTableToHFileJob(), args);
        System.exit(exitCode);
    }

}
