/*
 *
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *
 *  contributor license agreements. See the NOTICE file distributed with
 *
 *  this work for additional information regarding copyright ownership.
 *
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *
 *  (the "License"); you may not use this file except in compliance with
 *
 *  the License. You may obtain a copy of the License at
 *
 *
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *
 *  Unless required by applicable law or agreed to in writing, software
 *
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and
 *
 *  limitations under the License.
 *
 * /
 */

package org.apache.kylin.job;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractKylinTestCase;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.dict.lookup.HiveTableReader;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.invertedindex.model.IIJoinedFlatTableDesc;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.hadoop.cube.StorageCleanupJob;
import org.apache.kylin.job.hadoop.invertedindex.IICreateHTableJob;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.streaming.Stream;
import org.apache.kylin.streaming.invertedindex.IIStreamBuilder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;

import static org.junit.Assert.fail;

/**
 * Created by qianzhou on 3/9/15.
 */
public class BuildIIWithStreamTest {

    private static final Logger logger = LoggerFactory.getLogger(BuildIIWithStreamTest.class);

    private static final String[] II_NAME = new String[]{"test_kylin_ii_left_join", "test_kylin_ii_inner_join"};
    private IIManager iiManager;
    private KylinConfig kylinConfig;

    @BeforeClass
    public static void beforeClass() throws Exception {
        logger.info("Adding to classpath: " + new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        ClassUtil.addClasspath(new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        System.setProperty("hdp.version", "2.2.0.0-2041"); // mapred-site.xml ref this
    }

    @Before
    public void before() throws Exception {
        HBaseMetadataTestCase.staticCreateTestMetadata(AbstractKylinTestCase.SANDBOX_TEST_DATA);
        DeployUtil.overrideJobJarLocations();

        kylinConfig = KylinConfig.getInstanceFromEnv();
        iiManager = IIManager.getInstance(kylinConfig);
        iiManager = IIManager.getInstance(kylinConfig);
        for (String iiInstance : II_NAME) {

            IIInstance ii = iiManager.getII(iiInstance);
            if (ii.getStatus() != RealizationStatusEnum.DISABLED) {
                ii.setStatus(RealizationStatusEnum.DISABLED);
                iiManager.updateII(ii);
            }
        }
    }

    @AfterClass
    public static void afterClass() throws Exception {
        backup();
    }

    private static int cleanupOldStorage() throws Exception {
        String[] args = {"--delete", "true"};

        int exitCode = ToolRunner.run(new StorageCleanupJob(), args);
        return exitCode;
    }

    private static void backup() throws Exception {
        int exitCode = cleanupOldStorage();
        if (exitCode == 0) {
            exportHBaseData();
        }
    }

    private static void exportHBaseData() throws IOException {
        ExportHBaseData export = new ExportHBaseData();
        export.exportTables();
    }

    private String createIntermediateTable(IIDesc desc, KylinConfig kylinConfig) throws IOException {
        IIJoinedFlatTableDesc intermediateTableDesc = new IIJoinedFlatTableDesc(desc);
        JobEngineConfig jobEngineConfig = new JobEngineConfig(kylinConfig);
        final String uuid = UUID.randomUUID().toString();
        final String dropTableHql = JoinedFlatTable.generateDropTableStatement(intermediateTableDesc, uuid);
        final String createTableHql = JoinedFlatTable.generateCreateTableStatement(intermediateTableDesc, jobEngineConfig.getHdfsWorkingDirectory() + "/kylin-" + uuid, uuid);
        String insertDataHqls;
        try {
            insertDataHqls = JoinedFlatTable.generateInsertDataStatement(intermediateTableDesc, uuid, jobEngineConfig);
        } catch (IOException e1) {
            e1.printStackTrace();
            throw new RuntimeException("Failed to generate insert data SQL for intermediate table.");
        }

        ShellExecutable step = new ShellExecutable();
        StringBuffer buf = new StringBuffer();
        buf.append("hive -e \"");
        buf.append(dropTableHql + "\n");
        buf.append(createTableHql + "\n");
        buf.append(insertDataHqls + "\n");
        buf.append("\"");

        step.setCmd(buf.toString());
        logger.info(step.getCmd());
        step.setName(ExecutableConstants.STEP_NAME_CREATE_FLAT_HIVE_TABLE);
        kylinConfig.getCliCommandExecutor().execute(step.getCmd(), null);
        return intermediateTableDesc.getTableName(uuid);
    }

    private void clearSegment(String iiName) throws Exception {
        IIInstance ii = iiManager.getII(iiName);
        ii.getSegments().clear();
        iiManager.updateII(ii);
    }

    private IISegment createSegment(String iiName) throws Exception {
        clearSegment(iiName);
        SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
        f.setTimeZone(TimeZone.getTimeZone("GMT"));

        long date1 = 0;
        long date2 = f.parse("2015-01-01").getTime();
        return buildSegment(iiName, date1, date2);
    }

    private IISegment buildSegment(String iiName, long startDate, long endDate) throws Exception {
        IIInstance iiInstance = iiManager.getII(iiName);
        IISegment segment = iiManager.buildSegment(iiInstance, startDate, endDate);
        iiInstance.getSegments().add(segment);
        iiManager.updateII(iiInstance);
        return segment;
    }

    private void buildII(String iiName) throws Exception {
        final IIDesc desc = iiManager.getII(iiName).getDescriptor();
        final String tableName = createIntermediateTable(desc, kylinConfig);
        logger.info("intermediate table name:" + tableName);
        final Configuration conf = new Configuration();
        HCatInputFormat.setInput(conf, "default", tableName);
        final HCatSchema tableSchema = HCatInputFormat.getTableSchema(conf);
        logger.info(StringUtils.join(tableSchema.getFieldNames(), "\n"));
        HiveTableReader reader = new HiveTableReader("default", tableName);
        final List<TblColRef> tblColRefs = desc.listAllColumns();
        for (TblColRef tblColRef : tblColRefs) {
            if (desc.isMetricsCol(tblColRef)) {
                logger.info("matrix:" + tblColRef.getName());
            } else {
                logger.info("measure:" + tblColRef.getName());
            }
        }
        LinkedBlockingDeque<Stream> queue = new LinkedBlockingDeque<Stream>();
        final IISegment segment = createSegment(iiName);
        String[] args = new String[]{"-iiname", iiName, "-htablename", segment.getStorageLocationIdentifier()};
        ToolRunner.run(new IICreateHTableJob(), args);


        ExecutorService executorService = Executors.newSingleThreadExecutor();
        final IIStreamBuilder streamBuilder = new IIStreamBuilder(queue, segment.getStorageLocationIdentifier(), segment.getIIInstance(), 0);
        int count = 0;
        while (reader.next()) {
            queue.put(parse(reader.getRow()));
            count++;
        }
        reader.close();
        logger.info("total record count:" + count + " htable:" + segment.getStorageLocationIdentifier());
        queue.put(Stream.EOF);
        final Future<?> future = executorService.submit(streamBuilder);
        try {
            future.get();
        } catch (Exception e) {
            logger.error("stream build failed", e);
            fail("stream build failed");
        }

        logger.info("stream build finished, htable name:" + segment.getStorageLocationIdentifier());
    }

    @Test
    public void test() throws Exception {
        for (String iiName : II_NAME) {
            buildII(iiName);
            IIInstance ii = iiManager.getII(iiName);
            if (ii.getStatus() != RealizationStatusEnum.READY) {
                ii.setStatus(RealizationStatusEnum.READY);
                iiManager.updateII(ii);
            }
        }
    }

    private Stream parse(String[] row) {
        return new Stream(System.currentTimeMillis(), StringUtils.join(row, ",").getBytes());
    }

}
