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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractKylinTestCase;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.dict.lookup.HiveTableReader;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.job.common.ShellExecutable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.hadoop.hive.IIJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.streaming.Stream;
import org.apache.kylin.streaming.invertedindex.IIStreamBuilder;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by qianzhou on 3/9/15.
 */
public class BuildIIWithStreamTest {

    private static final Log logger = LogFactory.getLog(BuildIIWithStreamTest.class);

    private static final String II_NAME = "test_kylin_ii_inner_join";
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

        kylinConfig = KylinConfig.getInstanceFromEnv();
        iiManager = IIManager.getInstance(kylinConfig);
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

    @Test
    public void test() throws IOException, InterruptedException {
        final IIDesc desc = iiManager.getII(II_NAME).getDescriptor();
//        final String tableName = createIntermediateTable(desc, kylinConfig);
        final String tableName = "kylin_intermediate_ii_test_kylin_ii_inner_join_desc_f24b8e1f_1c1f_4835_8d78_8f21ce79a536";
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
        final IIStreamBuilder streamBuilder = new IIStreamBuilder(queue, "test_htable", desc, 0);
        final Thread thread = new Thread(streamBuilder);
        thread.start();
        while (reader.next()) {
            queue.put(parse(reader.getRow()));
        }
        queue.put(new Stream(-1, null));
        thread.join();

    }

    private Stream parse(String[] row) {
        return new Stream(System.currentTimeMillis(), StringUtils.join(row, ",").getBytes());
    }

}
