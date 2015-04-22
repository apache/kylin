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

import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.HBaseMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.invertedindex.IISegment;
import org.apache.kylin.invertedindex.model.IIDesc;
import org.apache.kylin.job.hadoop.invertedindex.IICreateHTableJob;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.streaming.JsonStreamParser;
import org.apache.kylin.streaming.StreamMessage;
import org.apache.kylin.streaming.invertedindex.IIStreamBuilder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;

import static org.junit.Assert.fail;

/**
 * Created by qianzhou on 3/9/15.
 */
public class BuildIIForEagleTest {

    private static final Logger logger = LoggerFactory.getLogger(BuildIIForEagleTest.class);

    private static final String[] II_NAME = new String[] {"eagle_ii"};
    private IIManager iiManager;

    @BeforeClass
    public static void beforeClass() throws Exception {
        logger.info("Adding to classpath: " + new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        ClassUtil.addClasspath(new File(HBaseMetadataTestCase.SANDBOX_TEST_DATA).getAbsolutePath());
        System.setProperty("hdp.version", "2.2.0.0-2041"); // mapred-site.xml ref this
    }

    private static void deployMetadata() throws IOException {
        // install metadata to hbase
        ResourceTool.reset(KylinConfig.getInstanceFromEnv());
        ResourceTool.copy(KylinConfig.createInstanceFromUri("../ebayExtension/examples/test_case_data/localmeta"), KylinConfig.getInstanceFromEnv());

        // update cube desc signature.
        for (CubeInstance cube : CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).listAllCubes()) {
            cube.getDescriptor().setSignature(cube.getDescriptor().calculateSignature());
            CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).updateCube(cube);
        }
    }

    @Before
    public void before() throws Exception {
        HBaseMetadataTestCase.staticCreateTestMetadata(HBaseMetadataTestCase.SANDBOX_TEST_DATA);
        DeployUtil.overrideJobJarLocations();
        deployMetadata();

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
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
    }

    private static int cleanupOldStorage() throws Exception {
        return 0;

        //do not delete intermediate files for debug purpose

        //        String[] args = {"--delete", "true"};
        //
        //        int exitCode = ToolRunner.run(new StorageCleanupJob(), args);
        //        return exitCode;
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
        final IIInstance ii = iiManager.getII(iiName);
        final IIDesc desc = ii.getDescriptor();
        final List<TblColRef> tblColRefs = desc.listAllColumns();
        final IISegment segment = ii.getFirstSegment();
        for (TblColRef tblColRef : tblColRefs) {
            if (desc.isMetricsCol(tblColRef)) {
                logger.info("matrix:" + tblColRef.getName());
            } else {
                logger.info("measure:" + tblColRef.getName());
            }
        }
        LinkedBlockingDeque<StreamMessage> queue = new LinkedBlockingDeque<StreamMessage>();
        String[] args = new String[] { "-iiname", iiName, "-htablename", segment.getStorageLocationIdentifier() };
        ToolRunner.run(new IICreateHTableJob(), args);

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        final IIStreamBuilder streamBuilder = new IIStreamBuilder(queue, iiName, segment.getStorageLocationIdentifier(), segment.getIIDesc(), 0, desc.isUseLocalDictionary());
        streamBuilder.setStreamParser(new JsonStreamParser(segment.getIIDesc().listAllColumns()));

        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream("/Users/qianzhou/Projects/Kylin/eagle_5m.data")));
        String line;
        int count = 0;
        while ((line = br.readLine()) != null) {
            queue.put(new StreamMessage(System.currentTimeMillis(), line.getBytes()));
            count++;
        }
        br.close();
        logger.info("total record count:" + count + " htable:" + segment.getStorageLocationIdentifier());
        queue.put(StreamMessage.EOF);
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

}
