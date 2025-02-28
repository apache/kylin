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

package org.apache.kylin.event;

import static org.awaitility.Awaitility.await;

import java.io.File;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.annotation.Clarification;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.tool.garbage.StorageCleaner;
import org.apache.kylin.util.SegmentInitializeUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Clarification(priority = Clarification.Priority.MAJOR, msg = "Enterprise")
public class ITStorageCleanerTest extends NLocalWithSparkSessionTest {

    @Before
    public void setUp() throws Exception {
        overwriteSystemProp("kylin.job.event.poll-interval-second", "1");
        overwriteSystemProp("kylin.engine.spark.build-class-name",
                "org.apache.kylin.engine.spark.job.MockedDFBuildJob");
        overwriteSystemProp("kylin.garbage.storage.cuboid-layout-survival-time-threshold", "0s");
        this.createTestMetadata();

        val projectMgr = NProjectManager.getInstance(getTestConfig());
        for (String project : Arrays.asList("bad_query_test", "broken_test", "demo", "match", "newten", "smart", "ssb",
                "top_n")) {
            projectMgr.forceDropProject(project);
        }

        JobContextUtil.cleanUp();
        JobContextUtil.getJobContext(getTestConfig());

        val tableMgr = NTableMetadataManager.getInstance(getTestConfig(), getProject());
        val table = tableMgr.getTableDesc("DEFAULT.TEST_KYLIN_FACT");
        table.setIncrementLoading(true);
        tableMgr.updateTableDesc(table);
    }

    @After
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        this.cleanupTestMetadata();
    }

    @Test
    @Ignore("TODO: remove or adapt")
    public void testStorageCleanWithJob_MultiThread() throws InterruptedException {
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), getProject());
        val indexManager = NIndexPlanManager.getInstance(getTestConfig(), getProject());
        val df = dataflowManager.getDataflow("741ca86a-1f13-46da-a59f-95fb68615e3a");
        val MAX_WAIT = 500 * 1000;
        val start = System.currentTimeMillis() + MAX_WAIT;
        val finished = new AtomicBoolean(false);
        new Thread(() -> {
            while (System.currentTimeMillis() < start && !finished.get()) {
                try {
                    val cleaner = new StorageCleaner();
                    cleaner.execute();
                    await().pollDelay(1100, TimeUnit.MILLISECONDS).until(() -> true);
                } catch (Exception e) {
                    log.warn("gc failed", e);
                }
            }
        }).start();
        SegmentInitializeUtil.prepareSegment(getTestConfig(), getProject(), df.getUuid(), "2012-01-01", "2012-06-01",
                true);
        SegmentInitializeUtil.prepareSegment(getTestConfig(), getProject(), df.getUuid(), "2012-06-01", "2012-09-01",
                false);

        indexManager.updateIndexPlan(df.getId(), copyForWrite -> {
            copyForWrite.removeLayouts(Sets.newHashSet(30001L, 20001L), true, true);
        });
        val df2 = dataflowManager.getDataflow(df.getUuid());

        await().pollDelay(3000, TimeUnit.MILLISECONDS).until(() -> true);
        val root = getTestConfig().getHdfsWorkingDirectory().substring(7) + "default/parquet/";
        val layoutFolders = FileUtils.listFiles(new File(root), new String[] { "parquet" }, true).stream()
                .map(File::getParent).distinct().sorted().collect(Collectors.toList());
        Set<String> expected = Sets.newTreeSet();
        for (NDataSegment segment : df2.getSegments()) {
            for (Map.Entry<Long, NDataLayout> entry : segment.getLayoutsMap().entrySet()) {
                expected.add(root + df2.getId() + "/" + segment.getId() + "/" + entry.getKey());
            }
        }
        finished.set(true);
        Assert.assertEquals(String.join(";\n", expected), String.join(";\n", layoutFolders));
    }
}
