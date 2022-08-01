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

package org.apache.kylin.tool;

import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.metadata.JdbcAuditLogStore;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.engine.spark.job.NSparkCubingJob;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.favorite.FavoriteRule;
import org.apache.kylin.metadata.favorite.FavoriteRule.AbstractCondition;
import org.apache.kylin.metadata.favorite.FavoriteRuleManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.tool.general.RollbackStatusEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.sparkproject.guava.collect.Sets;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.val;
import lombok.var;

public class RollbackToolTest extends NLocalFileMetadataTestCase {

    DateTimeFormatter DATE_TIME_FORMATTER = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss",
            Locale.getDefault(Locale.Category.FORMAT));

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        prepare();
        val jdbcTemplate = getJdbcTemplate();
        getStore().getMetadataStore().setAuditLogStore(new JdbcAuditLogStore(getTestConfig(), jdbcTemplate,
                new DataSourceTransactionManager(jdbcTemplate.getDataSource()), "test_audit_log"));
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testRestoreFail() throws Exception {

        val tool = Mockito.spy(new RollbackTool());
        Mockito.doReturn(true).when(tool).waitUserConfirm();
        Mockito.doReturn(true).when(tool).checkClusterStatus();

        val kylinConfig = KylinConfig.getInstanceFromEnv();
        MetadataTool.backup(kylinConfig);

        Thread.sleep(1000);
        val t1 = LocalDateTime.now(Clock.systemDefaultZone());
        Thread.sleep(1000);

        UnitOfWork.doInTransactionWithRetry(() -> {
            val projectMgr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            ProjectInstance projectInstance = new ProjectInstance();
            projectMgr.createProject("project12", "", "", Maps.newLinkedHashMap());
            return 0;
        }, "project12", 1);

        UnitOfWork.doInTransactionWithRetry(() -> {
            val ruleMgr = FavoriteRuleManager.getInstance(KylinConfig.getInstanceFromEnv(), "project12");
            FavoriteRule.Condition cond2 = new FavoriteRule.Condition();
            cond2.setRightThreshold("4");
            List<AbstractCondition> conds = Lists.newArrayList(cond2);
            FavoriteRule newRule = new FavoriteRule(conds, "new_rule", true);
            ruleMgr.createRule(newRule);
            return 0;
        }, "project12", 1);

        tool.execute(new String[] { "-skipCheckData", "true", "-time", t1.format(DATE_TIME_FORMATTER) });

        val currentResourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
        val projectItems = currentResourceStore.listResources("/_global/project");
        //Time travel successfully, does not include project12
        Assert.assertFalse(projectItems.contains("/_global/project/project12.json"));

        Thread.sleep(1000);
        val t2 = LocalDateTime.now(Clock.systemDefaultZone());
        Thread.sleep(1000);
        UnitOfWork.doInTransactionWithRetry(() -> {
            val projectMgr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            ProjectInstance projectInstance = new ProjectInstance();
            projectMgr.createProject("project13", "", "", Maps.newLinkedHashMap());
            return 0;
        }, "project13", 1);
        UnitOfWork.doInTransactionWithRetry(() -> {
            val ruleMgr = FavoriteRuleManager.getInstance(KylinConfig.getInstanceFromEnv(), "project13");
            FavoriteRule.Condition cond2 = new FavoriteRule.Condition();
            cond2.setRightThreshold("4");
            List<AbstractCondition> conds = Lists.newArrayList(cond2);
            FavoriteRule newRule = new FavoriteRule(conds, "new_rule", true);
            ruleMgr.createRule(newRule);
            return 0;
        }, "project13", 1);

        Thread.sleep(1000);
        val t3 = LocalDateTime.now(Clock.systemDefaultZone());
        Thread.sleep(1000);

        UnitOfWork.doInTransactionWithRetry(() -> {
            val projectMgr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            ProjectInstance projectInstance = new ProjectInstance();
            projectMgr.createProject("project14", "", "", Maps.newLinkedHashMap());
            return 0;
        }, "project14", 1);
        UnitOfWork.doInTransactionWithRetry(() -> {
            val ruleMgr = FavoriteRuleManager.getInstance(KylinConfig.getInstanceFromEnv(), "project14");
            FavoriteRule.Condition cond2 = new FavoriteRule.Condition();
            cond2.setRightThreshold("4");
            List<AbstractCondition> conds = Lists.newArrayList(cond2);
            FavoriteRule newRule = new FavoriteRule(conds, "new_rule", true);
            ruleMgr.createRule(newRule);
            return 0;
        }, "project14", 1);

        tool.execute(new String[] { "-skipCheckData", "true", "-time", t2.format(DATE_TIME_FORMATTER) });

        // Time travel successfully, does not include project13  does not include project14
        val projectItems1 = currentResourceStore.listResources("/_global/project");
        Assert.assertFalse(projectItems1.contains("/_global/project/project13.json"));
        Assert.assertFalse(projectItems1.contains("/_global/project/project14.json"));

        Thread.sleep(1000);
        tool.execute(new String[] { "-skipCheckData", "true", "-time", t3.format(DATE_TIME_FORMATTER) });

        // Time travel successfully, include project13  does not include project14
        val projectItems2 = currentResourceStore.listResources("/_global/project");
        Assert.assertTrue(projectItems2.contains("/_global/project/project13.json"));
        Assert.assertFalse(projectItems2.contains("/_global/project/project14.json"));

        Thread.sleep(1000);
        val t4 = LocalDateTime.now(Clock.systemDefaultZone());
        Thread.sleep(1000);

        UnitOfWork.doInTransactionWithRetry(() -> {
            val projectMgr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            ProjectInstance projectInstance = new ProjectInstance();
            projectMgr.createProject("project15", "", "", Maps.newLinkedHashMap());
            return 0;
        }, "project15", 1);
        UnitOfWork.doInTransactionWithRetry(() -> {
            val ruleMgr = FavoriteRuleManager.getInstance(KylinConfig.getInstanceFromEnv(), "project15");
            FavoriteRule.Condition cond2 = new FavoriteRule.Condition();
            cond2.setRightThreshold("4");
            List<AbstractCondition> conds = Lists.newArrayList(cond2);
            FavoriteRule newRule = new FavoriteRule(conds, "new_rule", true);
            ruleMgr.createRule(newRule);
            return 0;
        }, "project15", 1);

        Mockito.doReturn(false).when(tool).restoreMirror(Mockito.any(), Mockito.any(), Mockito.any());

        tool.execute(new String[] { "-skipCheckData", "true", "-time", t4.format(DATE_TIME_FORMATTER) });
        // Time travel failed, include project15
        val projectItems3 = currentResourceStore.listResources("/_global/project");
        Assert.assertTrue(projectItems3.contains("/_global/project/project13.json"));
        Assert.assertTrue(projectItems3.contains("/_global/project/project15.json"));
    }

    @Test
    public void testDeleteProject() throws Exception {
        val tool = Mockito.spy(new RollbackTool());
        Mockito.doReturn(true).when(tool).waitUserConfirm();
        Mockito.doReturn(true).when(tool).checkClusterStatus();
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        MetadataTool.backup(kylinConfig);

        Thread.sleep(1000);
        val t1 = LocalDateTime.now(Clock.systemDefaultZone());
        Thread.sleep(1000);

        UnitOfWork.doInTransactionWithRetry(() -> {
            val projectMgr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
            projectMgr.forceDropProject("default");
            return 0;
        }, "default", 1);

        tool.execute(new String[] { "-time", t1.format(DATE_TIME_FORMATTER), "-project", "default" });
        Assert.assertEquals(tool.rollbackStatus, RollbackStatusEnum.WAIT_USER_CONFIRM_SUCCESS);
    }

    @Test
    public void testJobRollback() throws Exception {
        val tool = Mockito.spy(new RollbackTool());
        Mockito.doReturn(true).when(tool).waitUserConfirm();
        Mockito.doReturn(true).when(tool).checkClusterStatus();
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        MetadataTool.backup(kylinConfig);
        val jobId = RandomUtil.randomUUIDStr();
        UnitOfWork.doInTransactionWithRetry(() -> {
            val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
            mockJob("default", "89af4ee2-2cdb-4b07-b39e-4c29856309aa", jobId, SegmentRange.dateToLong("2012-01-01"),
                    SegmentRange.dateToLong("2012-09-01"));
            return 0;
        }, "default", 1);

        Thread.sleep(1000);
        val t1 = LocalDateTime.now(Clock.systemDefaultZone());
        Thread.sleep(1000);

        UnitOfWork.doInTransactionWithRetry(() -> {
            val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
            executableManager.updateJobOutput(jobId, ExecutableState.RUNNING);
            return 0;
        }, "default", 1);

        UnitOfWork.doInTransactionWithRetry(() -> {
            val executableManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
            executableManager.updateJobOutput(jobId, ExecutableState.SUCCEED);
            return 0;
        }, "default", 1);
        tool.execute(new String[] { "-skipCheckData", "true", "-time", t1.format(DATE_TIME_FORMATTER), "-project",
                "default" });
        Assert.assertSame(ExecutableState.READY, NExecutableManager
                .getInstance(KylinConfig.getInstanceFromEnv(), "default").getAllExecutables().get(0).getStatus());
    }

    JdbcTemplate getJdbcTemplate() throws Exception {
        val url = StorageURL.valueOf(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=");
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        return new JdbcTemplate(dataSource);
    }

    private void mockJob(String project, String dataflowId, String jobId, long start, long end) {
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        var dataflow = dataflowManager.getDataflow(dataflowId);
        dataflow = dataflowManager.getDataflow(dataflow.getId());
        val segment = dataflow.getSegment("ef5e0663-feba-4ed2-b71c-21958122bbff");
        val layouts = dataflow.getIndexPlan().getAllLayouts();
        NSparkCubingJob job = NSparkCubingJob.create(Sets.newHashSet(segment), Sets.newLinkedHashSet(layouts), "ADMIN",
                JobTypeEnum.INDEX_BUILD, jobId, null, null, null);
        NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).addJob(job);
    }

    private void prepare() throws IOException {

        FileUtils.copyDirectory(new File("src/test/resources/ut_storage/working-dir"),
                new File(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory().replace("file://", "")));
    }

}
