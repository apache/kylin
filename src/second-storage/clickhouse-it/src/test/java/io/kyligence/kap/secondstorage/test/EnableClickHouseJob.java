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
package io.kyligence.kap.secondstorage.test;

import static org.apache.kylin.common.util.NLocalFileMetadataTestCase.getLocalWorkingDirectory;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.job.execution.NExecutableManager;
import org.eclipse.jetty.toolchain.test.SimpleRequest;
import org.junit.Assert;
import org.testcontainers.containers.JdbcDatabaseContainer;

import io.kyligence.kap.clickhouse.ClickHouseStorage;
import io.kyligence.kap.clickhouse.job.ClickHouseLoad;
import io.kyligence.kap.newten.clickhouse.ClickHouseUtils;
import io.kyligence.kap.newten.clickhouse.EmbeddedHttpServer;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.management.SecondStorageService;
import io.kyligence.kap.secondstorage.test.utils.JobWaiter;
import lombok.SneakyThrows;
import lombok.val;

public class EnableClickHouseJob extends EnableScheduler implements JobWaiter {

    private final List<String> modelNames;
    private final int replica;
    private final JdbcDatabaseContainer<?>[] clickhouse;
    private final SecondStorageService secondStorageService = new SecondStorageService();

    private EmbeddedHttpServer _httpServer;

    public EnableClickHouseJob(JdbcDatabaseContainer<?>[] clickhouse, int replica, String project,
            List<String> modelName, String... extraMeta) {
        super(project, extraMeta);
        this.modelNames = modelName;
        this.replica = replica;
        this.clickhouse = clickhouse;
    }

    @Override
    protected void before() throws Throwable {
        super.before();
        if (_httpServer != null) {
            _httpServer.stopServer();
        }
        // setup http server
        _httpServer = EmbeddedHttpServer.startServer(getLocalWorkingDirectory());
        Unsafe.setProperty(ClickHouseLoad.SOURCE_URL, _httpServer.uriAccessedByDocker.toString());
        Unsafe.setProperty(ClickHouseLoad.ROOT_PATH, getLocalWorkingDirectory());
        overwriteSystemProp("kylin.second-storage.class", ClickHouseStorage.class.getCanonicalName());
        ClickHouseUtils.internalConfigClickHouse(clickhouse, replica);
        secondStorageService.changeProjectSecondStorageState(project, SecondStorageNodeHelper.getAllPairs(), true);
        Assert.assertEquals(clickhouse.length, SecondStorageUtil.listProjectNodes(project).size());
        modelNames.forEach(modelName -> secondStorageService.changeModelSecondStorageState(project, modelName, true));
    }

    @SneakyThrows
    @Override
    protected void after() {
        val execManager = NExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val jobs = execManager.getAllExecutables();
        jobs.forEach(job -> waitJobEnd(project, job.getId()));
        val jobInfo = secondStorageService.changeProjectSecondStorageState(project, null, false);
        try {
            waitJobFinish(project, jobInfo.orElseThrow(null).getJobId());
        } catch (Exception e) {
            // when clickhouse can't accessed, this job will failed
        } finally {
            _httpServer.stopServer();
            super.after();
        }
    }

    @SneakyThrows
    public void checkHttpServer() throws IOException {
        SimpleRequest sr = new SimpleRequest(_httpServer.serverUri);
        final String content = sr.getString("/");
        Assert.assertTrue(content.length() > 0);
    }
}
