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

package org.apache.kylin.rest.config.initialize;

import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.datasourceParameters;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.metrics.MetricsController;
import org.apache.kylin.common.metrics.MetricsGroup;
import org.apache.kylin.common.metrics.MetricsName;
import org.apache.kylin.common.metrics.MetricsTag;
import org.apache.kylin.common.metrics.prometheus.PrometheusMetrics;
import org.apache.kylin.common.persistence.metadata.JdbcDataSource;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultOutput;
import org.apache.kylin.job.execution.Executable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.query.util.LoadCounter;
import org.apache.kylin.rest.response.StorageVolumeInfoResponse;
import org.apache.kylin.rest.service.ProjectService;
import org.apache.kylin.rest.util.SpringContext;
import org.apache.spark.sql.SparderEnv;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.test.util.ReflectionTestUtils;

import com.codahale.metrics.MetricFilter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.val;
import lombok.var;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ SpringContext.class, MetricsGroup.class, UserGroupInformation.class, JdbcDataSource.class,
        SparderEnv.class, NDefaultScheduler.class, NExecutableManager.class, LoadCounter.class })
public class MetricsRegistryTest extends NLocalFileMetadataTestCase {

    private MeterRegistry meterRegistry;

    private String project = "default";

    Map<String, Long> totalStorageSizeMap;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() throws IOException {
        PowerMockito.mockStatic(UserGroupInformation.class);
        UserGroupInformation userGroupInformation = Mockito.mock(UserGroupInformation.class);
        PowerMockito.when(UserGroupInformation.getCurrentUser()).thenReturn(userGroupInformation);

        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();

        totalStorageSizeMap = (Map<String, Long>) ReflectionTestUtils.getField(MetricsRegistry.class,
                "totalStorageSizeMap");
        totalStorageSizeMap.put(project, 1L);

        meterRegistry = new SimpleMeterRegistry();

        PowerMockito.mockStatic(SpringContext.class);
        PowerMockito.mockStatic(SparderEnv.class);
        PowerMockito.mockStatic(NDefaultScheduler.class);
        PowerMockito.mockStatic(NExecutableManager.class);
        PowerMockito.mockStatic(LoadCounter.class);
    }

    @Test
    public void testRefreshStorageVolumeInfo() {
        StorageVolumeInfoResponse response = Mockito.mock(StorageVolumeInfoResponse.class);
        Mockito.when(response.getTotalStorageSize()).thenReturn(2L);

        ProjectService projectService = PowerMockito.mock(ProjectService.class);
        Mockito.when(projectService.getStorageVolumeInfoResponse(project)).thenReturn(response);

        PowerMockito.when(SpringContext.getBean(ProjectService.class)).thenReturn(projectService);

        MetricsRegistry.refreshTotalStorageSize();
        Assert.assertEquals(totalStorageSizeMap.get(project), Long.valueOf(2));
    }

    @Test
    public void testRemoveProjectFromStorageSizeMap() {
        Assert.assertEquals(1, totalStorageSizeMap.size());
        MetricsRegistry.removeProjectFromStorageSizeMap(project);
        Assert.assertEquals(0, totalStorageSizeMap.size());
    }

    @Test
    public void testRegisterGlobalPrometheusMetrics() throws Exception {
        val url = getTestConfig().getMetadataUrl();
        val props = datasourceParameters(url);
        val dataSource = BasicDataSourceFactory.createDataSource(props);
        PowerMockito.mockStatic(JdbcDataSource.class);
        PowerMockito.when(JdbcDataSource.getDataSources()).thenReturn(Lists.newArrayList(dataSource));
        PowerMockito.when(SpringContext.getBean(MeterRegistry.class)).thenReturn(meterRegistry);
        LoadCounter loadCounter = Mockito.mock(LoadCounter.class);
        PowerMockito.when(LoadCounter.getInstance()).thenReturn(loadCounter);
        Mockito.when(loadCounter.getRunningTaskCount()).thenReturn(10);
        Mockito.when(loadCounter.getPendingTaskCount()).thenReturn(10);
        Mockito.when(loadCounter.getSlotCount()).thenReturn(100);
        MetricsRegistry.registerGlobalPrometheusMetrics();
        List<Meter> meters = meterRegistry.getMeters();
        Assert.assertEquals(6, meters.size());
        MetricsRegistry.registerGlobalMetrics(getTestConfig(), project);

        List<Meter> meterList = meterRegistry.getMeters().stream()
                .filter(e -> "idle".equals(e.getId().getTag(MetricsTag.STATE.getVal()))
                        || "active".equals(e.getId().getTag(MetricsTag.STATE.getVal())))
                .collect(Collectors.toList());
        Assert.assertEquals(2, meterList.size());
        Collection<Gauge> gauges = meterRegistry.find(PrometheusMetrics.JVM_DB_CONNECTIONS.getValue()).gauges();
        gauges.forEach(e -> Assert.assertEquals(0, e.value(), 0));
        Collection<Gauge> gauges1 = meterRegistry.find(PrometheusMetrics.SPARK_TASKS.getValue()).gauges();
        gauges1.forEach(e -> Assert.assertEquals(0, e.value(), 0));
        Collection<Gauge> gauges2 = meterRegistry.find(PrometheusMetrics.SPARK_TASK_UTILIZATION.getValue()).gauges();
        gauges2.forEach(e -> Assert.assertEquals(0, e.value(), 0));
        Collection<Gauge> gauges3 = meterRegistry.find(PrometheusMetrics.SPARDER_UP.getValue()).gauges();
        gauges3.forEach(e -> Assert.assertEquals(0, e.value(), 0));

        PowerMockito.when(SparderEnv.isSparkAvailable()).thenReturn(true);
        Assert.assertEquals(2, gauges1.size());
        gauges1.forEach(e -> Assert.assertEquals(10, e.value(), 0));
        gauges2.forEach(e -> Assert.assertEquals(0.1, e.value(), 0));
        gauges3.forEach(e -> Assert.assertEquals(1, e.value(), 0));
    }

    @Test
    public void testRegisterProjectPrometheusMetrics() {
        KylinConfig kylinConfig = getTestConfig();
        kylinConfig.setProperty("kylin.metrics.prometheus-enabled", "false");
        PowerMockito.when(SpringContext.getBean(MeterRegistry.class)).thenReturn(meterRegistry);
        MetricsRegistry.registerProjectPrometheusMetrics(kylinConfig, project);
        List<Meter> meters1 = meterRegistry.getMeters();
        Assert.assertEquals(0, meters1.size());

        kylinConfig.setProperty("kylin.metrics.prometheus-enabled", "true");
        MetricsRegistry.registerProjectPrometheusMetrics(kylinConfig, project);

        Collection<Gauge> gauges4 = meterRegistry.find(PrometheusMetrics.JOB_COUNTS.getValue()).gauges();
        Assert.assertEquals(1, gauges4.size());
        gauges4.forEach(Gauge::value);

        Collection<Meter> meters2 = meterRegistry.find(PrometheusMetrics.JOB_COUNTS.getValue()).meters();
        meters2.forEach(meter -> meterRegistry.remove(meter));
        NDefaultScheduler mockScheduler = PowerMockito.mock(NDefaultScheduler.class);
        Mockito.when(mockScheduler.getContext()).thenReturn(null);
        Collection<Gauge> gauges5 = meterRegistry.find(PrometheusMetrics.JOB_COUNTS.getValue()).gauges();
        gauges5.forEach(e -> Assert.assertEquals(0, e.value(), 0));

        Collection<Meter> meters3 = meterRegistry.find(PrometheusMetrics.JOB_COUNTS.getValue()).meters();
        meters3.forEach(meter -> meterRegistry.remove(meter));
        Executable mockExecutable1 = Mockito.mock(Executable.class);
        DefaultOutput defaultOutput1 = Mockito.mock(DefaultOutput.class);
        Mockito.when(defaultOutput1.getState()).thenReturn(ExecutableState.RUNNING);
        PowerMockito.when(NDefaultScheduler.getInstance(project)).thenReturn(mockScheduler);
        Mockito.when(mockExecutable1.getOutput()).thenReturn(defaultOutput1);
        Mockito.when(defaultOutput1.getState()).thenReturn(ExecutableState.RUNNING);
        Map<String, Executable> executableMap = Maps.newHashMap();
        executableMap.put("mockExecutable1", mockExecutable1);
        ExecutableContext executableContext = Mockito.mock(ExecutableContext.class);
        Mockito.when(mockScheduler.getContext()).thenReturn(executableContext);
        Mockito.when(executableContext.getRunningJobs()).thenReturn(executableMap);
        MetricsRegistry.registerProjectPrometheusMetrics(kylinConfig, project);
        Collection<Gauge> gauges6 = meterRegistry.find(PrometheusMetrics.JOB_COUNTS.getValue()).gauges();
        gauges6.forEach(e -> Assert.assertEquals(1, e.value(), 0));
        Collection<Meter> meters4 = meterRegistry.find(PrometheusMetrics.JOB_LONG_RUNNING.getValue()).meters();
        meters4.forEach(meter -> meterRegistry.remove(meter));
        MetricsRegistry.registerProjectPrometheusMetrics(kylinConfig, project);
        Collection<Gauge> gauges7 = meterRegistry.find(PrometheusMetrics.JOB_LONG_RUNNING.getValue()).gauges();
        Assert.assertEquals(0, gauges7.stream().filter(e -> e.value() == 1).count());
        NExecutableManager executableManager = PowerMockito.mock(NExecutableManager.class);
        PowerMockito.when(NExecutableManager.getInstance(kylinConfig, "default")).thenReturn(executableManager);
        ExecutablePO mockExecutablePO = Mockito.mock(ExecutablePO.class);
        ExecutablePO mockExecutablePO1 = Mockito.mock(ExecutablePO.class);
        AbstractExecutable mockAbstractExecutable = Mockito.mock(AbstractExecutable.class);
        AbstractExecutable mockAbstractExecutable1 = Mockito.mock(AbstractExecutable.class);
        ExecutableOutputPO mockExecutableOutputPO = Mockito.mock(ExecutableOutputPO.class);
        ExecutableOutputPO mockExecutableOutputPO1 = Mockito.mock(ExecutableOutputPO.class);
        Mockito.when(mockExecutablePO.getOutput()).thenReturn(mockExecutableOutputPO);
        Mockito.when(mockExecutablePO1.getOutput()).thenReturn(mockExecutableOutputPO1);
        Mockito.when(mockExecutableOutputPO.getStatus()).thenReturn(ExecutableState.READY.name());
        Mockito.when(mockExecutableOutputPO1.getStatus()).thenReturn(ExecutableState.RUNNING.name());
        Mockito.when(mockAbstractExecutable.getWaitTime()).thenReturn(8 * 60 * 1000L);
        Mockito.when(mockAbstractExecutable1.getDuration()).thenReturn(3 * 60 * 60 * 1000L);
        Mockito.when(executableManager.fromPO(mockExecutablePO)).thenReturn(mockAbstractExecutable);
        Mockito.when(executableManager.fromPO(mockExecutablePO1)).thenReturn(mockAbstractExecutable1);
        Mockito.when(executableManager.getAllJobs())
                .thenReturn(Lists.newArrayList(mockExecutablePO, mockExecutablePO1));
        Set<String> projectSet = new HashSet<>();
        projectSet.add(project);
        MetricsRegistry.refreshProjectLongRunningJobs(kylinConfig, projectSet);
        Assert.assertEquals(5, gauges7.stream().filter(e -> e.value() == 1).count());
    }


    @Test
    public void testRegisterMicrometerProjectMetrics() {
        StorageVolumeInfoResponse response = Mockito.mock(StorageVolumeInfoResponse.class);
        Mockito.when(response.getTotalStorageSize()).thenReturn(2L);
        ProjectService projectService = PowerMockito.mock(ProjectService.class);
        Mockito.when(projectService.getStorageVolumeInfoResponse(project)).thenReturn(response);
        PowerMockito.when(SpringContext.getBean(ProjectService.class)).thenReturn(projectService);

        val manager = PowerMockito.mock(NExecutableManager.class);
        PowerMockito.when(NExecutableManager.getInstance(getTestConfig(), project)).thenReturn(manager);
        MetricsRegistry.registerProjectMetrics(getTestConfig(), project, "localhost");
        MetricsRegistry.registerHostMetrics("localhost");
        List<Meter> meters = meterRegistry.getMeters();
        Assert.assertEquals(0, meters.size());

        ExecutablePO mockExecutablePO = Mockito.mock(ExecutablePO.class);
        ExecutableOutputPO mockExecutableOutputPO = Mockito.mock(ExecutableOutputPO.class);
        Mockito.when(mockExecutablePO.getOutput()).thenReturn(mockExecutableOutputPO);
        Mockito.when(mockExecutableOutputPO.getStatus()).thenReturn(ExecutableState.READY.name());
        Mockito.when(manager.getAllJobs()).thenReturn(Lists.newArrayList(mockExecutablePO));

        var result = MetricsController.getDefaultMetricRegistry()
                .getGauges(MetricFilter.contains(MetricsName.JOB_RUNNING_GAUGE.getVal()));
        Assert.assertEquals(1L, result.get(result.firstKey()).getValue());

        result = MetricsController.getDefaultMetricRegistry()
                .getGauges(MetricFilter.contains(MetricsName.JOB_ERROR_GAUGE.getVal()));
        Assert.assertEquals(0L, result.get(result.firstKey()).getValue());

        result = MetricsController.getDefaultMetricRegistry()
                .getGauges(MetricFilter.contains(MetricsName.JOB_PENDING_GAUGE.getVal()));
        Assert.assertEquals(1L, result.get(result.firstKey()).getValue());
    }

    @Test
    public void testDeletePrometheusProjectMetrics() {
        Assert.assertEquals(0, meterRegistry.getMeters().size());
        Counter counter = meterRegistry.counter(PrometheusMetrics.SPARK_TASKS.getValue(),
                Tags.of(MetricsTag.PROJECT.getVal(), "TEST"));
        counter.increment();

        Assert.assertNotEquals(0, meterRegistry.getMeters().size());
        PowerMockito.when(SpringContext.getBean(MeterRegistry.class)).thenReturn(meterRegistry);
        MetricsRegistry.deletePrometheusProjectMetrics("TEST");
        Assert.assertEquals(0, meterRegistry.getMeters().size());

        thrown.expect(IllegalArgumentException.class);
        MetricsRegistry.deletePrometheusProjectMetrics("");
    }

    @Test
    public void testRemovePrometheusModelMetrics() {
        Assert.assertEquals(0, meterRegistry.getMeters().size());
        Counter counter1 = meterRegistry.counter(PrometheusMetrics.MODEL_BUILD_DURATION.getValue(),
                Tags.of(MetricsTag.PROJECT.getVal(), "TEST", MetricsTag.MODEL.getVal(), "MODULE01"));
        Counter counter2 = meterRegistry.counter(PrometheusMetrics.MODEL_BUILD_DURATION.getValue(),
                Tags.of(MetricsTag.PROJECT.getVal(), "TEST", MetricsTag.MODEL.getVal(), "MODULE02"));
        counter1.increment();
        counter2.increment();

        Assert.assertEquals(2, meterRegistry.getMeters().size());
        PowerMockito.when(SpringContext.getBean(MeterRegistry.class)).thenReturn(meterRegistry);
        MetricsRegistry.removePrometheusModelMetrics("TEST", "MODULE01");

        Assert.assertEquals(1, meterRegistry.getMeters().size());
        List<Meter.Id> collect = meterRegistry.getMeters().stream().map(Meter::getId)
                .filter(id -> "TEST".equals(id.getTag(MetricsTag.PROJECT.getVal()))
                        && "MODULE01".equals(id.getTag(MetricsTag.MODEL.getVal())))
                .collect(Collectors.toList());

        Assert.assertEquals(0, collect.size());

        thrown.expect(IllegalArgumentException.class);
        MetricsRegistry.removePrometheusModelMetrics("", "");

        thrown.expect(IllegalArgumentException.class);
        MetricsRegistry.removePrometheusModelMetrics("project", "");
    }
}
