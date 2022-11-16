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

package org.apache.kylin.rest.util;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.query.NativeQueryRealization;
import org.apache.kylin.metadata.query.QueryMetricsContext;
import org.apache.kylin.rest.response.SQLResponse;
import org.apache.kylin.rest.service.CacheSignatureQuerySupporter;
import org.apache.kylin.rest.service.QueryService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.collect.Lists;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ SpringContext.class, UserGroupInformation.class })
public class QueryCacheSignatureUtilTest extends NLocalFileMetadataTestCase {
    private String project = "cache";
    private String modelId = "8c670664-8d05-466a-802f-83c023b56c77";
    private Long layoutId = 10001L;
    private SQLResponse response = new SQLResponse();
    private NDataflowManager dataflowManager;
    private NDataflow dataflow;

    @Before
    public void setup() throws Exception {

        //ReflectionTestUtils.setField(QueryCacheSignatureUtil, "queryService", queryService);
        PowerMockito.mockStatic(SpringContext.class);
        PowerMockito.mockStatic(UserGroupInformation.class);
        UserGroupInformation userGroupInformation = Mockito.mock(UserGroupInformation.class);
        PowerMockito.when(UserGroupInformation.getCurrentUser()).thenReturn(userGroupInformation);
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        this.createTestMetadata("src/test/resources/ut_cache");
        List<NativeQueryRealization> nativeRealizations = Lists
                .newArrayList(new NativeQueryRealization(modelId, layoutId, "TEST", Lists.newArrayList()));
        response.setNativeRealizations(nativeRealizations);
        QueryService queryService = PowerMockito.mock(QueryService.class);
        //PowerMockito.when(SpringContext.getBean(QueryService.class)).thenReturn(queryService);
        PowerMockito.when(SpringContext.getBean(CacheSignatureQuerySupporter.class)).thenReturn(queryService);
        PowerMockito.when(queryService.onCreateAclSignature(project)).thenReturn("root");
        response.setSignature(QueryCacheSignatureUtil.createCacheSignature(response, project));
        dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        dataflow = dataflowManager.getDataflow(modelId);

    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testMultiRealizations() {
        SQLResponse response = new SQLResponse();
        List<NativeQueryRealization> multiRealizations = Lists.newArrayList(
                new NativeQueryRealization(modelId, layoutId, "TEST", Lists.newArrayList()),
                new NativeQueryRealization(modelId, 10002L, "TEST", Lists.newArrayList()));
        response.setNativeRealizations(multiRealizations);
        String cacheSignature = QueryCacheSignatureUtil.createCacheSignature(response, project);
        Assert.assertEquals("1538323200000_1538323200000", cacheSignature.split(",")[1].split(";")[0]);
        Assert.assertEquals("1538323300000_1538323300000", cacheSignature.split(",")[2].split(";")[0]);
    }

    @Test
    public void testMultiRealizationsWhenDeleteLayout() {
        SQLResponse response = new SQLResponse();
        List<NativeQueryRealization> multiRealizations = Lists.newArrayList(
                new NativeQueryRealization(modelId, layoutId, "TEST"),
                new NativeQueryRealization(modelId, 10002L, "TEST"));
        response.setNativeRealizations(multiRealizations);
        NDataflowUpdate update = new NDataflowUpdate(modelId);
        update.setToRemoveLayouts(dataflowManager.getDataflow(modelId).getFirstSegment().getLayout(10002L));
        update.setToRemoveLayouts(dataflowManager.getDataflow(modelId).getLastSegment().getLayout(10002L));
        dataflowManager.updateDataflow(update);
        response.setSignature(QueryCacheSignatureUtil.createCacheSignature(response, project));
        Assert.assertTrue(QueryCacheSignatureUtil.checkCacheExpired(response, project));
    }

    @Test
    public void testCreateCacheSignature() {
        Assert.assertEquals("1538323200000_1538323200000", response.getSignature().split(",")[1].split(";")[0]);
    }

    @Test
    public void testCheckCacheExpiredWhenUpdateLayout() {
        NDataLayout layout = NDataLayout.newDataLayout(dataflow, dataflow.getSegments().getFirstSegment().getId(),
                layoutId);
        NDataflowUpdate update = new NDataflowUpdate(modelId);
        update.setToAddOrUpdateLayouts(layout);
        dataflowManager.updateDataflow(update);
        Assert.assertTrue(QueryCacheSignatureUtil.checkCacheExpired(response, project));
    }

    @Test
    public void testCheckCacheExpiredWhenUpdateOtherLayout() throws InterruptedException {
        Long otherLayout = 10002L;
        NDataLayout layout = NDataLayout.newDataLayout(dataflow, dataflow.getSegments().getFirstSegment().getId(),
                otherLayout);
        NDataflowUpdate update = new NDataflowUpdate(modelId);
        update.setToAddOrUpdateLayouts(layout);
        dataflowManager.updateDataflow(update);
        Thread.sleep(10L);
        Assert.assertTrue(QueryCacheSignatureUtil.checkCacheExpired(response, project));
    }

    @Test
    public void testCheckCacheExpiredWhenAddSegment() {
        SegmentRange.TimePartitionedSegmentRange timePartitionedSegmentRange = new SegmentRange.TimePartitionedSegmentRange(
                883612800000L, 1275321600000L);
        NDataSegment nDataSegment = dataflowManager.appendSegment(dataflowManager.getDataflow(modelId),
                timePartitionedSegmentRange);
        nDataSegment.setStatus(SegmentStatusEnum.READY);
        NDataLayout layout = NDataLayout.newDataLayout(dataflow, nDataSegment.getId(), layoutId);
        NDataflowUpdate update = new NDataflowUpdate(modelId);
        update.setToAddSegs(nDataSegment);
        update.setToAddOrUpdateLayouts(layout);
        dataflowManager.updateDataflow(update);
        Assert.assertTrue(QueryCacheSignatureUtil.checkCacheExpired(response, project));
    }

    @Test
    public void testCacheSignatureWhenHitSnapshotBasic() throws IOException {
        String project = "default";
        SQLResponse sqlResponse = new SQLResponse();
        List<NativeQueryRealization> nativeRealizations = Lists.newArrayList(new NativeQueryRealization(
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", -1L, QueryMetricsContext.TABLE_SNAPSHOT,
                Lists.newArrayList("DEFAULT.TEST_ORDER")));
        sqlResponse.setNativeRealizations(nativeRealizations);
        QueryService queryService = PowerMockito.mock(QueryService.class);
        PowerMockito.when(SpringContext.getBean(CacheSignatureQuerySupporter.class)).thenReturn(queryService);
        PowerMockito.when(queryService.onCreateAclSignature(project)).thenReturn("root");
        sqlResponse.setSignature(QueryCacheSignatureUtil.createCacheSignature(sqlResponse, project));
        Assert.assertFalse(QueryCacheSignatureUtil.checkCacheExpired(sqlResponse, project));

        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                project);
        TableDesc table = tableMetadataManager.getTableDesc("DEFAULT.TEST_ORDER");
        table.setLastModified(System.currentTimeMillis());
        Assert.assertTrue(QueryCacheSignatureUtil.checkCacheExpired(sqlResponse, project));
    }

    @Test
    public void testCacheSignatureWhenHitMultiSnapshot() throws IOException {
        String project = "default";
        SQLResponse sqlResponse = new SQLResponse();
        List<NativeQueryRealization> nativeRealizations = Lists.newArrayList(new NativeQueryRealization(
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", -1L, QueryMetricsContext.TABLE_SNAPSHOT,
                Lists.newArrayList("DEFAULT.TEST_ORDER", "DEFAULT.TEST_ACCOUNT")));
        sqlResponse.setNativeRealizations(nativeRealizations);
        QueryService queryService = PowerMockito.mock(QueryService.class);
        PowerMockito.when(SpringContext.getBean(CacheSignatureQuerySupporter.class)).thenReturn(queryService);
        PowerMockito.when(queryService.onCreateAclSignature(project)).thenReturn("root");
        sqlResponse.setSignature(QueryCacheSignatureUtil.createCacheSignature(sqlResponse, project));
        Assert.assertFalse(QueryCacheSignatureUtil.checkCacheExpired(sqlResponse, project));

        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                project);
        TableDesc table = tableMetadataManager.getTableDesc("DEFAULT.TEST_ORDER");
        table.setLastModified(System.currentTimeMillis());
        Assert.assertTrue(QueryCacheSignatureUtil.checkCacheExpired(sqlResponse, project));
    }

    @Test
    public void testCacheSignatureWhenHitSnapshotMultiModel() throws IOException {
        String project = "default";
        SQLResponse sqlResponse = new SQLResponse();
        List<NativeQueryRealization> nativeRealizations = Lists.newArrayList(new NativeQueryRealization(
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", -1L, QueryMetricsContext.TABLE_SNAPSHOT,
                Lists.newArrayList("DEFAULT.TEST_ORDER")), new NativeQueryRealization(
                "89af4ee2-2cdb-4b07-b39e-4c29856309aa", 1000001L, QueryMetricsContext.AGG_INDEX, Lists.newArrayList()));
        sqlResponse.setNativeRealizations(nativeRealizations);
        QueryService queryService = PowerMockito.mock(QueryService.class);
        //PowerMockito.when(SpringContext.getBean(QueryService.class)).thenReturn(queryService);
        PowerMockito.when(SpringContext.getBean(CacheSignatureQuerySupporter.class)).thenReturn(queryService);
        PowerMockito.when(queryService.onCreateAclSignature(project)).thenReturn("root");
        sqlResponse.setSignature(QueryCacheSignatureUtil.createCacheSignature(sqlResponse, project));
        Assert.assertFalse(QueryCacheSignatureUtil.checkCacheExpired(sqlResponse, project));

        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(),
                project);
        TableDesc table = tableMetadataManager.getTableDesc("DEFAULT.TEST_ORDER");
        table.setLastModified(System.currentTimeMillis());
        Assert.assertTrue(QueryCacheSignatureUtil.checkCacheExpired(sqlResponse, project));
    }

    @Test
    public void testCacheSignatureWhenSegmentOutOfRange() {
        SQLResponse sqlResponse = new SQLResponse();
        List<NativeQueryRealization> nativeRealizations = Lists.newArrayList(new NativeQueryRealization(
                modelId, -1L, null,
                Lists.newArrayList()));
        sqlResponse.setNativeRealizations(nativeRealizations);
        sqlResponse.setSignature(QueryCacheSignatureUtil.createCacheSignature(sqlResponse, project));
        Assert.assertFalse(QueryCacheSignatureUtil.checkCacheExpired(sqlResponse, project));

        SegmentRange.TimePartitionedSegmentRange timePartitionedSegmentRange = new SegmentRange.TimePartitionedSegmentRange(
                883612800000L, 1275321600000L);
        NDataSegment nDataSegment = dataflowManager.appendSegment(dataflowManager.getDataflow(modelId),
                timePartitionedSegmentRange);
        nDataSegment.setStatus(SegmentStatusEnum.READY);
        NDataLayout layout = NDataLayout.newDataLayout(dataflow, nDataSegment.getId(), 1000001L);
        NDataflowUpdate update = new NDataflowUpdate(modelId);
        update.setToAddSegs(nDataSegment);
        update.setToAddOrUpdateLayouts(layout);
        dataflowManager.updateDataflow(update);
        Assert.assertTrue(QueryCacheSignatureUtil.checkCacheExpired(sqlResponse, project));
    }
}
