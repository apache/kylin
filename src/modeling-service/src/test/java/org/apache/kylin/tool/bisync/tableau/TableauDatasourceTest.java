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

package org.apache.kylin.tool.bisync.tableau;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.base.Charsets;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.io.CharStreams;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.tool.bisync.SyncModelBuilder;
import org.apache.kylin.tool.bisync.SyncModelTestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.val;

public class TableauDatasourceTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testModelDescAddMeasuresComment() {
        KylinConfig testConfig = getTestConfig();
        val modelMgr = NDataModelManager.getInstance(testConfig, "default");

        NDataModel dataModelDesc = modelMgr.getDataModelDesc("cb596712-3a09-46f8-aea1-988b43fe9b6c");
        List<NDataModel.Measure> allMeasures = dataModelDesc.getAllMeasures();
        allMeasures.get(1).setComment("求和");
        modelMgr.updateDataModelDesc(dataModelDesc);

        dataModelDesc = modelMgr.getDataModelDesc("cb596712-3a09-46f8-aea1-988b43fe9b6c");
        Assert.assertEquals("求和", dataModelDesc.getAllMeasures().get(1).getComment());
    }

    @Test
    public void testTableauDataSource() throws IOException {
        testModelDescAddMeasuresComment();
        val project = "default";
        val modelId = "cb596712-3a09-46f8-aea1-988b43fe9b6c";
        val syncContext = SyncModelTestUtil.createSyncContext(project, modelId, KylinConfig.getInstanceFromEnv());
        val syncModel = new SyncModelBuilder(syncContext).buildSourceSyncModel(ImmutableList.of(), ImmutableList.of());

        TableauDatasourceModel datasource = new TableauDataSourceConverter().convert(syncModel, syncContext);
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        datasource.dump(outStream);
        Assert.assertEquals(getExpectedTds("/bisync_tableau/nmodel_full_measure_test.tds"),
                outStream.toString(Charset.defaultCharset().name()));
    }

    @Test
    public void testTableauDataConnectorSource() throws IOException {
        val project = "default";
        val modelId = "cb596712-3a09-46f8-aea1-988b43fe9b6c";
        val syncContext = SyncModelTestUtil.createSyncContext(project, modelId, KylinConfig.getInstanceFromEnv());
        val syncModel = new SyncModelBuilder(syncContext).buildSourceSyncModel(ImmutableList.of(), ImmutableList.of());

        TableauDatasourceModel datasource = new TableauDataSourceConverter().convert(syncModel, syncContext);
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        datasource.dump(outStream);
        Assert.assertEquals(getExpectedTds("/bisync_tableau/nmodel_full_measure_test.connector.tds"),
                outStream.toString(Charset.defaultCharset().name()));
    }

    @Test
    public void testDataTypeConversion() {
        Assert.assertEquals("real", TableauDataSourceConverter.TypeConverter.convertKylinType(" deciMAL(18,2) "));
        Assert.assertEquals("real", TableauDataSourceConverter.TypeConverter.convertKylinType("decimal(18,2)"));
        Assert.assertEquals("real", TableauDataSourceConverter.TypeConverter.convertKylinType("double"));
        Assert.assertEquals("real", TableauDataSourceConverter.TypeConverter.convertKylinType("float"));
        Assert.assertEquals("real", TableauDataSourceConverter.TypeConverter.convertKylinType("real"));
        Assert.assertEquals("integer", TableauDataSourceConverter.TypeConverter.convertKylinType("integer"));
        Assert.assertEquals("integer", TableauDataSourceConverter.TypeConverter.convertKylinType("bigint"));
        Assert.assertEquals("integer", TableauDataSourceConverter.TypeConverter.convertKylinType("smallint"));
        Assert.assertEquals("integer", TableauDataSourceConverter.TypeConverter.convertKylinType("tinyint"));
        Assert.assertEquals("string", TableauDataSourceConverter.TypeConverter.convertKylinType("char(12)"));
        Assert.assertEquals("string", TableauDataSourceConverter.TypeConverter.convertKylinType("varchar(12)"));
        Assert.assertEquals("string", TableauDataSourceConverter.TypeConverter.convertKylinType("string"));
        Assert.assertEquals("date", TableauDataSourceConverter.TypeConverter.convertKylinType("date"));
        Assert.assertEquals("datetime", TableauDataSourceConverter.TypeConverter.convertKylinType("datetime"));
        Assert.assertEquals("boolean", TableauDataSourceConverter.TypeConverter.convertKylinType("boolean"));
        Assert.assertEquals("integer", TableauDataSourceConverter.TypeConverter.convertKylinType("hllc(12)"));
    }

    private String getExpectedTds(String path) throws IOException {
        return CharStreams.toString(new InputStreamReader(getClass().getResourceAsStream(path), Charsets.UTF_8));
    }
}
