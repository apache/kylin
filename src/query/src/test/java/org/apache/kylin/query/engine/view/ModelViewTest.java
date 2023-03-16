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

package org.apache.kylin.query.engine.view;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.LinkedHashMap;

import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.query.QueryExtension;
import org.apache.kylin.query.engine.QueryExec;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.kylin.guava30.shaded.common.collect.Lists;

import lombok.val;

public class ModelViewTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() {
        overwriteSystemProp("kylin.query.auto-model-view-enabled", "TRUE");
        this.createTestMetadata();
        // Use default Factory for Open Core
        QueryExtension.setFactory(new QueryExtension.Factory());
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
        // Unset Factory for Open Core
        QueryExtension.setFactory(null);
    }

    private void createProject(String project) throws IOException {
        val projMgr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        if (projMgr.getProject(project) == null) {
            NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).createProject(project, "ADMIN", "",
                    new LinkedHashMap<>());
        }
        // copy tables from project default
        val ssbMgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        val tableMgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        for (TableDesc tableDesc : ssbMgr.listAllTables()) {
            if (tableDesc.getDatabase().equalsIgnoreCase("SSB")) {
                if (tableMgr.getTableDesc(tableDesc.getName()) == null) {
                    val clone = new TableDesc(tableDesc);
                    clone.setMvcc(-1);
                    tableMgr.saveSourceTable(clone);
                }
            }
        }

        val contents = StringUtils
                .join(Files.readAllLines(new File("src/test/resources/ut_meta/view/DEFAULT.TEST_DECIMAL.json").toPath(),
                        Charset.defaultCharset()), "\n");
        val bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        val decimalTableDesc = tableMgr.getTableMetadataSerializer().deserialize(new DataInputStream(bais));
        decimalTableDesc.setMvcc(-1);
        tableMgr.saveSourceTable(decimalTableDesc);
    }

    private NDataModel createModel(String project, String modelAlias) throws IOException {
        val mgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val serializer = mgr.getDataModelSerializer();
        val contents = StringUtils
                .join(Files.readAllLines(new File("src/test/resources/ut_meta/view/" + modelAlias + ".json").toPath(),
                        Charset.defaultCharset()), "\n");
        val bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        val deserialized = serializer.deserialize(new DataInputStream(bais));
        deserialized.setProject(project);
        val model = mgr.createDataModelDesc(deserialized, "ADMIN");

        val emptyIndex = new IndexPlan();
        emptyIndex.setUuid(model.getUuid());
        NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project).createIndexPlan(emptyIndex);

        val df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).createDataflow(emptyIndex,
                model.getOwner());
        NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateDataflowStatus(df.getId(),
                RealizationStatusEnum.ONLINE);

        return model;
    }

    private void assertView(String project, String modelAlias) throws IOException {
        val model = createModel(project, modelAlias);

        // assert generated sql
        val expectedSQL = StringUtils
                .join(Files.readAllLines(new File("src/test/resources/ut_meta/view/" + modelAlias + ".sql").toPath(),
                        Charset.defaultCharset()), "")
                .replace("  ", " "); // remove extra spaces
        val generated = new ModelViewGenerator(model).generateViewSQL().replace("  ", " "); // remove extra spaces
        Assert.assertEquals(String.format("%s view sql generated unexpected sql", modelAlias), expectedSQL.trim(),
                generated);

        // assert schema
        val rootSchema = new QueryExec(project, KylinConfig.getInstanceFromEnv()).getRootSchema();
        Assert.assertNotNull(String.format("%s view sql generated unexpected schema", modelAlias),
                rootSchema.getSubSchema(project, false).getTableBasedOnNullaryFunction(modelAlias, false).getTable());

        // check view parsing
        try {
            new QueryExec(project, KylinConfig.getInstanceFromEnv())
                    .parseAndOptimize(String.format("select * from %s.%s", project, modelAlias));
        } catch (SqlParseException e) {
            Assert.fail(String.format("%s failed sql parsing %s", modelAlias, e));
        }
    }

    @Test
    public void testConfig() throws IOException {
        overwriteSystemProp("kylin.query.auto-model-view-enabled", "FALSE");
        val projectName = "SSB_TEST";
        createProject(projectName);
        createModel(projectName, "model_single_table");
        val schemaBefore = new QueryExec(projectName, KylinConfig.getInstanceFromEnv()).getRootSchema()
                .getSubSchema(projectName, false);
        Assert.assertNull(schemaBefore);

        overwriteSystemProp("kylin.query.auto-model-view-enabled", "TRUE");
        val schemaAfter = new QueryExec(projectName, KylinConfig.getInstanceFromEnv()).getRootSchema()
                .getSubSchema(projectName, false);
        Assert.assertNotNull(schemaAfter);
    }

    @Test
    public void testModelViews() throws IOException {
        val views = Lists.newArrayList("model_single_table", "model_joins", "model_cc");
        val projectName = "SSB_TEST";
        createProject(projectName);
        for (String view : views) {
            assertView(projectName, view);
        }
    }

    @Test
    public void testDBNameCollision() throws IOException {
        // same db, different table name
        val views = Lists.newArrayList("model_single_table", "model_joins", "model_cc");
        val projectName = "SSB";
        createProject(projectName);
        for (String view : views) {
            assertView(projectName, view);
        }

        // same db, same name
        val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), projectName);
        val model = modelMgr.getDataModelDescByAlias("model_single_table");
        modelMgr.updateDataModel(model.getId(), (m) -> m.setAlias("LINEORDER"));
        val rootSchema = new QueryExec(projectName, KylinConfig.getInstanceFromEnv()).getRootSchema()
                .getSubSchema(projectName, false);
        // assert model view disappears
        Assert.assertNull(rootSchema.getTable("model_single_table", false));
        // assert lineorder is not the model view table
        Assert.assertNotEquals(ViewTable.class, rootSchema.getTable("LINEORDER", false).getTable().getClass());
    }

    // see AL-5321
    @Test
    public void testModelViewsDeicmal() throws IOException, SqlParseException {
        val projectName = "DECIAML_TEST";
        val modelName = "model_decimal";
        createProject(projectName);
        createModel(projectName, "model_decimal");

        val relNode = new QueryExec(projectName, KylinConfig.getInstanceFromEnv())
                .parseAndOptimize(String.format("select sum(PRICE) from %s.%s", projectName, modelName));
        Assert.assertEquals("DECIMAL(35, 6)", relNode.getRowType().getFieldList().get(0).getType().toString());
    }
}
