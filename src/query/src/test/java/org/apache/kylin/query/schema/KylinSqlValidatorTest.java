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

package org.apache.kylin.query.schema;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.query.QueryExtension;
import org.apache.kylin.query.engine.QueryExec;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.val;

public class KylinSqlValidatorTest extends NLocalFileMetadataTestCase {

    private final String PROJECT = "tpch";

    @Before
    public void setup() throws IOException {
        createTestMetadata();
        val mgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
        val serializer = mgr.getDataModelSerializer();
        val contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/validator/model.json").toPath(), Charset.defaultCharset()), "\n");
        val bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        val deserialized = serializer.deserialize(new DataInputStream(bais));
        deserialized.setProject(PROJECT);
        val model = mgr.createDataModelDesc(deserialized, "ADMIN");

        val emptyIndex = new IndexPlan();
        emptyIndex.setUuid(model.getUuid());
        NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT).createIndexPlan(emptyIndex);

        val df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT).createDataflow(emptyIndex,
                model.getOwner());
        NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT).updateDataflowStatus(df.getId(),
                RealizationStatusEnum.ONLINE);

        // Use default Factory for Open Core
        QueryExtension.setFactory(new QueryExtension.Factory());
    }

    @After
    public void teardown() {
        this.cleanupTestMetadata();
        // Unset Factory for Open Core
        QueryExtension.setFactory(null);
    }

    private void assertExpandFields(String sql, int expectedFiledNum) {
        QueryExec queryExec = new QueryExec(PROJECT, KylinConfig.getInstanceFromEnv());
        RelNode rel = queryExec.wrapSqlTest(exec -> {
            try {
                return exec.parseAndOptimize(sql);
            } catch (SqlParseException e) {
                Assert.fail(e.toString());
                return null;
            }
        });
        Assert.assertEquals(expectedFiledNum, rel.getRowType().getFieldCount());
    }

    @Test
    public void testExpandSelectStar() {
        String[] sqls = new String[] { "select * from tpch.nation", "select t1.* from tpch.nation t1", };
        overwriteSystemProp("kylin.query.metadata.expose-computed-column", "TRUE");
        for (String sql : sqls) {
            assertExpandFields(sql, 5);
        }

        overwriteSystemProp("kylin.query.metadata.expose-computed-column", "FALSE");
        for (String sql : sqls) {
            assertExpandFields(sql, 4);
        }
    }

}
