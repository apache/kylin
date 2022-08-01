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

package org.apache.kylin.query.engine;

import static org.apache.kylin.common.util.TestUtils.getTestConfig;

import java.util.Collections;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.query.calcite.KylinRelDataTypeSystem;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.query.QueryExtension;
import org.apache.kylin.query.util.QueryContextCutter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import lombok.val;

@MetadataInfo(project = "default")
class SelectRealizationTest {

    @BeforeEach
    public void setUp() throws Exception {
        // Use default Factory for Open Core
        QueryExtension.setFactory(new QueryExtension.Factory());
    }

    @AfterEach
    public void tearDown() throws Exception {
        // Unset Factory for Open Core
        QueryExtension.setFactory(null);
    }

    @Test
    void testDerivedFromSameContext() throws SqlParseException {
        val kylinConfig = getTestConfig();
        val config = KECalciteConfig.fromKapConfig(kylinConfig);
        val schemaFactory = new ProjectSchemaFactory("default", kylinConfig);
        val rootSchema = schemaFactory.createProjectRootSchema();
        String defaultSchemaName = schemaFactory.getDefaultSchema();
        val catalogReader = createCatalogReader(config, rootSchema, defaultSchemaName);
        val planner = new PlannerFactory(kylinConfig).createVolcanoPlanner(config);
        val sqlConverter = SQLConverter.createConverter(config, planner, catalogReader);
        val queryOptimizer = new QueryOptimizer(planner);
        RelRoot relRoot = sqlConverter
                .convertSqlToRelNode("SELECT count(1)\n" + "FROM \"SSB\".\"LINEORDER\" \"LINEORDER\"\n"
                        + "JOIN (SELECT MAX(\"LINEORDER\".\"LO_ORDERDATE\") AS \"X_measure__0\"\n"
                        + "FROM \"SSB\".\"LINEORDER\" \"LINEORDER\"\n" + "GROUP BY 1.1000000000000001 ) \"t0\"\n"
                        + "ON LINEORDER.LO_ORDERDATE = t0.X_measure__0\n" + "GROUP BY 1.1000000000000001");
        RelNode node = queryOptimizer.optimize(relRoot).rel;
        val olapContexts = QueryContextCutter.selectRealization(node, BackdoorToggles.getIsQueryFromAutoModeling());
        Assertions.assertNotNull(olapContexts);
        Assertions.assertFalse(olapContexts.isEmpty());
    }

    private Prepare.CatalogReader createCatalogReader(CalciteConnectionConfig connectionConfig,
            CalciteSchema rootSchema, String defaultSchemaName) {
        RelDataTypeSystem relTypeSystem = new KylinRelDataTypeSystem();
        JavaTypeFactory javaTypeFactory = new JavaTypeFactoryImpl(relTypeSystem);
        return new CalciteCatalogReader(rootSchema, Collections.singletonList(defaultSchemaName), javaTypeFactory,
                connectionConfig);
    }
}
