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

package org.apache.kylin.query;

import java.util.stream.Collectors;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.kylin.common.NativeQueryRealization;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.query.engine.QueryExec;
import org.apache.kylin.query.engine.TypeSystem;
import org.apache.kylin.query.engine.meta.SimpleDataContext;
import org.apache.kylin.query.relnode.ContextUtil;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.query.routing.RealizationChooser;
import org.apache.kylin.query.schema.OlapSchema;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import lombok.val;

public class StreamingOlapContextTest extends NLocalWithSparkSessionTest {

    @Before
    public void setUp() throws Exception {
        staticCreateTestMetadata("src/test/resources/ut_meta/streaming");
        getTestConfig().setProperty("kylin.streaming.enabled", "true");
    }

    @Override
    public String getProject() {
        return "streaming_test2";
    }

    @Test
    public void testHybridRealizationContext() {
        /*
         * hit both batch and streaming
         */
        String project = getProject();
        String sql = "select count(*) from SSB_STREAMING";
        val proposeContext = AccelerationUtil.runWithSmartContext(getTestConfig(), project, new String[] { sql }, true);
        OlapContext context = Lists
                .newArrayList(proposeContext.getModelContexts().get(0).getModelTree().getOlapContexts()).get(0);
        setExecutor(context);
        OlapSchema olapSchema = context.getOlapSchema();
        OlapSchema newSchema = new OlapSchema(getTestConfig().base(), olapSchema.getProject(),
                olapSchema.getSchemaName(), olapSchema.getTables(), olapSchema.getModelsMap());
        context.setOlapSchema(newSchema);
        RealizationChooser.attemptSelectCandidate(context);
        ContextUtil.registerContext(context);

        val nativeQueryRealizations = ContextUtil.getNativeRealizations();
        Assert.assertEquals(2, nativeQueryRealizations.size());

        val modelIdAliasMap = nativeQueryRealizations.stream()
                .collect(Collectors.toMap(NativeQueryRealization::getModelId, NativeQueryRealization::getModelAlias));

        Assert.assertEquals(2, modelIdAliasMap.size());
        Assert.assertEquals("model_streaming", modelIdAliasMap.get("4965c827-fbb4-4ea1-a744-3f341a3b030d"));
        Assert.assertEquals("model_streaming", modelIdAliasMap.get("cd2b9a23-699c-4699-b0dd-38c9412b3dfd"));
    }

    @Test
    public void testOnlyBatchRealizationContextOnHybridModel() {
        /*
         * hit batch model only,
         * the fact table is hive table
         */
        String project = getProject();
        String sql = "select LO_ORDERKEY, count(*) from SSB.LINEORDER_HIVE group by LO_ORDERKEY";
        val proposeContext = AccelerationUtil.runWithSmartContext(getTestConfig(), project, new String[] { sql }, true);
        OlapContext context = Lists
                .newArrayList(proposeContext.getModelContexts().get(0).getModelTree().getOlapContexts()).get(0);
        setExecutor(context);
        OlapSchema olapSchema = context.getOlapSchema();
        OlapSchema newSchema = new OlapSchema(getTestConfig().base(), olapSchema.getProject(),
                olapSchema.getSchemaName(), olapSchema.getTables(), olapSchema.getModelsMap());
        context.setOlapSchema(newSchema);
        RealizationChooser.attemptSelectCandidate(context);
        ContextUtil.registerContext(context);

        val nativeQueryRealizations = ContextUtil.getNativeRealizations();
        Assert.assertEquals(1, nativeQueryRealizations.size());

        val nativeQueryRealization = nativeQueryRealizations.get(0);

        Assert.assertEquals("cd2b9a23-699c-4699-b0dd-38c9412b3dfd", nativeQueryRealization.getModelId());
        Assert.assertEquals("model_streaming", nativeQueryRealization.getModelAlias());
    }

    private void setExecutor(OlapContext context) {
        CalciteSchema rootSchema = new QueryExec(getProject(), getTestConfig()).getRootSchema();
        SimpleDataContext dataContext = new SimpleDataContext(rootSchema.plus(), TypeSystem.javaTypeFactory(),
                getTestConfig());
        context.getFirstTableScan().getCluster().getPlanner().setExecutor(new RexExecutorImpl(dataContext));
    }
}
