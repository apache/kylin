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
package org.apache.kylin.query.routing;

import java.util.Map;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.util.OlapContextTestUtil;
import org.junit.Assert;
import org.junit.Test;

public class KylinTableChooserRuleTest extends NLocalWithSparkSessionTest {

    @Override
    public String getProject() {
        return "storage_v3_test";
    }

    @Test
    public void testCapabilityResult() throws SqlParseException {
        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject())
                .getDataflow("7d840904-7b34-4edd-aabd-79df992ef32e");
        String sql = "SELECT SUM(PRICE) FROM TEST_KYLIN_FACT GROUP BY CAL_DT";
        OlapContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
        Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
        olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
        KylinTableChooserRule rule = new KylinTableChooserRule();
        CapabilityResult result = rule.check(dataflow, olapContext, olapContext.getSQLDigest());
        Assert.assertNotNull(result);
        Assert.assertEquals(result.getSelectedCandidate().getCost(), result.getCost(), 0.001);
    }

    @Test
    public void testSelectLookup() throws SqlParseException {
        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject())
                .getDataflow("7d840904-7b34-4edd-aabd-79df992ef32e");
        String sql = "SELECT ORDER_ID FROM TEST_ORDER";
        OlapContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
        KylinTableChooserRule rule = new KylinTableChooserRule();
        CapabilityResult result = rule.check(dataflow, olapContext, olapContext.getSQLDigest());
        Assert.assertNotNull(result);
        Assert.assertTrue(((NLayoutCandidate) result.getSelectedCandidate()).isEmpty());
    }
}
