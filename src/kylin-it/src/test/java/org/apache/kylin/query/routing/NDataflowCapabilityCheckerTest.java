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
import org.apache.kylin.metadata.cube.cuboid.NLookupCandidate;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowCapabilityChecker;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.util.OlapContextUtil;
import org.junit.Assert;
import org.junit.Test;

public class NDataflowCapabilityCheckerTest extends NLocalWithSparkSessionTest {

    @Test
    public void testCapabilityResult() throws SqlParseException {
        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject())
                .getDataflow("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96");
        String sql = "SELECT seller_ID FROM TEST_KYLIN_FACT LEFT JOIN TEST_ACCOUNT ON SELLER_ID = ACCOUNT_ID";
        OLAPContext olapContext = OlapContextUtil.getOlapContexts(getProject(), sql).get(0);
        Map<String, String> sqlAlias2ModelNameMap = OlapContextUtil.matchJoins(dataflow.getModel(), olapContext);
        olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
        CapabilityResult result = NDataflowCapabilityChecker.check(dataflow, dataflow.getQueryableSegments(),
                olapContext.getSQLDigest(), null);
        Assert.assertNotNull(result);
        Assert.assertEquals(result.getSelectedCandidate().getCost(), result.getCost(), 0.001);
    }

    @Test
    public void testLookupMatch() throws SqlParseException {
        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject())
                .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");

        // prepare table desc snapshot path
        NTableMetadataManager.getInstance(dataflow.getConfig(), dataflow.getProject()).getTableDesc("EDW.TEST_SITES")
                .setLastSnapshotPath("default/table_snapshot/EDW.TEST_SITES/c1e8096e-4e7f-4387-b7c3-5147c1ce38d6");

        // case 1. raw-query answered by Lookup
        {
            String sql = "select SITE_ID from EDW.TEST_SITES";
            OLAPContext olapContext = OlapContextUtil.getOlapContexts(getProject(), sql).get(0);
            Map<String, String> sqlAlias2ModelNameMap = OlapContextUtil.matchJoins(dataflow.getModel(), olapContext);
            olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
            CapabilityResult result = NDataflowCapabilityChecker.check(dataflow, dataflow.getQueryableSegments(),
                    olapContext.getSQLDigest(), null);
            Assert.assertNotNull(result);
            Assert.assertTrue(result.getSelectedCandidate() instanceof NLookupCandidate);
            Assert.assertFalse(olapContext.getSQLDigest().allColumns.isEmpty());
            Assert.assertEquals(1, olapContext.getSQLDigest().allColumns.size());
        }

        // case 2. aggregate-query answered by lookup
        {
            String sql = "select sum(SITE_ID) from EDW.TEST_SITES";
            OLAPContext olapContext = OlapContextUtil.getOlapContexts(getProject(), sql).get(0);
            Map<String, String> sqlAlias2ModelNameMap = OlapContextUtil.matchJoins(dataflow.getModel(), olapContext);
            olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
            CapabilityResult result = NDataflowCapabilityChecker.check(dataflow, dataflow.getQueryableSegments(),
                    olapContext.getSQLDigest(), null);
            Assert.assertNotNull(result);
            Assert.assertTrue(result.getSelectedCandidate() instanceof NLookupCandidate);
            Assert.assertFalse(olapContext.getSQLDigest().allColumns.isEmpty());
            Assert.assertEquals(1, olapContext.getSQLDigest().allColumns.size());
        }

        {
            // case 3. cannot answer when there are no ready segment
            String sql = "select sum(SITE_ID) from EDW.TEST_SITES";
            OLAPContext olapContext = OlapContextUtil.getOlapContexts(getProject(), sql).get(0);
            removeAllSegments(dataflow);
            dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject())
                    .getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
            CapabilityResult result = NDataflowCapabilityChecker.check(dataflow, dataflow.getQueryableSegments(),
                    olapContext.getSQLDigest(), null);
            Assert.assertFalse(result.isCapable());
        }
    }

    private void removeAllSegments(NDataflow dataflow) {
        NDataflowUpdate dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        dataflowUpdate.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[0]));
        NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject()).updateDataflow(dataflowUpdate);
    }
}
