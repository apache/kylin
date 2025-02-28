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
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NDataflowUpdate;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.util.OlapContextTestUtil;
import org.junit.Assert;
import org.junit.Test;

public class DataflowCapabilityCheckerTest extends NLocalWithSparkSessionTest {

    @Test
    public void testCapabilityResult() throws SqlParseException {
        NDataflow dataflow = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject())
                .getDataflow("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96");
        String sql = "SELECT seller_ID FROM TEST_KYLIN_FACT LEFT JOIN TEST_ACCOUNT ON SELLER_ID = ACCOUNT_ID";
        OlapContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
        Map<String, String> sqlAlias2ModelNameMap = OlapContextTestUtil.matchJoins(dataflow.getModel(), olapContext);
        olapContext.fixModel(dataflow.getModel(), sqlAlias2ModelNameMap);
        Candidate candidate = new Candidate(dataflow, olapContext, sqlAlias2ModelNameMap);
        CapabilityResult result = DataflowCapabilityChecker.check(dataflow, candidate, olapContext.getSQLDigest());
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
            OlapContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
            NLayoutCandidate candidate = olapContext.getStorageContext().getBatchCandidate();
            CapabilityResult result = candidate.getCapabilityResult();
            Assert.assertNotNull(result);
            Assert.assertNotNull(olapContext.getStorageContext().getLookupCandidate());
            Assert.assertFalse(olapContext.getSQLDigest().getAllColumns().isEmpty());
            Assert.assertEquals(1, olapContext.getSQLDigest().getAllColumns().size());
        }

        // case 2. aggregate-query answered by lookup
        {
            String sql = "select sum(SITE_ID) from EDW.TEST_SITES";
            OlapContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
            NLayoutCandidate candidate = olapContext.getStorageContext().getBatchCandidate();
            CapabilityResult result = candidate.getCapabilityResult();
            Assert.assertNotNull(result);
            Assert.assertNotNull(olapContext.getStorageContext().getLookupCandidate());
            Assert.assertFalse(olapContext.getSQLDigest().getAllColumns().isEmpty());
            Assert.assertEquals(1, olapContext.getSQLDigest().getAllColumns().size());
        }

        // case 3. can answer by snapshot when there are no ready segment
        {
            removeAllSegments(dataflow);
            String sql = "select sum(SITE_ID) from EDW.TEST_SITES";
            OlapContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
            NLayoutCandidate candidate = olapContext.getStorageContext().getBatchCandidate();
            CapabilityResult result = candidate.getCapabilityResult();
            Assert.assertNotNull(result);
            Assert.assertNotNull(olapContext.getStorageContext().getLookupCandidate());
            Assert.assertFalse(olapContext.getSQLDigest().getAllColumns().isEmpty());
            Assert.assertEquals(1, olapContext.getSQLDigest().getAllColumns().size());
        }

        // case 4. no snapshot
        {
            NTableMetadataManager.getInstance(dataflow.getConfig(), getProject()).updateTableDesc("EDW.TEST_SITES",
                    copyForWrite -> copyForWrite.setLastSnapshotPath(""));
            String sql = "select sum(SITE_ID) from EDW.TEST_SITES";
            OlapContext olapContext = OlapContextTestUtil.getOlapContexts(getProject(), sql).get(0);
            NLayoutCandidate candidate = olapContext.getStorageContext().getBatchCandidate();
            CapabilityResult result = candidate.getCapabilityResult();
            Assert.assertNotNull(result);
            Assert.assertNull(olapContext.getStorageContext().getLookupCandidate());
        }
    }

    private void removeAllSegments(NDataflow dataflow) {
        NDataflowUpdate dataflowUpdate = new NDataflowUpdate(dataflow.getUuid());
        dataflowUpdate.setToRemoveSegs(dataflow.getSegments().toArray(new NDataSegment[0]));
        NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject()).updateDataflow(dataflowUpdate);
    }
}
