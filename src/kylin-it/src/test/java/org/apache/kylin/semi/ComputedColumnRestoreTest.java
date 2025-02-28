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

package org.apache.kylin.semi;

import java.util.List;
import java.util.Set;

import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;
import org.apache.kylin.metadata.recommendation.candidate.JdbcRawRecStore;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.RawRecService;
import org.apache.kylin.util.MetadataTestUtils;
import org.apache.kylin.util.SemiAutoTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

public class ComputedColumnRestoreTest extends SemiAutoTestBase {

    private NDataModelManager modelManager;
    private JdbcRawRecStore jdbcRawRecStore;
    private RDBMSQueryHistoryDAO queryHistoryDAO;

    @InjectMocks
    private final RawRecService rawRecService = new RawRecService();

    @Override
    public String getProject() {
        return "ssb";
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
        prepareData();
        jdbcRawRecStore = new JdbcRawRecStore(getTestConfig());
        modelManager = NDataModelManager.getInstance(getTestConfig(), getProject());
        queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();
        prepareACL();
    }

    private void prepareACL() {
        TestingAuthenticationToken auth = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }

    @After
    public void teardown() throws Exception {
        queryHistoryDAO.deleteAllQueryHistory();
        super.tearDown();
    }

    @Test
    public void testRecCCWithKeywords() {
        //prepare model
        AbstractContext smartContext = AccelerationUtil.runWithSmartContext(getTestConfig(), getProject(),
                new String[] { "SELECT sum(\"hour\"+1) from SSB.CUSTOMER" }, true);

        // assert model exists
        Set<String> strings = modelManager.listAllModelIds();
        Assert.assertEquals(1, strings.size());

        String uuid = smartContext.getModelContexts().get(0).getTargetModel().getUuid();

        // check cc
        ComputedColumnDesc ccNew = modelManager.getDataModelDesc(uuid).getComputedColumnDescs().get(0);
        Assert.assertNotNull(ccNew);
        String ccName = ccNew.getColumnName();

        MetadataTestUtils.toSemiAutoMode(getProject());

        //  select sum(hour_1 + 1) from ssb.customer
        //  propose cc using the column named by keyword
        String[] sql = new String[] { String.join("", "SELECT SUM(", ccName, " + 1) from SSB.CUSTOMER") };
        AbstractContext context2 = AccelerationUtil.genOptRec(getTestConfig(), getProject(), sql);
        rawRecService.transferAndSaveRecommendations(context2);
        List<RawRecItem> rawRecItems = jdbcRawRecStore.queryAll();
        Assert.assertEquals(3, rawRecItems.size());

    }

    private void prepareData() {
        //prepare data, create table, then add a column named with sql keyword
        String tableName = "SSB.CUSTOMER";
        UnitOfWork.doInTransactionWithRetry(() -> {
            NTableMetadataManager tableManager = NTableMetadataManager.getInstance(getTestConfig(), getProject());
            tableManager.updateTableDesc(tableName, copyForWrite -> {
                ColumnDesc[] columns = copyForWrite.getColumns();
                columns[0].setName("hour");
                copyForWrite.setColumns(columns);
            });
            return true;
        }, getProject());
    }

}
