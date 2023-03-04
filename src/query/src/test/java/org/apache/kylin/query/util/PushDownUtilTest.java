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

package org.apache.kylin.query.util;

import java.util.List;
import java.util.Properties;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.QueryErrorCode;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PushDownUtilTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testTryForcePushDown() {
        try {
            QueryParams queryParams = new QueryParams();
            queryParams.setProject("default");
            queryParams.setSelect(true);
            queryParams.setForcedToPushDown(true);
            PushDownUtil.tryIterQuery(queryParams);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(ServerErrorCode.SPARK_FAILURE.toErrorCode(), ((KylinException) e).getErrorCode());
        }
    }

    @Test
    public void testTryWithPushDownDisable() {
        try {
            overwriteSystemProp("kylin.query.pushdown-enabled", "false");
            QueryParams queryParams = new QueryParams();
            queryParams.setProject("default");
            queryParams.setSelect(true);
            queryParams.setForcedToPushDown(true);
            PushDownUtil.tryIterQuery(queryParams);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            Assert.assertEquals(QueryErrorCode.INVALID_PARAMETER_PUSH_DOWN.toErrorCode(),
                    ((KylinException) e).getErrorCode());
        }
    }

    @Test
    public void testBacktickQuote() {
        String table = "db.table";
        Assert.assertEquals("`db`.`table`", String.join(".", PushDownUtil.backtickQuote(table.split("\\."))));
    }

    @Test
    public void testMassagePushDownSql() {
        KylinConfig config = KylinConfig.createKylinConfig(new Properties());
        try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            config.setProperty("kylin.query.pushdown.converter-class-names",
                    SparkSQLFunctionConverter.class.getCanonicalName());
            String sql = "SELECT \"Z_PROVDASH_UM_ED\".\"GENDER\" AS \"GENDER\",\n"
                    + "SUM({fn CONVERT(0, SQL_BIGINT)}) AS \"sum_Calculation_336925569152049156_ok\"\n"
                    + "FROM \"POPHEALTH_ANALYTICS\".\"Z_PROVDASH_UM_ED\" \"Z_PROVDASH_UM_ED\"";

            QueryParams queryParams = new QueryParams("", sql, "default", false);
            queryParams.setKylinConfig(config);
            String massagedSql = PushDownUtil.massagePushDownSql(queryParams);
            String expectedSql = "SELECT `Z_PROVDASH_UM_ED`.`GENDER` AS `GENDER`,\n"
                    + "SUM(CAST(0 AS BIGINT)) AS `sum_Calculation_336925569152049156_ok`\n"
                    + "FROM `POPHEALTH_ANALYTICS`.`Z_PROVDASH_UM_ED` `Z_PROVDASH_UM_ED`";
            Assert.assertEquals(expectedSql, massagedSql);
        }
    }

    @Test
    public void testMassagePushDownSqlWithDoubleQuote() {
        KylinConfig config = KylinConfig.createKylinConfig(new Properties());
        String sql = "select '''',trans_id from test_kylin_fact where LSTG_FORMAT_NAME like '%''%' group by trans_id limit 2;";
        QueryParams queryParams = new QueryParams("", sql, "default", false);
        queryParams.setKylinConfig(config);
        String massagedSql = PushDownUtil.massagePushDownSql(queryParams);
        String expectedSql = "select '\\'', `TRANS_ID` from `TEST_KYLIN_FACT` where `LSTG_FORMAT_NAME` like '%\\'%' group by `TRANS_ID` limit 2";
        Assert.assertEquals(expectedSql, massagedSql);
    }

    @Test
    public void testMassagePushDownSqlWithDialectConverter() {
        KylinConfig config = KylinConfig.createKylinConfig(new Properties());
        try (KylinConfig.SetAndUnsetThreadLocalConfig autoUnset = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
            config.setProperty("kylin.query.pushdown.converter-class-names",
                    "org.apache.kylin.query.util.DialectConverter,org.apache.kylin.source.adhocquery.DoubleQuotePushDownConverter,"
                            + SparkSQLFunctionConverter.class.getCanonicalName());
            String sql = "SELECT \"Z_PROVDASH_UM_ED\".\"GENDER\" AS \"GENDER\",\n"
                    + "SUM({fn CONVERT(0, SQL_BIGINT)}) AS \"sum_Calculation_336925569152049156_ok\"\n"
                    + "FROM \"POPHEALTH_ANALYTICS\".\"Z_PROVDASH_UM_ED\" \"Z_PROVDASH_UM_ED\""
                    + " fetch first 1 rows only";

            QueryParams queryParams = new QueryParams("", sql, "default", false);
            queryParams.setKylinConfig(config);
            String massagedSql = PushDownUtil.massagePushDownSql(queryParams);
            String expectedSql = "SELECT `Z_PROVDASH_UM_ED`.`GENDER` AS `GENDER`, "
                    + "SUM(CAST(0 AS BIGINT)) AS `sum_Calculation_336925569152049156_ok`\n"
                    + "FROM `POPHEALTH_ANALYTICS`.`Z_PROVDASH_UM_ED` AS `Z_PROVDASH_UM_ED`\n" + "LIMIT 1";
            Assert.assertEquals(expectedSql, massagedSql);
        }
    }

    @Test
    public void testReplaceDoubleQuoteToSingle() {
        String sql = "select ab from table where aa = '' and bb = '''as''n'''";
        String resSql = "select ab from table where aa = '' and bb = '\\'as\\'n\\''";
        Assert.assertEquals(resSql, PushDownUtil.replaceEscapedQuote(sql));
    }

    @Test
    public void testGenerateFlatTableSql() {
        String project = "default";
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataModel model = modelManager.getDataModelDescByAlias("test_bank");
        String expected = "SELECT\n" //
                + "\"TEST_BANK_INCOME\".\"COUNTRY\" as \"TEST_BANK_INCOME_COUNTRY\"\n"
                + ", \"TEST_BANK_INCOME\".\"INCOME\" as \"TEST_BANK_INCOME_INCOME\"\n"
                + ", \"TEST_BANK_INCOME\".\"NAME\" as \"TEST_BANK_INCOME_NAME\"\n"
                + ", \"TEST_BANK_INCOME\".\"DT\" as \"TEST_BANK_INCOME_DT\"\n"
                + ", \"TEST_BANK_LOCATION\".\"COUNTRY\" as \"TEST_BANK_LOCATION_COUNTRY\"\n"
                + ", \"TEST_BANK_LOCATION\".\"OWNER\" as \"TEST_BANK_LOCATION_OWNER\"\n"
                + ", \"TEST_BANK_LOCATION\".\"LOCATION\" as \"TEST_BANK_LOCATION_LOCATION\"\n"
                + "FROM \"DEFAULT\".\"TEST_BANK_INCOME\" as \"TEST_BANK_INCOME\"\n"
                + "INNER JOIN \"DEFAULT\".\"TEST_BANK_LOCATION\" as \"TEST_BANK_LOCATION\"\n"
                + "ON \"TEST_BANK_INCOME\".\"COUNTRY\" = \"TEST_BANK_LOCATION\".\"COUNTRY\"\n" //
                + "WHERE\n" //
                + "1 = 1";
        Assert.assertEquals(expected, PushDownUtil.generateFlatTableSql(model, false));
    }

    @Test
    public void testGenerateFlatTableSqlWithCCJoin() {
        String project = "default";
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataModel model = modelManager.getDataModelDescByAlias("test_bank");
        updateModelToAddCC(project, model);
        // change join condition
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            NDataModelManager modelMgr = NDataModelManager.getInstance(kylinConfig, project);
            modelMgr.updateDataModel(model.getUuid(), copyForWrite -> {
                List<JoinTableDesc> joinTables = copyForWrite.getJoinTables();
                TableRef rootTableRef = copyForWrite.getRootFactTable();
                TblColRef cc1 = rootTableRef.getColumn("CC1");
                JoinDesc join = joinTables.get(0).getJoin();
                join.setForeignKeyColumns(new TblColRef[] { cc1 });
                join.setForeignKey(new String[] { "TEST_BANK_INCOME.CC1" });
            });
            return null;
        }, project);
        String expected = "SELECT\n" //
                + "\"TEST_BANK_INCOME\".\"COUNTRY\" as \"TEST_BANK_INCOME_COUNTRY\"\n"
                + ", \"TEST_BANK_INCOME\".\"INCOME\" as \"TEST_BANK_INCOME_INCOME\"\n"
                + ", \"TEST_BANK_INCOME\".\"NAME\" as \"TEST_BANK_INCOME_NAME\"\n"
                + ", \"TEST_BANK_INCOME\".\"DT\" as \"TEST_BANK_INCOME_DT\"\n"
                + ", \"TEST_BANK_LOCATION\".\"COUNTRY\" as \"TEST_BANK_LOCATION_COUNTRY\"\n"
                + ", \"TEST_BANK_LOCATION\".\"OWNER\" as \"TEST_BANK_LOCATION_OWNER\"\n"
                + ", \"TEST_BANK_LOCATION\".\"LOCATION\" as \"TEST_BANK_LOCATION_LOCATION\"\n"
                + "FROM \"DEFAULT\".\"TEST_BANK_INCOME\" as \"TEST_BANK_INCOME\"\n"
                + "INNER JOIN \"DEFAULT\".\"TEST_BANK_LOCATION\" as \"TEST_BANK_LOCATION\"\n"
                + "ON SUBSTRING(\"TEST_BANK_INCOME\".\"COUNTRY\", 0, 4) = \"TEST_BANK_LOCATION\".\"COUNTRY\"\n"
                + "WHERE\n" //
                + "1 = 1";
        NDataModel updatedModel = modelManager.getDataModelDesc(model.getUuid());
        Assert.assertEquals(expected, PushDownUtil.generateFlatTableSql(updatedModel, false));

    }

    @Test
    public void testGenerateFlatTableSqlWithFilterCondition() {
        String project = "default";
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataModel model = modelManager.getDataModelDescByAlias("test_bank");
        updateModelToAddCC(project, model);
        // change filter condition
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            NDataModelManager modelMgr = NDataModelManager.getInstance(kylinConfig, project);
            modelMgr.updateDataModel(model.getUuid(), copyForWrite -> {
                copyForWrite.setFilterCondition(
                        "SUBSTRING(\"TEST_BANK_INCOME\".\"COUNTRY\", 0, 4) = 'china' and cc1 = 'china'");
            });
            return null;
        }, project);
        String expected = "SELECT\n" //
                + "\"TEST_BANK_INCOME\".\"COUNTRY\" as \"TEST_BANK_INCOME_COUNTRY\"\n"
                + ", \"TEST_BANK_INCOME\".\"INCOME\" as \"TEST_BANK_INCOME_INCOME\"\n"
                + ", \"TEST_BANK_INCOME\".\"NAME\" as \"TEST_BANK_INCOME_NAME\"\n"
                + ", \"TEST_BANK_INCOME\".\"DT\" as \"TEST_BANK_INCOME_DT\"\n"
                + ", \"TEST_BANK_LOCATION\".\"COUNTRY\" as \"TEST_BANK_LOCATION_COUNTRY\"\n"
                + ", \"TEST_BANK_LOCATION\".\"OWNER\" as \"TEST_BANK_LOCATION_OWNER\"\n"
                + ", \"TEST_BANK_LOCATION\".\"LOCATION\" as \"TEST_BANK_LOCATION_LOCATION\"\n"
                + "FROM \"DEFAULT\".\"TEST_BANK_INCOME\" as \"TEST_BANK_INCOME\"\n"
                + "INNER JOIN \"DEFAULT\".\"TEST_BANK_LOCATION\" as \"TEST_BANK_LOCATION\"\n"
                + "ON \"TEST_BANK_INCOME\".\"COUNTRY\" = \"TEST_BANK_LOCATION\".\"COUNTRY\"\n" //
                + "WHERE\n" //
                + "1 = 1\n" //
                + " AND (SUBSTRING(\"TEST_BANK_INCOME\".\"COUNTRY\", 0, 4) = 'china' and cc1 = 'china')";
        NDataModel updatedModel = modelManager.getDataModelDesc(model.getUuid());
        Assert.assertEquals(expected, PushDownUtil.generateFlatTableSql(updatedModel, false));
    }

    @Test
    public void testGenerateFlatTableSqlWithSpecialFunctions() {
        String project = "default";
        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        NDataModel model = modelManager.getDataModelDescByAlias("test_bank");
        updateModelToAddCC(project, model);
        // change filter condition
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            NDataModelManager modelMgr = NDataModelManager.getInstance(kylinConfig, project);
            modelMgr.updateDataModel(model.getUuid(), copyForWrite -> {
                copyForWrite.setFilterCondition("timestampadd(day, 1, current_date) = '2012-01-01' and cc1 = 'china'");
            });
            return null;
        }, project);
        String expected = "SELECT\n" //
                + "\"TEST_BANK_INCOME\".\"COUNTRY\" as \"TEST_BANK_INCOME_COUNTRY\"\n"
                + ", \"TEST_BANK_INCOME\".\"INCOME\" as \"TEST_BANK_INCOME_INCOME\"\n"
                + ", \"TEST_BANK_INCOME\".\"NAME\" as \"TEST_BANK_INCOME_NAME\"\n"
                + ", \"TEST_BANK_INCOME\".\"DT\" as \"TEST_BANK_INCOME_DT\"\n"
                + ", \"TEST_BANK_LOCATION\".\"COUNTRY\" as \"TEST_BANK_LOCATION_COUNTRY\"\n"
                + ", \"TEST_BANK_LOCATION\".\"OWNER\" as \"TEST_BANK_LOCATION_OWNER\"\n"
                + ", \"TEST_BANK_LOCATION\".\"LOCATION\" as \"TEST_BANK_LOCATION_LOCATION\"\n"
                + "FROM \"DEFAULT\".\"TEST_BANK_INCOME\" as \"TEST_BANK_INCOME\"\n"
                + "INNER JOIN \"DEFAULT\".\"TEST_BANK_LOCATION\" as \"TEST_BANK_LOCATION\"\n"
                + "ON \"TEST_BANK_INCOME\".\"COUNTRY\" = \"TEST_BANK_LOCATION\".\"COUNTRY\"\n" //
                + "WHERE\n" //
                + "1 = 1\n" //
                + " AND (TIMESTAMPADD(day, 1, current_date) = '2012-01-01' and cc1 = 'china')";
        NDataModel updatedModel = modelManager.getDataModelDesc(model.getUuid());
        Assert.assertEquals(expected, PushDownUtil.generateFlatTableSql(updatedModel, false));
    }

    private void updateModelToAddCC(String project, NDataModel model) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            NDataModelManager modelMgr = NDataModelManager.getInstance(kylinConfig, project);
            modelMgr.updateDataModel(model.getUuid(), copyForWrite -> {
                ComputedColumnDesc cc = new ComputedColumnDesc();
                cc.setColumnName("CC1");
                cc.setDatatype("int");
                cc.setExpression("substring(\"TEST_BANK_INCOME\".\"COUNTRY\", 0, 4)");
                cc.setInnerExpression("SUBSTRING(`TEST_BANK_INCOME`.`COUNTRY`, 0, 4)");
                cc.setTableAlias("TEST_BANK_INCOME");
                cc.setTableIdentity(model.getRootFactTableName());
                copyForWrite.getComputedColumnDescs().add(cc);
                List<NDataModel.NamedColumn> columns = copyForWrite.getAllNamedColumns();
                int id = columns.size();
                NDataModel.NamedColumn namedColumn = new NDataModel.NamedColumn();
                namedColumn.setName("CC1");
                namedColumn.setId(id);
                namedColumn.setAliasDotColumn("TEST_BANK_INCOME.CC1");
                columns.add(namedColumn);
                copyForWrite.setAllNamedColumns(columns);
            });
            return null;
        }, project);
    }
}
