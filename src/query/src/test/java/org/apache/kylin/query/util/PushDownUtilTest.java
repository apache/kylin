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
import java.util.Set;

import org.apache.kylin.common.AbstractTestCase;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.QueryErrorCode;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.util.TestUtils;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.acl.AclTCR;
import org.apache.kylin.metadata.acl.AclTCRManager;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.model.util.ComputedColumnUtil;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.util.MetadataTestUtils;
import org.glassfish.jersey.internal.guava.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

@MetadataInfo
class PushDownUtilTest extends AbstractTestCase {

    @BeforeAll
    public static void setUpForClass() {
        ComputedColumnUtil.setEXTRACTOR(ComputedColumnRewriter::extractCcRexNode);
    }

    private static void setAdminAuthentication() {
        TestingAuthenticationToken auth = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(auth);
    }

    private static void setNormalAuthenticationAndAclTcr() {
        TestingAuthenticationToken auth = new TestingAuthenticationToken("KYLIN", "KYLIN", Constant.ROLE_MODELER);
        SecurityContextHolder.getContext().setAuthentication(auth);
        AclTCRManager manager = AclTCRManager.getInstance(TestUtils.getTestConfig(), "default");
        manager.updateAclTCR(new AclTCR(), "KYLIN", true);
    }

    private static QueryContext.AclInfo prepareRoleModelerAclInfo(String project) {
        Set<String> groups = Sets.newHashSet();
        groups.add(Constant.ROLE_MODELER);
        return AclPermissionUtil.createAclInfo(project, groups);
    }

    private static QueryContext.AclInfo prepareRoleAdminAclInfo(String project) {
        Set<String> groups = Sets.newHashSet();
        groups.add("admin");
        groups.add(Constant.ROLE_ADMIN);
        return AclPermissionUtil.createAclInfo(project, groups);
    }

    @Test
    void testTryForcePushDown() {
        try {
            QueryParams queryParams = new QueryParams();
            queryParams.setProject("default");
            queryParams.setSelect(true);
            queryParams.setForcedToPushDown(true);
            PushDownUtil.tryIterQuery(queryParams);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof KylinException);
            Assertions.assertEquals(ServerErrorCode.SPARK_FAILURE.toErrorCode(), ((KylinException) e).getErrorCode());
        }
    }

    @Test
    void testTryWithPushDownDisable() {
        try {
            String project = "default";
            MetadataTestUtils.updateProjectConfig(project, "kylin.query.pushdown-enabled", "false");
            QueryParams queryParams = new QueryParams();
            queryParams.setProject("default");
            queryParams.setSelect(true);
            queryParams.setForcedToPushDown(true);
            PushDownUtil.tryIterQuery(queryParams);
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertTrue(e instanceof KylinException);
            Assertions.assertEquals(QueryErrorCode.INVALID_PARAMETER_PUSH_DOWN.toErrorCode(),
                    ((KylinException) e).getErrorCode());
        }
    }

    @Test
    void testBacktickQuote() {
        String table = "db.table";
        Assertions.assertEquals("`db`.`table`", String.join(".", PushDownUtil.backtickQuote(table.split("\\."))));
    }

    @Test
    void testMassagePushDownSql() {
        KylinConfig config = KylinConfig.createKylinConfig(new Properties());
        try (KylinConfig.SetAndUnsetThreadLocalConfig ignored = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
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
            Assertions.assertEquals(expectedSql, massagedSql);
        }
    }

    @Test
    void testMassagePushDownSqlWithDoubleQuote() {
        KylinConfig config = KylinConfig.createKylinConfig(new Properties());
        String sql = "select '''',trans_id from test_kylin_fact where LSTG_FORMAT_NAME like '%''%' group by trans_id limit 2;";
        QueryParams queryParams = new QueryParams("default", sql, "default", false);
        queryParams.setKylinConfig(config);
        String massagedSql = PushDownUtil.massagePushDownSql(queryParams);
        String expectedSql = "select '\\'', `TRANS_ID` from `TEST_KYLIN_FACT` where `LSTG_FORMAT_NAME` like '%\\'%' group by `TRANS_ID` limit 2";
        Assertions.assertEquals(expectedSql, massagedSql);
    }

    @Test
    void testMassagePushDownSqlWithDialectConverter() {
        KylinConfig config = KylinConfig.createKylinConfig(new Properties());
        try (KylinConfig.SetAndUnsetThreadLocalConfig ignored = KylinConfig.setAndUnsetThreadLocalConfig(config)) {
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
            Assertions.assertEquals(expectedSql, massagedSql);
        }
    }

    @Test
    void testReplaceDoubleQuoteToSingle() {
        String sql = "select ab from table where aa = '' and bb = '''as''n'''";
        String resSql = "select ab from table where aa = '' and bb = '\\'as\\'n\\''";
        Assertions.assertEquals(resSql, PushDownUtil.replaceEscapedQuote(sql));
    }

    @Test
    void testGenerateFlatTableSql() {
        try (QueryContext context = QueryContext.current()) {
            String project = "default";
            setNormalAuthenticationAndAclTcr();
            context.setAclInfo(prepareRoleModelerAclInfo(project));
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
            Assertions.assertEquals(expected, PushDownUtil.generateFlatTableSql(model, false));
        }
    }

    @Test
    void testGenerateFlatTableSqlOfRoleModeler() {
        try (QueryContext context = QueryContext.current()) {
            String project = "default";
            setAdminAuthentication();
            context.setAclInfo(prepareRoleAdminAclInfo(project));
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
            Assertions.assertEquals(expected, PushDownUtil.generateFlatTableSql(model, false));
        }
    }

    @Test
    void testGenerateFlatTableSqlWithCCJoin() {
        try (QueryContext context = QueryContext.current()) {
            String project = "default";
            setNormalAuthenticationAndAclTcr();
            context.setAclInfo(prepareRoleModelerAclInfo(project));
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
            Assertions.assertEquals(expected, PushDownUtil.generateFlatTableSql(updatedModel, false));
        }
    }

    @Test
    void testGenerateFlatTableSqlWithFilterCondition() {
        try (QueryContext context = QueryContext.current()) {
            String project = "default";
            setNormalAuthenticationAndAclTcr();
            context.setAclInfo(prepareRoleModelerAclInfo(project));
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
            Assertions.assertEquals(expected, PushDownUtil.generateFlatTableSql(updatedModel, false));
        }
    }

    @Test
    void testGenerateFlatTableSqlWithSpecialFunctions() {
        try (QueryContext context = QueryContext.current()) {
            String project = "default";
            setNormalAuthenticationAndAclTcr();
            context.setAclInfo(prepareRoleModelerAclInfo(project));
            NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            NDataModel model = modelManager.getDataModelDescByAlias("test_bank");
            updateModelToAddCC(project, model);
            // change filter condition
            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
                NDataModelManager modelMgr = NDataModelManager.getInstance(kylinConfig, project);
                modelMgr.updateDataModel(model.getUuid(), copyForWrite -> {
                    copyForWrite
                            .setFilterCondition("timestampadd(day, 1, current_date) = '2012-01-01' and cc1 = 'china'");
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
            Assertions.assertEquals(expected, PushDownUtil.generateFlatTableSql(updatedModel, false));
        }
    }

    @Test
    public void testRemoveSqlHints() {
        {
            overwriteSystemProp("kylin.query.pushdown.sql-hints-erasing.enabled", "true");
            String sql = "select /*+ AABB, MODEL_PRIORITY(m1,m2), ACCEPT_CACHE_TIME(123123), DEF */ col1, col2 from tb";
            String ret = PushDownUtil.removeSqlHints(sql, KylinConfig.getInstanceFromEnv());
            Assertions.assertEquals("select col1, col2 from tb", ret);
        }
        {
            overwriteSystemProp("kylin.query.pushdown.sql-hints-erasing.enabled", "false");
            String sql = "select /*+ AABB, MODEL_PRIORITY(m1,m2), ACCEPT_CACHE_TIME(123123), DEF */ col1, col2 from tb";
            String ret = PushDownUtil.removeSqlHints(sql, KylinConfig.getInstanceFromEnv());
            Assertions.assertEquals(sql, ret);
        }
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
