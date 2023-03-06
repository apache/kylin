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

import static org.mockito.Mockito.mock;

import java.util.List;

import org.apache.calcite.sql.parser.ddl.ParseException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.query.engine.KECalciteConfig;
import org.junit.Assert;
import org.junit.Test;

public class DDLParserTest {
    @Test
    public void test_multi_join() throws Exception {

        KylinConfig kylinConfig = mock(KylinConfig.class);
        KECalciteConfig config = KECalciteConfig.fromKapConfig(kylinConfig);

        String sql1 = "CREATE MATERIALIZED VIEW project.test_model.table1.c1_1 AS\n" + "SELECT table1.c1_1,\n"
                + "       table2.c2_2,\n" + "       MAX(table1.c3),\n" + "       hll_count(table1.c4),\n"
                + "       bitmap_count(table1.c5)\n" + "FROM \n" + "  db.table1 JOIN db.table2\n"
                + "  ON table1.c1_1 = table2.c2_1 and table1.c1_3 = table2.c2_3\n" + "  JOIN db.table3\n"
                + " ON table1.c1_2 = table2.c2_2\n" + "GROUP BY CUBE(table1.c1_1, table2.c2_2)";

        DDLParser ddlParser = DDLParser.CreateParser(config);
        DDLParser.DDLParserResult result = ddlParser.parseSQL(sql1);

        Assert.assertEquals(result.getProjectName(), "project");
        Assert.assertEquals(result.getModelName(), "test_model");
        Assert.assertEquals(result.getPartitionColName(), "table1.c1_1".toUpperCase());

        List<NDataModel.NamedColumn> simplifiedDimensions = result.getSimplifiedDimensions();
        Assert.assertEquals(simplifiedDimensions.size(), 2);
        //Not use id info
        Assert.assertEquals(simplifiedDimensions.toString(),
                "[NDataModel.NamedColumn(id=0, name=TABLE1_C1_1, aliasDotColumn=TABLE1.C1_1, status=EXIST), NDataModel.NamedColumn(id=0, name=TABLE2_C2_2, aliasDotColumn=TABLE2.C2_2, status=EXIST)]");

        List<DDLParser.InnerMeasure> simplifiedMeasures = result.getSimplifiedMeasures();
        Assert.assertEquals(simplifiedMeasures.size(), 3);
        Assert.assertEquals(simplifiedMeasures.get(0).toString(),
                "DDLParser.InnerMeasure(expression=MAX, returnType=UNDEFINED, parameterValues=[{column,TABLE1.C3}])");
        Assert.assertEquals(simplifiedMeasures.get(1).toString(),
                "DDLParser.InnerMeasure(expression=COUNT_DISTINCT, returnType=hllc(14), parameterValues=[{column,TABLE1.C4}])");
        Assert.assertEquals(simplifiedMeasures.get(2).toString(),
                "DDLParser.InnerMeasure(expression=COUNT_DISTINCT, returnType=bitmap, parameterValues=[{column,TABLE1.C5}])");

        Assert.assertEquals(result.getFactTable(), "db.table1".toUpperCase());

        List<JoinTableDesc> joinTables = result.getJoinTables();
        Assert.assertEquals(joinTables.size(), 2);
        Assert.assertEquals(joinTables.get(0).toString(),
                "JoinTableDesc(table=DB.TABLE3, kind=LOOKUP, alias=TABLE3, join=JoinDesc [type=INNER, primary_key=[TABLE2.C2_2], foreign_key=[TABLE1.C1_2]], flattenable=null, joinRelationTypeEnum=MANY_TO_ONE, tableRef=null)");
        Assert.assertEquals(joinTables.get(1).toString(),
                "JoinTableDesc(table=DB.TABLE2, kind=LOOKUP, alias=TABLE2, join=JoinDesc [type=INNER, primary_key=[TABLE2.C2_1, TABLE2.C2_3], foreign_key=[TABLE1.C1_1, TABLE1.C1_3]], flattenable=null, joinRelationTypeEnum=MANY_TO_ONE, tableRef=null)");

    }

    @Test
    public void test_without_join() throws Exception {
        KylinConfig kylinConfig = mock(KylinConfig.class);
        KECalciteConfig config = KECalciteConfig.fromKapConfig(kylinConfig);

        String sql1 = "CREATE MATERIALIZED VIEW project.test_model AS\n" + "SELECT table1.c1,\n" + "       table1.c2,\n"
                + "       percentile(table1.c3)\n" + "FROM \n" + "  db.table1 \n"
                + "GROUP BY CUBE(table1.c1, table1.c2)";

        DDLParser ddlParser = DDLParser.CreateParser(config);
        DDLParser.DDLParserResult result = ddlParser.parseSQL(sql1);

        Assert.assertEquals(result.getProjectName(), "project");
        Assert.assertEquals(result.getModelName(), "test_model");
        Assert.assertNull(result.getPartitionColName());

        List<NDataModel.NamedColumn> simplifiedDimensions = result.getSimplifiedDimensions();
        Assert.assertEquals(simplifiedDimensions.size(), 2);
        Assert.assertEquals(simplifiedDimensions.toString(),
                "[NDataModel.NamedColumn(id=0, name=TABLE1_C1, aliasDotColumn=TABLE1.C1, status=EXIST), NDataModel.NamedColumn(id=0, name=TABLE1_C2, aliasDotColumn=TABLE1.C2, status=EXIST)]");

        List<DDLParser.InnerMeasure> simplifiedMeasures = result.getSimplifiedMeasures();
        Assert.assertEquals(simplifiedMeasures.size(), 1);
        Assert.assertEquals(simplifiedMeasures.get(0).toString(),
                "DDLParser.InnerMeasure(expression=PERCENTILE_APPROX, returnType=percentile(100), parameterValues=[{column,TABLE1.C3}])");
        Assert.assertEquals(result.getFactTable(), "db.table1".toUpperCase());

        List<JoinTableDesc> joinTables = result.getJoinTables();
        Assert.assertEquals(joinTables.size(), 0);

    }

    @Test
    public void test_one_join() throws Exception {

        KylinConfig kylinConfig = mock(KylinConfig.class);
        KECalciteConfig config = KECalciteConfig.fromKapConfig(kylinConfig);

        String sql1 = "CREATE MATERIALIZED VIEW project.test_model.table1.c1_1 AS\n" + "SELECT table1.c1_1,\n"
                + "       table2.c2_2,\n" + "       MAX(table1.c3)\n" + "FROM \n" + "  db.table1 JOIN db.table2\n"
                + "  ON table1.c1_1 = table2.c2_1 and table1.c1_3 = table2.c2_3\n"
                + "GROUP BY CUBE(table1.c1_1, table2.c2_2)";

        DDLParser ddlParser = DDLParser.CreateParser(config);
        DDLParser.DDLParserResult result = ddlParser.parseSQL(sql1);

        Assert.assertEquals(result.getProjectName(), "project");
        Assert.assertEquals(result.getModelName(), "test_model");
        Assert.assertEquals(result.getPartitionColName(), "table1.c1_1".toUpperCase());

        List<NDataModel.NamedColumn> simplifiedDimensions = result.getSimplifiedDimensions();
        Assert.assertEquals(simplifiedDimensions.size(), 2);
        //Not use id info
        Assert.assertEquals(simplifiedDimensions.toString(),
                "[NDataModel.NamedColumn(id=0, name=TABLE1_C1_1, aliasDotColumn=TABLE1.C1_1, status=EXIST), NDataModel.NamedColumn(id=0, name=TABLE2_C2_2, aliasDotColumn=TABLE2.C2_2, status=EXIST)]");

        List<DDLParser.InnerMeasure> simplifiedMeasures = result.getSimplifiedMeasures();
        Assert.assertEquals(simplifiedMeasures.size(), 1);
        Assert.assertEquals(simplifiedMeasures.get(0).toString(),
                "DDLParser.InnerMeasure(expression=MAX, returnType=UNDEFINED, parameterValues=[{column,TABLE1.C3}])");

        Assert.assertEquals(result.getFactTable(), "db.table1".toUpperCase());

        List<JoinTableDesc> joinTables = result.getJoinTables();
        Assert.assertEquals(joinTables.size(), 1);
        Assert.assertEquals(joinTables.get(0).toString(),
                "JoinTableDesc(table=DB.TABLE2, kind=LOOKUP, alias=TABLE2, join=JoinDesc [type=INNER, primary_key=[TABLE2.C2_1, TABLE2.C2_3], foreign_key=[TABLE1.C1_1, TABLE1.C1_3]], flattenable=null, joinRelationTypeEnum=MANY_TO_ONE, tableRef=null)");
    }

    @Test
    public void model_project_name_case() throws Exception {

        KylinConfig kylinConfig = mock(KylinConfig.class);
        KECalciteConfig config = KECalciteConfig.fromKapConfig(kylinConfig);

        String sql1 = "CREATE MATERIALIZED VIEW proJect_Name.tesT_model.table1.c1_1 AS\n" + "SELECT table1.c1_1,\n"
                + "       table2.c2_2,\n" + "       MAX(table1.c3)\n" + "FROM \n" + "  db.table1 JOIN db.table2\n"
                + "  ON table1.c1_1 = table2.c2_1 and table1.c1_3 = table2.c2_3\n"
                + "GROUP BY CUBE(table1.c1_1, table2.c2_2)";

        DDLParser ddlParser = DDLParser.CreateParser(config);
        DDLParser.DDLParserResult result = ddlParser.parseSQL(sql1);

        Assert.assertEquals(result.getProjectName(), "proJect_Name");
        Assert.assertEquals(result.getModelName(), "tesT_model");
        Assert.assertEquals(result.getPartitionColName(), "table1.c1_1".toUpperCase());
    }

    @Test
    public void test_forbidden_join_condition() throws Exception {
        KylinConfig kylinConfig = mock(KylinConfig.class);
        KECalciteConfig config = KECalciteConfig.fromKapConfig(kylinConfig);

        // 1. no equal join
        String sql1 = "CREATE MATERIALIZED VIEW proJect_Name.tesT_model.table1.c1_1 AS\n" + "SELECT table1.c1_1,\n"
                + "       table2.c2_2,\n" + "       MAX(table1.c3)\n" + "FROM \n" + "  db.table1 JOIN db.table2\n"
                + "  ON table1.c1_1 = table2.c2_1 and table1.c1_3 > table2.c2_3\n"
                + "GROUP BY CUBE(table1.c1_1, table2.c2_2)";

        DDLParser ddlParser = DDLParser.CreateParser(config);
        Assert.assertThrows(ParseException.class, () -> ddlParser.parseSQL(sql1));

        // 2. no equal join
        String sql2 = "CREATE MATERIALIZED VIEW proJect_Name.tesT_model.table1.c1_1 AS\n" + "SELECT table1.c1_1,\n"
                + "       table2.c2_2,\n" + "       MAX(table1.c3)\n" + "FROM \n" + "  db.table1 JOIN db.table2\n"
                + "  ON table1.c1_1 > table2.c2_1 \n" + "GROUP BY CUBE(table1.c1_1, table2.c2_2)";

        Assert.assertThrows(ParseException.class, () -> ddlParser.parseSQL(sql2));

        // 3. no join condition
        String sql3 = "CREATE MATERIALIZED VIEW proJect_Name.tesT_model.table1.c1_1 AS\n" + "SELECT table1.c1_1,\n"
                + "       table2.c2_2,\n" + "       MAX(table1.c3)\n" + "FROM \n" + "  db.table1 JOIN db.table2\n"
                + "GROUP BY CUBE(table1.c1_1, table2.c2_2)";
        Assert.assertThrows(ParseException.class, () -> ddlParser.parseSQL(sql3));

        // 3. right join
        String sql4 = "CREATE MATERIALIZED VIEW proJect_Name.tesT_model.table1.c1_1 AS\n" + "SELECT table1.c1_1,\n"
                + "       table2.c2_2,\n" + "       MAX(table1.c3)\n" + "FROM \n" + "  db.table1 RIGHT JOIN db.table2\n"
                + "  ON table1.c1_1 = table2.c2_1 and table1.c1_3 = table2.c2_3\n"
                + "GROUP BY CUBE(table1.c1_1, table2.c2_2)";
        Assert.assertThrows(ParseException.class, () -> ddlParser.parseSQL(sql4));
    }

    @Test
    public void test_measure_diff_accuracy_type() throws Exception {
        KylinConfig kylinConfig = mock(KylinConfig.class);
        KECalciteConfig config = KECalciteConfig.fromKapConfig(kylinConfig);

        String sql1 = "CREATE MATERIALIZED VIEW project.test_model AS\n" + "SELECT table1.c1,\n" + "       table1.c2,\n"
                + "       percentile_100(table1.c3),\n" + "       percentile_1000(table1.c3),\n"
                + "       percentile_10000(table1.c3),\n" + "       hll_count_10(table1.c4),\n"
                + "       hll_count_12(table1.c4),\n" + "       hll_count_14(table1.c4),\n"
                + "       hll_count_15(table1.c4),\n" + "       hll_count_16(table1.c4)\n" + "FROM \n"
                + "  db.table1 \n" + "GROUP BY CUBE(table1.c1, table1.c2)";

        DDLParser ddlParser = DDLParser.CreateParser(config);
        DDLParser.DDLParserResult result = ddlParser.parseSQL(sql1);

        List<NDataModel.NamedColumn> simplifiedDimensions = result.getSimplifiedDimensions();
        Assert.assertEquals(simplifiedDimensions.size(), 2);
        Assert.assertEquals(simplifiedDimensions.toString(),
                "[NDataModel.NamedColumn(id=0, name=TABLE1_C1, aliasDotColumn=TABLE1.C1, status=EXIST), NDataModel.NamedColumn(id=0, name=TABLE1_C2, aliasDotColumn=TABLE1.C2, status=EXIST)]");

        List<DDLParser.InnerMeasure> simplifiedMeasures = result.getSimplifiedMeasures();
        Assert.assertEquals(simplifiedMeasures.size(), 8);
        Assert.assertEquals(simplifiedMeasures.get(0).toString(),
                "DDLParser.InnerMeasure(expression=PERCENTILE_APPROX, returnType=percentile(100), parameterValues=[{column,TABLE1.C3}])");
        Assert.assertEquals(result.getFactTable(), "db.table1".toUpperCase());
        Assert.assertEquals(simplifiedMeasures.get(1).toString(),
                "DDLParser.InnerMeasure(expression=PERCENTILE_APPROX, returnType=percentile(1000), parameterValues=[{column,TABLE1.C3}])");
        Assert.assertEquals(result.getFactTable(), "db.table1".toUpperCase());
        Assert.assertEquals(simplifiedMeasures.get(2).toString(),
                "DDLParser.InnerMeasure(expression=PERCENTILE_APPROX, returnType=percentile(10000), parameterValues=[{column,TABLE1.C3}])");
        Assert.assertEquals(result.getFactTable(), "db.table1".toUpperCase());

        Assert.assertEquals(simplifiedMeasures.get(3).toString(),
                "DDLParser.InnerMeasure(expression=COUNT_DISTINCT, returnType=hllc(10), parameterValues=[{column,TABLE1.C4}])");
        Assert.assertEquals(simplifiedMeasures.get(4).toString(),
                "DDLParser.InnerMeasure(expression=COUNT_DISTINCT, returnType=hllc(12), parameterValues=[{column,TABLE1.C4}])");
        Assert.assertEquals(simplifiedMeasures.get(5).toString(),
                "DDLParser.InnerMeasure(expression=COUNT_DISTINCT, returnType=hllc(14), parameterValues=[{column,TABLE1.C4}])");
        Assert.assertEquals(simplifiedMeasures.get(6).toString(),
                "DDLParser.InnerMeasure(expression=COUNT_DISTINCT, returnType=hllc(15), parameterValues=[{column,TABLE1.C4}])");
        Assert.assertEquals(simplifiedMeasures.get(7).toString(),
                "DDLParser.InnerMeasure(expression=COUNT_DISTINCT, returnType=hllc(16), parameterValues=[{column,TABLE1.C4}])");

    }

}
