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

package org.apache.kylin.auto;

import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;

import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.rec.util.AccelerationUtil;
import org.apache.kylin.util.ExecAndCompExt;
import org.apache.kylin.util.SuggestTestBase;
import org.apache.spark.sql.SparderEnv;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Ignore("For development")
@Slf4j
public class AutoSqlFunctionsValidationTest extends SuggestTestBase {

    private static final String PARAM_TAG = "<p>";
    private static final String EMPTY = "";
    private static final String SPACE = " ";
    private static final String STR_CONNECTOR = "+";
    private static final int MAX_BOUND = 20;

    private final Random random = new Random();
    private final Set<Function> functions = Sets.newHashSet();
    private final String[] intTypeCols = new String[] { "ID1", "ID2", "ID3", "ID4", "price5", "price6", "price7" };
    private final String[] numericTypeCols = new String[] { "ID1", "ID2", "ID3", "ID4", "price1", "price2", "price3",
            "price5", "price6", "price7", "name4" };
    private final String[] stringTypeCols = new String[] { "name1", "name2", "name3" };
    private final String[] datetimeTypeCols = new String[] { "time1", "time2" };
    private final String[] boolTypeCols = new String[] { "flag" };
    private final String[] calcAggs = new String[] { "MAX", "MIN", "COUNT", "AVG", "COUNT(DISTINCT)", "SUM" };
    private final String[] describeAggs = new String[] { "MAX", "MIN", "COUNT", "COUNT(DISTINCT)" };
    private final String[] operators = new String[] { "+", "-", "*" };

    @Before
    public void setUp() throws Exception {
        super.setUp();
        initFunctions();
    }

    private void initFunctions() {
        addArithmeticFunctions();
        addStringFunctions();
        addTimeRelatedFunctions();
        addConvertFunctions();
        addConditionFunctions();
    }

    @Test
    public void testProposeComputedColumnWithFunctionsInManual() throws InterruptedException {
        Set<String> measures = generateAllMeasures();
        String sql = "select ID1, ".concat(String.join(", ", measures)).concat(" from test_measure group by ID1");
        log.info("random generated sql is:{}", sql);
        AccelerationUtil.runWithSmartContext(getTestConfig(), getProject(), new String[] { sql }, true);
        buildAllModels(getTestConfig(), getProject());
        populateSSWithCSVData(getTestConfig(), getProject(), SparderEnv.getSparkSession());
        List<Pair<String, String>> queries = Lists.newArrayList();
        queries.add(new Pair<>("random generated sql", sql));
        ExecAndCompExt.execAndCompare(queries, getProject(), ExecAndCompExt.CompareLevel.SAME, "default");
    }

    private Set<String> generateAllMeasures() {
        Set<String> measures = Sets.newHashSet();
        functions.forEach(function -> {
            List<String> params = Lists.newArrayList();
            for (ParamType dataType : function.getParaTypes()) {
                params.add(createParam(dataType));
            }
            measures.add(fillFunctionParams(function.getFuncForm(), params, function.sumCapable));
        });

        return measures;
    }

    private void addArithmeticFunctions() {
        functions.add(new Function("POWER(<p>, <p>)", new ParamType[] { ParamType.DOUBLE, ParamType.DOUBLE }, true));
        functions.add(new Function("ABS(<p>)", new ParamType[] { ParamType.DOUBLE }, true));
        functions.add(new Function("MOD(<p>, <p>)", new ParamType[] { ParamType.INT, ParamType.INT }, true));
        functions.add(new Function("LN(<p>)", new ParamType[] { ParamType.POSITIVE_DOUBLE }, true));
        // functions.add(new Function("LOG10(<p>)", new ParamType[] { ParamType.POSITIVE_DOUBLE }, true));
        // functions.add(new Function("EXP(<p>)", new ParamType[] { ParamType.DOUBLE }, true));
        functions.add(new Function("CEIL(<p>)", new ParamType[] { ParamType.DOUBLE }, true));
        functions.add(new Function("FLOOR(<p>)", new ParamType[] { ParamType.DOUBLE }, true));

        //==== unsupported yet =====
        // functions.add(new Function("SQRT(<p>)", new ParamType[] { ParamType.DOUBLE }, true));
        // functions.add(new Function("RAND(<p>)", new ParamType[] { ParamType.FRACTION }, true));
        // functions.add(new Function("RAND_INTEGER(<p>)", new ParamType[] { ParamType.DOUBLE }, true));
        // functions.add(new Function("ACOS(<p>)", new ParamType[] { ParamType.DOUBLE }, true));
        // functions.add(new Function("ASIN(<p>)", new ParamType[] { ParamType.DOUBLE }, true));
        // functions.add(new Function("ATAN(<p>)", new ParamType[] { ParamType.DOUBLE }, true));
        // functions.add(new Function("ATAN2(<p>, <p>)", new ParamType[] { ParamType.FRACTION, ParamType.DOUBLE }, true));
        // functions.add(new Function("COS(<p>)", new ParamType[] { ParamType.DOUBLE }, true));
        // functions.add(new Function("COT(<p>)", new ParamType[] { ParamType.DOUBLE }, true));
        // functions.add(new Function("DEGREES(<p>)", new ParamType[] { ParamType.DOUBLE }, true));
        // functions.add(new Function("RADIANS(<p>)", new ParamType[] { ParamType.DOUBLE }, true));
        // functions.add(new Function("ROUND(<p>, <p>)", new ParamType[] { ParamType.DOUBLE, ParamType.INT }, true));
        // functions.add(new Function("SIGN(<p>)", new ParamType[] { ParamType.DOUBLE }, true));
        // functions.add(new Function("SIN(<p>)", new ParamType[] { ParamType.DOUBLE }, true));
        // functions.add(new Function("TAN(<p>)", new ParamType[] { ParamType.DOUBLE }, true));
        // functions.add(new Function("TRUNCATE(<p>, <p>)", new ParamType[] { ParamType.DOUBLE, ParamType.INT }, true));
    }

    private void addStringFunctions() {
        functions.add(new Function("UPPER(<p>)", new ParamType[] { ParamType.STR }, false));
        functions.add(new Function("LOWER(<p>)", new ParamType[] { ParamType.STR }, false));
        functions.add(new Function("INITCAP(<p>)", new ParamType[] { ParamType.STR }, false));
        // functions.add(new Function("SUBSTRING(<p> from <p> for <p>)", new ParamType[] { ParamType.STR, ParamType.INT, ParamType.INT }, false));
        // functions.add(new Function("SUBSTRING(<p>, <p>, <p>)", new ParamType[] { ParamType.STR, ParamType.INT, ParamType.INT }, false));
        functions.add(new Function("POSITION(name1 in name2)", new ParamType[] { ParamType.STR, ParamType.STR }, true));

        //==== unsupported yet =====
        //  functions.add(new Function("CHAR_LENGTH(<p>)", new ParamType[] { ParamType.STR }, false));
        //  functions.add(new Function("CHARACTER_LENGTH(<p>)", new ParamType[] { ParamType.STR }, false));
        //  functions.add(new Function("OVERLAY(<p> placing <p> from <p> for <p>)", new ParamType[] { ParamType.STR, ParamType.STR, ParamType.CONST, ParamType.CONST }, false));
        //  functions.add(new Function("TRIM(both <p> from <p>)", new ParamType[] { ParamType.CONST_STR, ParamType.STR }, false));
        //  functions.add(new Function("REPLACE(<p>, <p>, <p>)", new ParamType[] { ParamType.STR, ParamType.CONST_STR, ParamType.CONST_STR }, false));
    }

    private void addTimeRelatedFunctions() {
        functions.add(new Function("TIMESTAMPADD(second, <p>, <p>)",
                new ParamType[] { ParamType.CONST, ParamType.DATETIME }, false));
        functions.add(new Function("TIMESTAMPDIFF(minute, <p>, <p>)",
                new ParamType[] { ParamType.DATETIME, ParamType.DATETIME }, false));

        //==== unsupported yet =====
        // functions.add(new Function("EXTRACT(second from <p>)", new ParamType[] { ParamType.DATETIME }, true));
        // functions.add(new Function("FLOOR(<p> to hour)", new ParamType[] { ParamType.DATETIME }, false));
        // functions.add(new Function("CEIL(<p> to minute)", new ParamType[] { ParamType.DATETIME }, false));
        // functions.add(new Function("YEAR(<p>)", new ParamType[] { ParamType.DATETIME }, true));
        // functions.add(new Function("QUARTER(<p>)", new ParamType[] { ParamType.DATETIME }, true));
        // functions.add(new Function("MONTH(<p>)", new ParamType[] { ParamType.DATETIME }, true));
        // functions.add(new Function("WEEK(<p>)", new ParamType[] { ParamType.DATETIME }, true));
        // functions.add(new Function("DAYOFYEAR(<p>)", new ParamType[] { ParamType.DATETIME }, true));
        // functions.add(new Function("DAYOFMONTH(<p>)", new ParamType[] { ParamType.DATETIME }, true));
        // functions.add(new Function("DAYOFWEEK(<p>)", new ParamType[] { ParamType.DATETIME }, true));
        // functions.add(new Function("HOUR(<p>)", new ParamType[] { ParamType.DATETIME }, true));
        // functions.add(new Function("MINUTE(<p>)", new ParamType[] { ParamType.DATETIME }, true));
        // functions.add(new Function("SECOND(<p>)", new ParamType[] { ParamType.DATETIME }, true));
    }

    private void addConditionFunctions() {
        // maybe need more complex cases for caseWhen
        functions.add(new Function("case when <p> > <p> then <p> else <p> end",
                new ParamType[] { ParamType.SOLO_DOUBLE, ParamType.CONST, ParamType.CONST_STR, ParamType.CONST_STR },
                false));
        functions.add(new Function("case <p> when <p> then <p> when <p> then <p> else <p> end",
                new ParamType[] { ParamType.SOLO_DOUBLE, ParamType.CONST, ParamType.CONST_STR, ParamType.CONST,
                        ParamType.CONST_STR, ParamType.CONST_STR },
                false));

        //==== unsupported yet =====
        // functions.add(new Function("NULLIF(<p>, <p>)", new ParamType[] { ParamType.DOUBLE, ParamType.DOUBLE }, true));
        // functions.add(new Function("COALESCE(<p>, <p>, <p>)", new ParamType[] { ParamType.DOUBLE, ParamType.DOUBLE, ParamType.CONST }, true));
    }

    private void addConvertFunctions() {
        functions.add(new Function("CAST(<p> as bigint)", new ParamType[] { ParamType.INT }, true));
        functions.add(new Function("DATE '2018-10-10'", new ParamType[0], false));
        functions.add(new Function("TIMESTAMP '2018-10-10 15:57:07'", new ParamType[0], false));
    }

    private String createParam(ParamType type) {
        String exp;
        switch (type) {
        case DOUBLE:
            exp = createExpressionParam(numericTypeCols, false);
            break;
        case POSITIVE_DOUBLE:
            exp = "ABS(".concat(createExpressionParam(numericTypeCols, false)).concat(") + 1");
            break;
        case SOLO_DOUBLE:
            exp = createSingleParam(numericTypeCols, false);
            break;
        case FRACTION:
            String intVar = createSingleParam(intTypeCols, false);
            exp = String.format(Locale.ROOT, "ABS(%s) / (ABS(%s) + %d + 1)", intVar, intVar, random.nextInt(MAX_BOUND));
            break;
        case INT:
            exp = createExpressionParam(intTypeCols, false);
            break;
        case STR:
            exp = createExpressionParam(stringTypeCols, true);
            break;
        case CONST:
            exp = createSingleParam(new String[0], false);
            break;
        case CONST_STR:
            exp = createSingleParam(new String[0], true);
            break;
        case DATETIME:
            exp = createSingleParam(datetimeTypeCols, false);
            break;
        case BOOLEAN:
            exp = createSingleParam(boolTypeCols, false);
            break;
        default:
            throw new IllegalArgumentException("unsupported param type yet!");
        }
        return exp;
    }

    // use candidate columns to create a expression used for param of measure.
    // for example: ID1 + ID2 * ID3 + 4, name1 + name2 + name3 + '1', name1 + '1', etc.
    private String createExpressionParam(String[] candidateColumns, boolean needStringConnector) {
        int size = 1 + random.nextInt(20) % 3;
        List<String> eleList = Lists.newArrayList();
        for (int i = 0; i < size; i++) {
            int idx = random.nextInt(candidateColumns.length);
            eleList.add(candidateColumns[idx]);
            if (needStringConnector) {
                eleList.add(AutoSqlFunctionsValidationTest.STR_CONNECTOR);
            } else {
                eleList.add(operators[random.nextInt(operators.length)]);
            }
        }
        if (needStringConnector) {
            eleList.remove(eleList.size() - 1);
        } else {
            eleList.add(random.nextInt(20) + EMPTY);
        }
        return String.join(AutoSqlFunctionsValidationTest.SPACE, eleList);
    }

    // use candidate columns create a simple expression used for param of measure,
    // for example: 1, '1', ID1, etc.
    private String createSingleParam(String[] candidateColumns, boolean needQuota) {
        if (candidateColumns.length == 0) {
            return needQuota ? "'" + (1 + random.nextInt(MAX_BOUND)) + "'" : (1 + random.nextInt(MAX_BOUND)) + EMPTY;
        }
        return candidateColumns[random.nextInt(candidateColumns.length)];
    }

    // use params to replace `<p>` to generate a real measure
    private String fillFunctionParams(String oriFuncExp, List<String> params, boolean isSumCapable) {
        String measure;
        String exp = oriFuncExp;
        for (String param : params) {
            exp = exp.replaceFirst(PARAM_TAG, param);
        }
        Preconditions.checkState(!exp.contains(PARAM_TAG), "error defined function: " + oriFuncExp);

        String aggName = isSumCapable //
                ? calcAggs[random.nextInt(calcAggs.length)]
                : describeAggs[random.nextInt(describeAggs.length)];
        if (aggName.contains("(")) {
            measure = aggName.substring(0, aggName.length() - 1).concat(SPACE).concat(exp).concat(")");
        } else {
            measure = aggName.concat("(").concat(exp).concat(")");
        }

        return measure;
    }

    @Data
    private class Function {
        private String funcForm;
        private ParamType[] paraTypes;
        private boolean sumCapable;

        Function(String funcForm, ParamType[] paraTypes, boolean sumCapable) {
            this.funcForm = funcForm;
            this.paraTypes = paraTypes;
            this.sumCapable = sumCapable;
        }
    }

    private enum ParamType {
        /**
         * String exp
         */
        STR,

        /**
         * int exp
         */
        INT,

        /**
         * double exp
         */
        DOUBLE,

        /**
         * positive double exp
         */
        POSITIVE_DOUBLE,

        /**
         * solo double exp, for example: price1, price2, id1
         */
        SOLO_DOUBLE,

        /**
         * fraction double exp, for example: id1/(id1+1), (price3+1)/(price3+2)
         */
        FRACTION,

        DATETIME,

        BOOLEAN,

        /**
         * constant, for example: 1, 2, 3, etc.
         */
        CONST,

        /**
         * constant string, for example: '1', '2', '3', etc.
         */
        CONST_STR
    }
}
