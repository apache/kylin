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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.util.Litmus;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.query.IQueryTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DateNumberFilterTransformer implements IQueryTransformer {

    private static final Logger logger = LoggerFactory.getLogger(DateNumberFilterTransformer.class);

    private static final ThreadLocal<SimpleDateFormat> THREAD_LOCAL = ThreadLocal
            .withInitial(() -> new SimpleDateFormat("yyyy-MM-dd"));

    @Override
    public String transform(String originSql, String project, String defaultSchema) {
        try {
            SqlTimeFilterMatcher matcher = new SqlTimeFilterMatcher(originSql);
            SqlNode sqlNode = getSqlNode(originSql);
            sqlNode.accept(matcher);

            if (matcher.getTimeFilterPositions().isEmpty()) {
                return originSql;
            } else {
                logger.debug("'DateNumberFilterTransformer' will be used to transform SQL");
                return replaceTimeFilter(matcher.getTimeFilterPositions(), originSql);
            }
        } catch (Exception e) {
            logger.warn("Something unexpected in DateNumberFilterTransformer, return original query", e);
            return originSql;
        }
    }

    private SqlNode getSqlNode(String sql) {
        SqlNode sqlNode;
        try {
            sqlNode = CalciteParser.parse(sql);
        } catch (SqlParseException e) {
            throw new IllegalStateException(e);
        }
        return sqlNode;
    }

    private String replaceTimeFilter(List<Pair<String, Pair<Integer, Integer>>> positions, String originSql) {
        positions.sort(((o1, o2) -> o2.getSecond().getFirst() - o1.getSecond().getFirst()));
        String sql = originSql + " ";
        for (Pair<String, Pair<Integer, Integer>> pos : positions) {
            sql = sql.substring(0, pos.getSecond().getFirst()) + pos.getFirst()
                    + sql.substring(pos.getSecond().getSecond());
        }
        return sql.trim();
    }

    static class SqlTimeFilterMatcher extends AbstractSqlVisitor {
        private final List<Pair<String, Pair<Integer, Integer>>> timeFilterPositions = new ArrayList<>();
        private static final List<String> SUPPORT_FUN = Arrays.asList("=", "IN", "NOT IN", "BETWEEN", "NOT BETWEEN",
                "<", ">", "<=", ">=", "!=", "<>");
        private static final List<String> YEAR_FUN = Arrays.asList("YEAR", "{fn YEAR}");
        private static final List<String> MONTH_FUN = Arrays.asList("MONTH", "{fn MONTH}");
        private static final List<String> DAY_FUN = Arrays.asList("DAYOFMONTH", "{fn DAYOFMONTH}");

        public SqlTimeFilterMatcher(String originSql) {
            super(originSql);
        }

        @Override
        public void visitInSqlWhere(SqlNode call) {
            Queue<SqlNode> conditions = new LinkedList<>();
            conditions.add(call);
            while (!conditions.isEmpty()) {
                SqlNode cond = conditions.poll();
                if (cond instanceof SqlBasicCall) {
                    SqlBasicCall node = (SqlBasicCall) cond;
                    if (SUPPORT_FUN.contains(node.getOperator().toString())) {
                        fetchTimeFilter(node);
                    } else {
                        conditions.addAll(node.getOperandList());
                    }
                }
            }
        }

        public void fetchTimeFilter(SqlBasicCall call) {
            switch (call.getOperator().toString()) {
            case "=":
            case "!=":
            case ">":
            case ">=":
            case "<":
            case "<=":
            case "<>":
                fetchTimeFilterInNormalCondition(call);
                break;
            case "BETWEEN":
            case "NOT BETWEEN":
                fetchTimeFilterInBetweenCondition(call);
                break;
            case "IN":
            case "NOT IN":
                fetchTimeFilterOfInCondition(call);
                break;
            default:
                break;
            }

        }

        private void fetchTimeFilterOfInCondition(SqlBasicCall call) {
            SqlBasicCall expression;
            SqlNodeList timeList;
            if (call.operand(0) instanceof SqlBasicCall && call.operand(1) instanceof SqlNodeList) {
                expression = call.operand(0);
                timeList = call.operand(1);
            } else {
                return;
            }
            TimeExpression timeExpression = new TimeExpression(expression);

            String operator;
            String delimiter;
            if (call.getOperator().toString().equals("IN")) {
                operator = "=";
                delimiter = " OR ";
            } else if (call.getOperator().toString().equals("NOT IN")) {
                operator = "!=";
                delimiter = " AND ";
            } else {
                return;
            }
            List<String> targets = new ArrayList<>();
            for (SqlNode node : timeList) {
                String subTarget = null;
                if (node instanceof SqlNumericLiteral) {
                    subTarget = rewriteFilter(operator, (SqlNumericLiteral) node, timeExpression);
                }
                if (subTarget == null) {
                    return;
                } else {
                    targets.add(subTarget);
                }
            }
            if (!targets.isEmpty()) {
                timeFilterFound(call, String.format("(%s)", String.join(delimiter, targets)));
            }
        }

        private void fetchTimeFilterInNormalCondition(SqlBasicCall call) {
            SqlBasicCall expression;
            SqlNumericLiteral time;
            if (call.operand(0) instanceof SqlBasicCall && call.operand(1) instanceof SqlNumericLiteral) {
                expression = call.operand(0);
                time = call.operand(1);
            } else if (call.operand(1) instanceof SqlBasicCall && call.operand(0) instanceof SqlNumericLiteral) {
                expression = call.operand(1);
                time = call.operand(0);
            } else {
                return;
            }
            TimeExpression timeExpression = new TimeExpression(expression);

            String target = rewriteFilter(call.getOperator().toString(), time, timeExpression);
            if (target != null) {
                timeFilterFound(call, target);
            }
        }

        private void fetchTimeFilterInBetweenCondition(SqlBasicCall call) {
            if (!(call.operand(0) instanceof SqlBasicCall) || !(call.operand(1) instanceof SqlNumericLiteral)
                    || !(call.operand(2) instanceof SqlNumericLiteral)) {
                return;
            }
            SqlBasicCall expression = call.operand(0);
            SqlNumericLiteral leftTime = call.operand(1);
            SqlNumericLiteral rightTime = call.operand(2);
            if (leftTime.toString().length() != rightTime.toString().length()) {
                return;
            }
            TimeExpression timeExpression = new TimeExpression(expression);
            String target = rewriteFilter(call.getOperator().toString(), leftTime, rightTime, timeExpression);
            if (target != null) {
                timeFilterFound(call, target);
            }
        }

        String rewriteFilter(String op, SqlNumericLiteral leftTime, SqlNumericLiteral rightTime,
                TimeExpression timeExpression) {
            String target = null;
            if (timeExpression.isYear(leftTime)) {
                target = String.format("cast(%s as date) %s %s and %s", timeExpression.colNameString(), op,
                        yearToDate(leftTime.getValueAs(Integer.class), false),
                        yearToDate(rightTime.getValueAs(Integer.class), true));
            } else if (timeExpression.isMonthYear(leftTime)) {
                target = String.format("cast(%s as date) %s %s and %s", timeExpression.colNameString(), op,
                        monthToDate(leftTime.getValueAs(Integer.class), false),
                        monthToDate(rightTime.getValueAs(Integer.class), true));
            } else if (timeExpression.isDayMonthYear(leftTime)) {
                target = String.format("cast(%s as date) %s %s and %s", timeExpression.colNameString(), op,
                        dayMonthYearToDate(leftTime.toString()), dayMonthYearToDate(rightTime.toString()));
            }
            return target;
        }

        String rewriteFilter(String op, SqlNumericLiteral time, TimeExpression timeExpression) {
            String target = null;
            switch (op) {
            case "=":
                if (timeExpression.isYear(time)) {
                    target = String.format("cast(%s as date) BETWEEN %s", timeExpression.colNameString(),
                            yearToRange(time.getValueAs(Integer.class)));
                } else if (timeExpression.isMonthYear(time)) {
                    target = String.format("cast(%s as date) BETWEEN %s", timeExpression.colNameString(),
                            monthYearToRange(time.getValueAs(Integer.class)));
                } else if (timeExpression.isDayMonthYear(time)) {
                    target = String.format("cast(%s as date) = %s", timeExpression.colNameString(),
                            dayMonthYearToDate(time.toString()));
                }
                break;
            case "!=":
            case "<>":
                if (timeExpression.isYear(time)) {
                    target = String.format("cast(%s as date) NOT BETWEEN %s", timeExpression.colNameString(),
                            yearToRange(time.getValueAs(Integer.class)));
                } else if (timeExpression.isMonthYear(time)) {
                    target = String.format("cast(%s as date) NOT BETWEEN %s", timeExpression.colNameString(),
                            monthYearToRange(time.getValueAs(Integer.class)));
                } else if (timeExpression.isDayMonthYear(time)) {
                    target = String.format("cast(%s as date) <> %s", timeExpression.colNameString(),
                            dayMonthYearToDate(time.toString()));
                }
                break;
            case ">":
            case "<":
            case ">=":
            case "<=":
                if (timeExpression.isYear(time)) {
                    target = String.format("cast(%s as date) %s %s", timeExpression.colNameString(), op,
                            yearToDate(time.getValueAs(Integer.class), op.equals(">") || op.equals("<=")));
                } else if (timeExpression.isMonthYear(time)) {
                    target = String.format("cast(%s as date) %s %s", timeExpression.colNameString(), op,
                            monthToDate(time.getValueAs(Integer.class), op.equals(">") || op.equals("<=")));
                } else if (timeExpression.isDayMonthYear(time)) {
                    target = String.format("cast(%s as date) %s %s", timeExpression.colNameString(), op,
                            dayMonthYearToDate(time.toString()));
                }
                break;
            default:
                break;
            }
            return target;
        }

        String yearToRange(int year) {
            return yearToDate(year, false) + " and " + yearToDate(year, true);
        }

        String monthYearToRange(int monthYear) {
            return monthToDate(monthYear, false) + " and " + monthToDate(monthYear, true);
        }

        String yearToDate(int year, boolean end) {
            return end ? String.format("'%d-12-31'", year) : String.format("'%d-01-01'", year);

        }

        String monthToDate(int monthYear, boolean end) {
            int month = monthYear % 100;
            int year = (monthYear - month) / 100;

            Calendar cal = Calendar.getInstance();
            cal.clear();
            cal.set(Calendar.YEAR, year);
            cal.set(Calendar.MONTH, month - 1);
            int day = end ? cal.getActualMaximum(Calendar.DAY_OF_MONTH) : 1;
            cal.set(Calendar.DAY_OF_MONTH, day);
            SimpleDateFormat sdf = THREAD_LOCAL.get();
            return String.format("'%s'", sdf.format(cal.getTime()));
        }

        String dayMonthYearToDate(String dayMonthYear) {
            return String.format("'%s-%s-%s'", dayMonthYear.substring(0, 4), dayMonthYear.substring(4, 6),
                    dayMonthYear.substring(6));
        }

        public void timeFilterFound(SqlNode filter, String target) {
            Pair<Integer, Integer> pos = CalciteParser.getReplacePos(filter, originSql);
            timeFilterPositions.add(new Pair<>(target, pos));
        }

        public List<Pair<String, Pair<Integer, Integer>>> getTimeFilterPositions() {
            return timeFilterPositions;
        }

        static class TimeExpression {
            private int timeType = 0;
            // 0: not support; 1: only has YEAR; 2: has YEAR and MONTH; 3: has YEAR, MONTH, DAY
            private SqlNode colName = null;
            private List<SqlNode> addedExpression = new ArrayList<>();
            private SqlBasicCall yearExpression = null;
            private SqlBasicCall monthExpression = null;
            private SqlBasicCall dayExpression = null;

            public TimeExpression(SqlBasicCall expression) {
                extraSubExpression(expression);
                if (addedExpression.size() == 1) {
                    initYearTime();
                } else if (addedExpression.size() == 2) {
                    initMonthYearTime();
                } else if (addedExpression.size() == 3) {
                    initDayMonthYearTime();
                }
            }

            private void initYearTime() {
                if (!(addedExpression.get(0) instanceof SqlBasicCall)) {
                    return;
                }
                yearExpression = (SqlBasicCall) addedExpression.get(0);
                if (YEAR_FUN.contains(yearExpression.getOperator().toString())) {
                    timeType = 1;
                    colName = yearExpression.operand(0);
                }
            }

            private void initMonthYearTime() {
                if (!(addedExpression.get(0) instanceof SqlBasicCall)
                        || !(addedExpression.get(1) instanceof SqlBasicCall)) {
                    return;
                }
                for (SqlNode node : addedExpression) {
                    SqlBasicCall subExp = (SqlBasicCall) node;
                    if (MONTH_FUN.contains(subExp.getOperator().toString())) {
                        monthExpression = subExp;
                        if (colName == null) {
                            colName = subExp.operand(0);
                        } else if (!colName.equalsDeep(subExp.operand(0), Litmus.IGNORE)) {
                            return;
                        }
                    } else if (subExp.getOperator().toString().equals("*")) {
                        checkMultiplicationExpression(subExp);
                    }
                }
                if (monthExpression != null && yearExpression != null) {
                    timeType = 2;
                }
            }

            private void initDayMonthYearTime() {
                if (!(addedExpression.get(0) instanceof SqlBasicCall)
                        || !(addedExpression.get(1) instanceof SqlBasicCall)
                        || !(addedExpression.get(2) instanceof SqlBasicCall)) {
                    return;
                }
                for (SqlNode node : addedExpression) {
                    SqlBasicCall subExp = (SqlBasicCall) node;
                    if (DAY_FUN.contains(subExp.getOperator().toString())) {
                        dayExpression = subExp;
                        if (colName == null) {
                            colName = subExp.operand(0);
                        } else if (!colName.equalsDeep(subExp.operand(0), Litmus.IGNORE)) {
                            return;
                        }
                    } else if (subExp.getOperator().toString().equals("*")) {
                        checkMultiplicationExpression(subExp);
                    }
                }
                if (dayExpression != null && monthExpression != null && yearExpression != null) {
                    timeType = 3;
                }
            }

            void checkMultiplicationExpression(SqlBasicCall subExp) {
                SqlNode multiplier;
                SqlBasicCall tmpExpression;
                if (subExp.operand(0) instanceof SqlBasicCall) {
                    tmpExpression = subExp.operand(0);
                    multiplier = subExp.operand(1);
                } else if (subExp.operand(1) instanceof SqlBasicCall) {
                    tmpExpression = subExp.operand(1);
                    multiplier = subExp.operand(0);
                } else {
                    return;
                }
                if ((multiplier instanceof SqlNumericLiteral)) {
                    if (multiplier.toString().equals("100") && addedExpression.size() == 3
                            && MONTH_FUN.contains(tmpExpression.getOperator().toString())) {
                        if (colName == null || colName.equalsDeep(tmpExpression.operand(0), Litmus.IGNORE)) {
                            colName = tmpExpression.operand(0);
                            monthExpression = tmpExpression;
                        }
                    } else if (isYearMultiplicationExpression(multiplier, tmpExpression)
                            && (colName == null || colName.equalsDeep(tmpExpression.operand(0), Litmus.IGNORE))) {
                        colName = tmpExpression.operand(0);
                        yearExpression = tmpExpression;
                    }
                }
            }

            public String colNameString() {
                return colName.toSqlString(CalciteSqlDialect.DEFAULT).toString();
            }

            void extraSubExpression(SqlNode node) {
                if (node instanceof SqlBasicCall && ((SqlBasicCall) node).getOperator().toString().equals("+")) {
                    for (SqlNode subNode : ((SqlBasicCall) node).getOperandList()) {
                        extraSubExpression(subNode);
                    }
                } else {
                    addedExpression.add(node);
                }
            }

            public boolean isYearMultiplicationExpression(SqlNode multiplier, SqlBasicCall expression) {
                return (multiplier.toString().equals("10000") && addedExpression.size() == 3
                        || multiplier.toString().equals("100") && addedExpression.size() == 2)
                        && YEAR_FUN.contains(expression.getOperator().toString());
            }

            public boolean isYear(SqlNumericLiteral time) {
                return this.timeType == 1 && time.toString().length() == 4;
            }

            public boolean isMonthYear(SqlNumericLiteral time) {
                return this.timeType == 2 && time.toString().length() == 6;
            }

            public boolean isDayMonthYear(SqlNumericLiteral time) {
                return this.timeType == 3 && time.toString().length() == 8;
            }
        }
    }
}
