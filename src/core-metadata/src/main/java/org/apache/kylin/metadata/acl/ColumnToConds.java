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

package org.apache.kylin.metadata.acl;

import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.text.StrBuilder;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.CommonErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.CaseInsensitiveStringMap;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.kylin.metadata.acl.ColumnToConds.Cond.IntervalType;
import org.apache.kylin.metadata.model.NTableMetadataManager;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
//all row conditions in the table, for example:C1:{cond1, cond2},C2{cond1, cond3}, immutable
public class ColumnToConds extends CaseInsensitiveStringMap<List<ColumnToConds.Cond>> implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(ColumnToConds.class);
    private static final String COMMA = ",";

    public ColumnToConds() {
    }

    public ColumnToConds(Map<String, List<Cond>> columnToConds) {
        super.putAll(columnToConds);
    }

    public List<Cond> getCondsByColumn(String col) {
        List<Cond> conds = super.get(col);
        if (conds == null) {
            conds = new ArrayList<>();
        }
        return ImmutableList.copyOf(conds);
    }

    @Override
    public Set<String> keySet() {
        return ImmutableSet.copyOf(super.keySet());
    }

    public static Map<String, String> getColumnWithType(String project, String table) {
        Map<String, String> columnWithType = new HashMap<>();
        ColumnDesc[] columns = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project) //
                .getTableDesc(table) //
                .getColumns(); //
        for (ColumnDesc column : columns) {
            columnWithType.put(column.getName(), column.getTypeName());
        }
        return columnWithType;
    }

    private static boolean isValidLikeColumnType(String type) {
        return type.startsWith("varchar") || type.equals("string") || type.startsWith("char");
    }

    public static String concatConds(ColumnToConds condsWithCol, ColumnToConds likeCondsWithCol,
            Map<String, String> columnWithType) {
        StrBuilder result = new StrBuilder();
        Set<String> conditionCols = new HashSet<>();
        conditionCols.addAll(condsWithCol.keySet());
        conditionCols.addAll(likeCondsWithCol.keySet());
        if (conditionCols.size() > 1) {
            result.append("(");
        }
        for (String col : conditionCols) {
            String type = Preconditions.checkNotNull(columnWithType.get(col), "column:" + col + " type not found");
            List<Cond> intervalConditions = condsWithCol.getCondsByColumn(col);
            List<Cond> likeConditions = likeCondsWithCol.getCondsByColumn(col);
            result.append("(");
            if (CollectionUtils.isNotEmpty(intervalConditions)) {
                if (intervalConditions.stream().allMatch(cond -> cond.type == IntervalType.CLOSED)) {
                    result.append(col).append(" in ").append("(")
                            .append(Joiner.on(COMMA)
                                    .join(intervalConditions.stream()
                                            .map(cond -> Cond.trimWithoutCheck(cond.leftExpr, type))
                                            .collect(Collectors.toList())))
                            .append(")");
                } else {
                    result.append(Joiner.on(" or ").join(intervalConditions.stream()
                            .map(cond -> cond.toString(col, type)).collect(Collectors.toList())));
                }
            }
            if (CollectionUtils.isNotEmpty(likeConditions)) {
                if (!isValidLikeColumnType(type)) {
                    logger.error(MsgPicker.getMsg().getRowAclNotStringType());
                } else {
                    if (CollectionUtils.isNotEmpty(intervalConditions)) {
                        result.append(" or ");
                    }
                    result.append(Joiner.on(" or ")
                            .join(likeConditions.stream()
                                    .map(cond -> col + " like " + Cond.trimWithoutCheck(cond.leftExpr, type))
                                    .collect(Collectors.toList())));
                }
            }
            result.append(")");
            result.append(" AND ");
        }

        if (!conditionCols.isEmpty()) {
            result.setLength(result.size() - 5);
        }

        if (conditionCols.size() > 1) {
            result.append(")");
        }
        return result.toString();
    }

    public static String preview(String project, String table, ColumnToConds condsWithColumn,
            ColumnToConds likeCondsWithColumn) {
        Map<String, String> columnWithType = Preconditions.checkNotNull(getColumnWithType(project, table));
        return concatConds(condsWithColumn, likeCondsWithColumn, columnWithType);
    }

    @JsonSerialize(using = Cond.RowACLCondSerializer.class)
    @JsonDeserialize(using = Cond.RowACLCondDeserializer.class)
    public static class Cond implements Serializable {
        public enum IntervalType implements Serializable {
            OPEN, // x in (a,b): a < x < b
            CLOSED, // x in [a,b]: a ≤ x ≤ b
            LEFT_INCLUSIVE, // x in [a,b): a ≤ x < b
            RIGHT_INCLUSIVE, // x in (a,b]: a < x ≤ b
            LIKE // x LIKE 'abc%'
        }

        private IntervalType type;
        private String leftExpr;
        private String rightExpr;

        //just for json deserialization
        Cond() {
        }

        public Cond(IntervalType type, String leftExpr, String rightExpr) {
            this.type = type;
            this.leftExpr = leftExpr;
            this.rightExpr = rightExpr;
        }

        public Cond(String value, IntervalType type) {
            this.type = type;
            this.leftExpr = this.rightExpr = value;
        }

        public String toString(String column, String columnType) {
            Pair<String, String> op = getOp(type);
            String leftValue = trimWithoutCheck(leftExpr, columnType);
            String rightValue = trimWithoutCheck(rightExpr, columnType);

            if (leftValue == null && rightValue != null) {
                if (type == IntervalType.OPEN) {
                    return "(" + column + "<" + rightValue + ")";
                } else if (type == IntervalType.RIGHT_INCLUSIVE) {
                    return "(" + column + "<=" + rightValue + ")";
                } else {
                    throw new KylinException(CommonErrorCode.UNKNOWN_ERROR_CODE, "error expr");
                }
            }

            if (rightValue == null && leftValue != null) {
                if (type == IntervalType.OPEN) {
                    return "(" + column + ">" + leftValue + ")";
                } else if (type == IntervalType.LEFT_INCLUSIVE) {
                    return "(" + column + ">=" + leftValue + ")";
                } else {
                    throw new KylinException(CommonErrorCode.UNKNOWN_ERROR_CODE, "error expr");
                }
            }

            if (leftValue == null /* implies rightValue == null too */ || leftValue.equals(rightValue)) {
                if (type == IntervalType.CLOSED) {
                    return "(" + column + "=" + leftValue + ")";
                }
                if (type == IntervalType.OPEN) {
                    return "(" + column + "<>" + leftValue + ")";
                }
            }
            return "(" + column + op.getFirst() + leftValue + " AND " + column + op.getSecond() + rightValue + ")";
        }

        //add cond with single quote and escape single quote
        public static String trim(String expr, String type) {
            if (expr == null) {
                return null;
            }
            if (isValidLikeColumnType(type)) {
                expr = expr.replace("'", "''");
                expr = "'" + expr + "'";
            }
            if (type.equals("date")) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault(Locale.Category.FORMAT));
                sdf.setTimeZone(TimeZone.getDefault());
                expr = sdf.format(new Date(Long.parseLong(expr)));
                expr = "DATE '" + expr + "'";
            }
            if (type.equals("timestamp") || type.equals("datetime")) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
                        Locale.getDefault(Locale.Category.FORMAT));
                sdf.setTimeZone(TimeZone.getDefault());
                expr = sdf.format(new Date(Long.parseLong(expr)));
                expr = "TIMESTAMP '" + expr + "'";
            }
            if (type.equals("time")) {
                final int TIME_START_POS = 11; //"1970-01-01 ".length() = 11
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
                        Locale.getDefault(Locale.Category.FORMAT));
                sdf.setTimeZone(TimeZone.getDefault());
                expr = sdf.format(new Date(Long.parseLong(expr)));
                //transform "1970-01-01 00:00:59" into "00:00:59"
                expr = "TIME '" + expr.substring(TIME_START_POS, expr.length()) + "'";
            }
            return expr;
        }

        static String trimWithoutCheck(String expr, String type) {
            if (expr == null) {
                return null;
            }
            if (type.startsWith("varchar") || type.equals("string") || type.equals("char")) {
                expr = expr.replace("'", "''");
                expr = "'" + expr + "'";
            }
            if (type.equals("date")) {
                expr = "DATE '" + expr + "'";
            }
            if (type.equals("timestamp") || type.equals("datetime")) {
                expr = "TIMESTAMP '" + expr + "'";
            }
            if (type.equals("time")) {
                final int TIME_START_POS = 11; //"1970-01-01 ".length() = 11
                //transform "1970-01-01 00:00:59" into "00:00:59"
                expr = "TIME '" + expr.substring(TIME_START_POS) + "'";
            }
            return expr;
        }

        private static Pair<String, String> getOp(Cond.IntervalType type) {
            switch (type) {
            case OPEN:
                return Pair.newPair(">", "<");
            case CLOSED:
                return Pair.newPair(">=", "<=");
            case LEFT_INCLUSIVE:
                return Pair.newPair(">=", "<");
            case RIGHT_INCLUSIVE:
                return Pair.newPair(">", "<=");
            default:
                throw new KylinException(CommonErrorCode.UNKNOWN_ERROR_CODE, "error, unknown type for condition");
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            Cond cond = (Cond) o;

            if (type != cond.type)
                return false;
            if (leftExpr != null ? !leftExpr.equals(cond.leftExpr) : cond.leftExpr != null)
                return false;
            return rightExpr != null ? rightExpr.equals(cond.rightExpr) : cond.rightExpr == null;
        }

        @Override
        public int hashCode() {
            int result = type != null ? type.hashCode() : 0;
            result = 31 * result + (leftExpr != null ? leftExpr.hashCode() : 0);
            result = 31 * result + (rightExpr != null ? rightExpr.hashCode() : 0);
            return result;
        }

        static class RowACLCondSerializer extends JsonSerializer<Cond> {

            @Override
            public void serialize(Cond cond, JsonGenerator gen, SerializerProvider serializers) throws IOException {
                Object[] c;
                if (cond.leftExpr.equals(cond.rightExpr)) {
                    c = new Object[2];
                    c[0] = cond.type.ordinal();
                    c[1] = cond.leftExpr;
                } else {
                    c = new Object[3];
                    c[0] = cond.type.ordinal();
                    c[1] = cond.leftExpr;
                    c[2] = cond.rightExpr;
                }
                gen.writeObject(c);
            }
        }

        static class RowACLCondDeserializer extends JsonDeserializer<Cond> {

            @Override
            public Cond deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
                Object[] c = p.readValueAs(Object[].class);
                Cond cond = new Cond();
                cond.type = IntervalType.values()[(int) c[0]];
                if (c.length == 2) {
                    cond.leftExpr = cond.rightExpr = (String) c[1];
                } else {
                    cond.leftExpr = (String) c[1];
                    cond.rightExpr = (String) c[2];
                }
                return cond;
            }
        }
    }
}
