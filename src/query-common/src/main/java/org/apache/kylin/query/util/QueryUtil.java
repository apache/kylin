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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinTimeoutException;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.query.BigQueryThresholdUpdater;
import org.apache.kylin.query.IQueryTransformer;
import org.apache.kylin.query.SlowQueryDetector;
import org.apache.kylin.query.exception.UserStopQueryException;
import org.apache.kylin.query.relnode.KapJoinRel;
import org.apache.kylin.query.security.AccessDeniedException;
import org.apache.kylin.source.adhocquery.IPushDownConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class QueryUtil {

    private QueryUtil() {
    }

    private static final Logger log = LoggerFactory.getLogger("query");

    public static final String DEFAULT_SCHEMA = "DEFAULT";
    public static final ImmutableSet<String> REMOVED_TRANSFORMERS = ImmutableSet.of("ReplaceStringWithVarchar");
    private static final Pattern SELECT_PATTERN = Pattern.compile("^select", Pattern.CASE_INSENSITIVE);
    private static final Pattern SELECT_STAR_PTN = Pattern.compile("^select\\s+\\*\\p{all}*", Pattern.CASE_INSENSITIVE);
    private static final Pattern LIMIT_PATTERN = Pattern.compile("(limit\\s+\\d+)$", Pattern.CASE_INSENSITIVE);
    private static final String SELECT = "select";
    private static final String COLON = ":";
    private static final String SEMI_COLON = ";";
    public static final String JDBC = "jdbc";

    public static List<IQueryTransformer> queryTransformers = Collections.emptyList();
    public static List<IPushDownConverter> pushDownConverters = Collections.emptyList();

    public static boolean isSelectStatement(String sql) {
        String sql1 = sql.toLowerCase(Locale.ROOT);
        sql1 = removeCommentInSql(sql1);
        sql1 = sql1.trim();
        while (sql1.startsWith("(")) {
            sql1 = sql1.substring(1).trim();
        }

        return sql1.startsWith(SELECT) || (sql1.startsWith("with") && sql1.contains(SELECT))
                || (sql1.startsWith("explain") && sql1.contains(SELECT));
    }

    public static String removeCommentInSql(String sql) {
        // match two patterns, one is "-- comment", the other is "/* comment */"
        try {
            return new RawSqlParser(sql).parse().getStatementString();
        } catch (Exception ex) {
            log.error("Something unexpected while removing comments in the query, return original query", ex);
            return sql;
        }
    }

    public static String makeErrorMsgUserFriendly(Throwable e) {
        String msg = e.getMessage();

        // pick ParseException error message if possible
        Throwable cause = e;
        boolean needBreak = false;
        while (cause != null) {
            String className = cause.getClass().getName();
            if (className.contains("ParseException") || className.contains("NoSuchTableException")
                    || className.contains("NoSuchDatabaseException") || cause instanceof AccessDeniedException) {
                msg = cause.getMessage();
                needBreak = true;
            } else if (className.contains("ArithmeticException")) {
                msg = "ArithmeticException: " + cause.getMessage();
                needBreak = true;
            } else if (className.contains("NoStreamingRealizationFoundException")) {
                msg = "NoStreamingRealizationFoundException: " + cause.getMessage();
                needBreak = true;
            }
            if (needBreak) {
                break;
            }
            cause = cause.getCause();
        }

        return makeErrorMsgUserFriendly(msg);
    }

    public static String makeErrorMsgUserFriendly(String errorMsg) {
        if (StringUtils.isBlank(errorMsg)) {
            return errorMsg;
        }
        errorMsg = errorMsg.trim();
        String[] split = errorMsg.split(COLON);
        if (split.length == 3) {
            String prefix = "Error";
            if (StringUtils.startsWithIgnoreCase(split[0], prefix)) {
                split[0] = split[0].substring(prefix.length()).trim();
            }
            prefix = "while executing SQL";
            if (StringUtils.startsWith(split[0], prefix)) {
                split[0] = split[0].substring(0, prefix.length()) + COLON + split[0].substring(prefix.length());
            }
            return split[1].trim() + COLON + StringUtils.SPACE + split[2].trim() + "\n" + split[0];
        } else {
            return errorMsg;
        }
    }

    public static String addLimit(String originString) {
        if (StringUtils.isBlank(originString)) {
            return originString;
        }
        String replacedString = originString.trim();
        Matcher selectMatcher = SELECT_PATTERN.matcher(replacedString);
        if (!selectMatcher.find()) {
            return originString;
        }

        while (replacedString.endsWith(SEMI_COLON)) {
            replacedString = replacedString.substring(0, replacedString.length() - 1).trim();
        }

        Matcher limitMatcher = LIMIT_PATTERN.matcher(replacedString);
        return limitMatcher.find() ? originString : replacedString.concat(" limit 1");
    }

    public static String massageExpression(NDataModel model, String project, String expression,
            QueryContext.AclInfo aclInfo, boolean isForPushDown) {
        String tempConst = "'" + RandomUtil.randomUUIDStr() + "'";
        StringBuilder forCC = new StringBuilder();
        forCC.append("select ").append(expression).append(" ,").append(tempConst) //
                .append(" FROM ") //
                .append(model.getRootFactTable().getTableDesc().getDoubleQuoteIdentity());
        appendJoinStatement(model, forCC, false);

        String ccSql = KeywordDefaultDirtyHack.transform(forCC.toString());
        try {
            // massage nested CC for drafted model
            Map<String, NDataModel> modelMap = Maps.newHashMap();
            modelMap.put(model.getUuid(), model);
            ccSql = RestoreFromComputedColumn.convertWithGivenModels(ccSql, project, DEFAULT_SCHEMA, modelMap);
            QueryParams queryParams = new QueryParams(project, ccSql, DEFAULT_SCHEMA, false);
            queryParams.setKylinConfig(NProjectManager.getProjectConfig(project));
            queryParams.setAclInfo(aclInfo);

            if (isForPushDown) {
                ccSql = massagePushDownSql(queryParams);
            }
        } catch (Exception e) {
            log.warn("Failed to massage SQL expression [{}] with input model {}", ccSql, model.getUuid(), e);
        }

        return ccSql.substring("select ".length(), ccSql.indexOf(tempConst) - 2).trim();
    }

    public static String massageExpression(NDataModel model, String project, String expression,
            QueryContext.AclInfo aclInfo) {
        return massageExpression(model, project, expression, aclInfo, true);
    }

    public static String massageComputedColumn(NDataModel model, String project, ComputedColumnDesc cc,
            QueryContext.AclInfo aclInfo) {
        return massageExpression(model, project, cc.getExpression(), aclInfo);
    }

    public static void appendJoinStatement(NDataModel model, StringBuilder sql, boolean singleLine) {
        final String sep = singleLine ? " " : "\n";
        Set<TableRef> dimTableCache = Sets.newHashSet();
        sql.append(sep);
        for (JoinTableDesc lookupDesc : model.getJoinTables()) {
            JoinDesc join = lookupDesc.getJoin();
            TableRef dimTable = lookupDesc.getTableRef();
            if (join == null || StringUtils.isEmpty(join.getType()) || dimTableCache.contains(dimTable)) {
                continue;
            }

            TblColRef[] pk = join.getPrimaryKeyColumns();
            TblColRef[] fk = join.getForeignKeyColumns();
            if (pk.length != fk.length) {
                throw new IllegalArgumentException("Invalid join condition of lookup table:" + lookupDesc);
            }
            String joinType = join.getType().toUpperCase(Locale.ROOT);

            sql.append(String.format(Locale.ROOT, "%s JOIN \"%s\".\"%s\" as \"%s\"", //
                    joinType, dimTable.getTableDesc().getDatabase(), dimTable.getTableDesc().getName(),
                    dimTable.getAlias()));
            sql.append(sep);
            sql.append("ON ");

            if (pk.length == 0 && join.getNonEquiJoinCondition() != null) {
                sql.append(join.getNonEquiJoinCondition().getExpr());
                dimTableCache.add(dimTable);
            } else {
                String collect = IntStream.range(0, pk.length) //
                        .mapToObj(i -> fk[i].getDoubleQuoteExpressionInSourceDB() + " = "
                                + pk[i].getDoubleQuoteExpressionInSourceDB())
                        .collect(Collectors.joining(" AND ", "", sep));
                sql.append(collect);
                dimTableCache.add(dimTable);
            }
        }
    }

    public static SqlSelect extractSqlSelect(SqlCall selectOrOrderby) {
        SqlSelect sqlSelect = null;

        if (selectOrOrderby instanceof SqlSelect) {
            sqlSelect = (SqlSelect) selectOrOrderby;
        } else if (selectOrOrderby instanceof SqlOrderBy) {
            SqlOrderBy sqlOrderBy = ((SqlOrderBy) selectOrOrderby);
            if (sqlOrderBy.query instanceof SqlSelect) {
                sqlSelect = (SqlSelect) sqlOrderBy.query;
            }
        }

        return sqlSelect;
    }

    public static boolean isJoinOnlyOneAggChild(KapJoinRel joinRel) {
        RelNode joinLeftChild;
        RelNode joinRightChild;
        final RelNode joinLeft = joinRel.getLeft();
        final RelNode joinRight = joinRel.getRight();
        if (joinLeft instanceof RelSubset && joinRight instanceof RelSubset) {
            final RelSubset joinLeftChildSub = (RelSubset) joinLeft;
            final RelSubset joinRightChildSub = (RelSubset) joinRight;
            joinLeftChild = Util.first(joinLeftChildSub.getBest(), joinLeftChildSub.getOriginal());
            joinRightChild = Util.first(joinRightChildSub.getBest(), joinRightChildSub.getOriginal());

        } else if (joinLeft instanceof HepRelVertex && joinRight instanceof HepRelVertex) {
            joinLeftChild = ((HepRelVertex) joinLeft).getCurrentRel();
            joinRightChild = ((HepRelVertex) joinRight).getCurrentRel();
        } else {
            return false;
        }

        if (!isContainAggregate(joinLeftChild) && !isContainAggregate(joinRightChild)) {
            return false;
        }
        return !isContainAggregate(joinLeftChild) || !isContainAggregate(joinRightChild);
    }

    private static boolean isContainAggregate(RelNode node) {
        boolean[] isContainAggregate = new boolean[] { false };
        new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                if (isContainAggregate[0]) {
                    // pruning
                    return;
                }
                RelNode relNode = node;
                if (node instanceof RelSubset) {
                    relNode = Util.first(((RelSubset) node).getBest(), ((RelSubset) node).getOriginal());
                } else if (node instanceof HepRelVertex) {
                    relNode = ((HepRelVertex) node).getCurrentRel();
                }
                if (relNode instanceof Aggregate) {
                    isContainAggregate[0] = true;
                }
                super.visit(relNode, ordinal, parent);
            }
        }.go(node);
        return isContainAggregate[0];
    }

    public static boolean isCast(RexNode rexNode) {
        if (!(rexNode instanceof RexCall)) {
            return false;
        }
        return SqlKind.CAST == rexNode.getKind();
    }

    public static boolean isPlainTableColumn(int colIdx, RelNode relNode) {
        if (relNode instanceof HepRelVertex) {
            relNode = ((HepRelVertex) relNode).getCurrentRel();
        }
        if (relNode instanceof TableScan) {
            return true;
        } else if (relNode instanceof Join) {
            Join join = (Join) relNode;
            int offset = 0;
            for (RelNode input : join.getInputs()) {
                if (colIdx >= offset && colIdx < offset + input.getRowType().getFieldCount()) {
                    return isPlainTableColumn(colIdx - offset, input);
                }
                offset += input.getRowType().getFieldCount();
            }
        } else if (relNode instanceof Project) {
            RexNode inputRex = ((Project) relNode).getProjects().get(colIdx);
            if (inputRex instanceof RexInputRef) {
                return isPlainTableColumn(((RexInputRef) inputRex).getIndex(), ((Project) relNode).getInput());
            }
        } else if (relNode instanceof Filter) {
            return isPlainTableColumn(colIdx, relNode.getInput(0));
        }
        return false;
    }

    public static boolean containCast(RexNode rexNode) {
        if (!(rexNode instanceof RexCall)) {
            return false;
        }
        if (SqlKind.CAST == rexNode.getKind()) {
            RexNode operand = ((RexCall) rexNode).getOperands().get(0);
            return !(operand instanceof RexCall) || operand.getKind() == SqlKind.CASE;
        }

        return false;
    }

    public static boolean isNotNullLiteral(RexNode node) {
        return !isNullLiteral(node);
    }

    public static boolean isNullLiteral(RexNode node) {
        return node instanceof RexLiteral && ((RexLiteral) node).isNull();
    }

    public static String massageSql(QueryParams queryParams) {
        String massagedSql = normalMassageSql(queryParams.getKylinConfig(), queryParams.getSql(),
                queryParams.getLimit(), queryParams.getOffset());
        queryParams.setSql(massagedSql);
        massagedSql = transformSql(queryParams);
        QueryContext.current().record("massage");
        return massagedSql;
    }

    public static String massageSqlAndExpandCC(QueryParams queryParams) {
        String massaged = massageSql(queryParams);
        return new RestoreFromComputedColumn().convert(massaged, queryParams.getProject(),
                queryParams.getDefaultSchema());
    }

    private static String transformSql(QueryParams queryParams) {
        // customizable SQL transformation
        initQueryTransformersIfNeeded(queryParams.getKylinConfig(), queryParams.isCCNeeded());
        String sql = queryParams.getSql();
        for (IQueryTransformer t : queryTransformers) {
            QueryUtil.checkThreadInterrupted("Interrupted sql transformation at the stage of " + t.getClass(),
                    "Current step: SQL transformation.");
            sql = t.transform(sql, queryParams.getProject(), queryParams.getDefaultSchema());
        }
        return sql;
    }

    private static String trimRightSemiColon(String sql) {
        while (sql.endsWith(SEMI_COLON)) {
            sql = sql.substring(0, sql.length() - 1).trim();
        }
        return sql;
    }

    public static String normalMassageSql(KylinConfig kylinConfig, String sql, int limit, int offset) {
        sql = sql.trim();
        sql = sql.replace("\r", StringUtils.SPACE).replace("\n", System.getProperty("line.separator"));
        sql = trimRightSemiColon(sql);

        //Split keywords and variables from sql by punctuation and whitespace character
        List<String> sqlElements = Lists.newArrayList(sql.toLowerCase(Locale.ROOT).split("(?![._'\"`])\\p{P}|\\s+"));

        Integer maxRows = kylinConfig.getMaxResultRows();
        if (maxRows != null && maxRows > 0 && (maxRows < limit || limit <= 0)) {
            limit = maxRows;
        }

        // https://issues.apache.org/jira/browse/KYLIN-2649
        if (kylinConfig.getForceLimit() > 0 && limit <= 0 && !sql.toLowerCase(Locale.ROOT).contains("limit")
                && SELECT_STAR_PTN.matcher(sql).find()) {
            limit = kylinConfig.getForceLimit();
        }

        if (checkBigQueryPushDown(kylinConfig)) {
            long bigQueryThreshold = BigQueryThresholdUpdater.getBigQueryThreshold();
            if (limit <= 0 && bigQueryThreshold > 0) {
                log.info("Big query route to pushdown, Add limit {} to sql.", bigQueryThreshold);
                limit = (int) bigQueryThreshold;
            }
        }

        if (limit > 0 && !sqlElements.contains("limit")) {
            sql += ("\nLIMIT " + limit);
        }

        if (offset > 0 && !sqlElements.contains("offset")) {
            sql += ("\nOFFSET " + offset);
        }

        return sql;
    }

    public static boolean checkBigQueryPushDown(KylinConfig kylinConfig) {
        return kylinConfig.isBigQueryPushDown()
                && JDBC.equals(KapConfig.getInstanceFromEnv().getShareStateSwitchImplement());
    }

    public static void initQueryTransformersIfNeeded(KylinConfig kylinConfig, boolean isCCNeeded) {
        String[] currentTransformers = queryTransformers.stream().map(Object::getClass).map(Class::getCanonicalName)
                .toArray(String[]::new);
        String[] configTransformers = kylinConfig.getQueryTransformers();
        boolean containsCCTransformer = Arrays.asList(configTransformers)
                .contains("org.apache.kylin.query.util.ConvertToComputedColumn");
        boolean transformersEqual = Objects.deepEquals(currentTransformers, configTransformers);
        if (transformersEqual && (isCCNeeded || !containsCCTransformer)) {
            return;
        }

        List<IQueryTransformer> transformers = initTransformers(isCCNeeded, configTransformers);
        queryTransformers = Collections.unmodifiableList(transformers);
        log.debug("SQL transformer: {}", queryTransformers);
    }

    public static List<IQueryTransformer> initTransformers(boolean isCCNeeded, String[] configTransformers) {
        List<IQueryTransformer> transformers = Lists.newArrayList();
        List<String> classList = Lists.newArrayList(configTransformers);
        classList.removeIf(clazz -> {
            String name = clazz.substring(clazz.lastIndexOf(".") + 1);
            return REMOVED_TRANSFORMERS.contains(name);
        });

        for (String clz : classList) {
            if (!isCCNeeded && clz.equals("org.apache.kylin.query.util.ConvertToComputedColumn"))
                continue;

            try {
                IQueryTransformer t = (IQueryTransformer) ClassUtil.newInstance(clz);

                transformers.add(t);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to init query transformer", e);
            }
        }
        return transformers;
    }

    // fix KE-34379，filter "/*+ MODEL_PRIORITY({cube_name}) */" hint
    private static final Pattern SQL_HINT_ERASER = Pattern
            .compile("/\\*\\s*\\+\\s*(?i)MODEL_PRIORITY\\s*\\([\\s\\S]*\\)\\s*\\*/");

    public static String massagePushDownSql(QueryParams queryParams) {
        String sql = queryParams.getSql();
        sql = trimRightSemiColon(sql);

        sql = SQL_HINT_ERASER.matcher(sql).replaceAll("");
        initPushDownConvertersIfNeeded(queryParams.getKylinConfig());
        for (IPushDownConverter converter : pushDownConverters) {
            QueryUtil.checkThreadInterrupted("Interrupted sql transformation at the stage of " + converter.getClass(),
                    "Current step: Massage push-down sql. ");
            sql = converter.convert(sql, queryParams.getProject(), queryParams.getDefaultSchema());
        }
        return sql;
    }

    static void initPushDownConvertersIfNeeded(KylinConfig kylinConfig) {
        String[] currentConverters = pushDownConverters.stream().map(Object::getClass).map(Class::getCanonicalName)
                .toArray(String[]::new);
        String[] configConverters = kylinConfig.getPushDownConverterClassNames();
        boolean skipInit = Objects.deepEquals(currentConverters, configConverters);

        if (skipInit) {
            return;
        }

        List<IPushDownConverter> converters = Lists.newArrayList();
        for (String clz : configConverters) {
            try {
                IPushDownConverter converter = (IPushDownConverter) ClassUtil.newInstance(clz);
                converters.add(converter);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to init pushdown converter", e);
            }
        }
        pushDownConverters = Collections.unmodifiableList(converters);
    }

    public static void checkThreadInterrupted(String errorMsgLog, String stepInfo) {
        if (Thread.currentThread().isInterrupted()) {
            log.error("{} {}", QueryContext.current().getQueryId(), errorMsgLog);
            if (SlowQueryDetector.getRunningQueries().get(Thread.currentThread()).isStopByUser()) {
                throw new UserStopQueryException("");
            }
            QueryContext.current().getQueryTagInfo().setTimeout(true);
            throw new KylinTimeoutException("The query exceeds the set time limit of "
                    + KylinConfig.getInstanceFromEnv().getQueryTimeoutSeconds() + "s. " + stepInfo);
        }
    }
}
