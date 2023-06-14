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

import static org.apache.kylin.common.exception.QueryErrorCode.EMPTY_TABLE;
import static org.apache.kylin.common.exception.ServerErrorCode.VIEW_PARTITION_DATE_FORMAT_DETECTION_FORBIDDEN;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.ws.rs.BadRequestException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.QueryTrace;
import org.apache.kylin.common.exception.CalciteNotSupportException;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.KylinTimeoutException;
import org.apache.kylin.common.exception.QueryErrorCode;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringHelper;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.metadata.realization.RoutingIndicatorException;
import org.apache.kylin.query.exception.NoAuthorizedColsError;
import org.apache.kylin.query.security.AccessDeniedException;
import org.apache.kylin.source.adhocquery.IPushDownConverter;
import org.apache.kylin.source.adhocquery.IPushDownRunner;
import org.apache.kylin.source.adhocquery.PushdownResult;
import org.codehaus.commons.compiler.CompileException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;

public class PushDownUtil {

    private static final Logger logger = LoggerFactory.getLogger("query");

    // sql hint "/*+ MODEL_PRIORITY({cube_name}) */" 
    private static final Pattern SQL_HINT_PATTERN = Pattern
            .compile("/\\*\\s*\\+\\s*(?i)MODEL_PRIORITY\\s*\\([\\s\\S]*\\)\\s*\\*/");
    public static final String DEFAULT_SCHEMA = "DEFAULT";
    private static final String CC_SPLITTER = "'##CC_PUSH_DOWN_TOKEN##'";
    private static final String UNDER_LINE = "_";
    private static final ExecutorService asyncExecutor = Executors.newCachedThreadPool();
    private static final Map<String, IPushDownConverter> PUSH_DOWN_CONVERTER_MAP = Maps.newConcurrentMap();

    static {
        String[] classNames = KylinConfig.getInstanceFromEnv().getPushDownConverterClassNames();
        for (String clz : classNames) {
            try {
                IPushDownConverter converter = (IPushDownConverter) ClassUtil.newInstance(clz);
                PUSH_DOWN_CONVERTER_MAP.put(clz, converter);
            } catch (Exception e) {
                logger.error("Failed to init push-down converter of the sys-config: {}", clz);
            }
        }
    }

    private PushDownUtil() {
    }

    public static PushdownResult tryIterQuery(QueryParams queryParams) throws SQLException {
        KylinConfig projectConfig = NProjectManager.getProjectConfig(queryParams.getProject());
        queryParams.setKylinConfig(projectConfig);
        if (!projectConfig.isPushDownEnabled()) {
            checkPushDownIncapable(queryParams);
            return null;
        }
        if (queryParams.isSelect()) {
            logger.info("Query:[{}] failed to utilize pre-calculation, routing to other engines",
                    QueryContext.current().getMetrics().getCorrectedSql(), queryParams.getSqlException());
            if (!queryParams.isForcedToPushDown() && !isExpectedCause(queryParams.getSqlException())) {
                logger.info("quit doPushDownQuery because prior exception thrown is unexpected");
                return null;
            }
        } else {
            Preconditions.checkState(queryParams.getSqlException() == null);
            logger.info("Kylin cannot support non-select queries, routing to other engines");
        }

        // Set a push-down engine for query context.
        IPushDownRunner runner = (IPushDownRunner) ClassUtil.newInstance(projectConfig.getPushDownRunnerClassName());
        runner.init(projectConfig, queryParams.getProject());
        logger.debug("Query Pushdown runner {}", runner);
        int sourceType = projectConfig.getDefaultSource();
        String engine = sourceType == ISourceAware.ID_SPARK && KapConfig.getInstanceFromEnv().isCloud()
                ? QueryContext.PUSHDOWN_OBJECT_STORAGE
                : runner.getName();
        QueryContext.current().setPushdownEngine(engine);

        String sql;
        try {
            sql = massagePushDownSql(queryParams);
        } catch (NoAuthorizedColsError e) {
            // on no authorized cols found, return empty result
            return PushdownResult.emptyResult();
        }

        QueryContext.currentTrace().startSpan(QueryTrace.PREPARE_AND_SUBMIT_JOB);
        if (queryParams.isSelect()) {
            PushdownResult result = runner.executeQueryToIterator(sql, queryParams.getProject());
            if (QueryContext.current().getQueryTagInfo().isAsyncQuery()) {
                AsyncQueryUtil.saveMetaDataAndFileInfo(QueryContext.current(), result.getColumnMetas());
            }
            return result;
        }
        return PushdownResult.emptyResult();
    }

    private static void checkPushDownIncapable(QueryParams queryParams) {
        SQLException sqlException = queryParams.getSqlException();
        if (queryParams.isForcedToPushDown() || (sqlException != null
                && sqlException.getMessage().contains(QueryContext.ROUTE_USE_FORCEDTOTIEREDSTORAGE))) {
            throw new KylinException(QueryErrorCode.INVALID_PARAMETER_PUSH_DOWN,
                    MsgPicker.getMsg().getDisablePushDownPrompt());
        }
    }

    public static String massageComputedColumn(NDataModel model, String project, ComputedColumnDesc cc,
            QueryContext.AclInfo aclInfo) {
        return massageExpression(model, project, cc.getExpression(), aclInfo);
    }

    public static String massageExpression(NDataModel model, String project, String expression,
            QueryContext.AclInfo aclInfo) {
        if (StringUtils.isBlank(expression)) {
            return "";
        }

        String ccSql = expandComputedColumnExp(model, project, StringHelper.backtickToDoubleQuote(expression));
        QueryParams queryParams = new QueryParams(project, ccSql, PushDownUtil.DEFAULT_SCHEMA, false);
        queryParams.setKylinConfig(NProjectManager.getProjectConfig(project));
        queryParams.setAclInfo(aclInfo);
        String s = massagePushDownSql(queryParams);
        return s.substring("select ".length(), s.indexOf(CC_SPLITTER) - 2).trim();
    }

    public static String massagePushDownSql(QueryParams queryParams) {
        if (queryParams.getSql() == null) {
            return StringUtils.EMPTY;
        }

        String sql = queryParams.getSql();
        sql = QueryUtil.trimRightSemiColon(sql);
        sql = SQL_HINT_PATTERN.matcher(sql).replaceAll("");

        List<IPushDownConverter> pushDownConverters = fetchConverters(queryParams.getKylinConfig());
        if (logger.isDebugEnabled()) {
            logger.debug("All used push-down converters are: {}", pushDownConverters.stream()
                    .map(c -> c.getClass().getCanonicalName()).collect(Collectors.joining(",")));
        }
        for (IPushDownConverter converter : pushDownConverters) {
            QueryInterruptChecker.checkThreadInterrupted("Interrupted sql transformation at the stage of " + converter.getClass(),
                    "Current step: Massage push-down sql. ");
            sql = converter.convert(sql, queryParams.getProject(), queryParams.getDefaultSchema());
        }
        sql = replaceEscapedQuote(sql);
        return sql;
    }

    static List<IPushDownConverter> fetchConverters(KylinConfig kylinConfig) {
        List<IPushDownConverter> converters = Lists.newArrayList();
        for (String clz : kylinConfig.getPushDownConverterClassNames()) {
            if (PUSH_DOWN_CONVERTER_MAP.containsKey(clz)) {
                converters.add(PUSH_DOWN_CONVERTER_MAP.get(clz));
            } else {
                try {
                    IPushDownConverter converter = (IPushDownConverter) ClassUtil.newInstance(clz);
                    PUSH_DOWN_CONVERTER_MAP.put(clz, converter);
                    converters.add(PUSH_DOWN_CONVERTER_MAP.get(clz));
                } catch (Exception e) {
                    throw new IllegalStateException("Failed to init pushdown converter", e);
                }
            }
        }
        return converters;
    }

    /**
     * Generate flat-table SQL which can be parsed by Apache Calcite. 
     * If querying push-down is required, please use it in conjunction with {@link PushDownUtil#massagePushDownSql}.
     */
    public static String generateFlatTableSql(NDataModel model, boolean singleLine) {
        String sep = singleLine ? " " : "\n";
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT ").append(sep);

        List<TblColRef> tblColRefs = Lists.newArrayList(model.getEffectiveCols().values());
        if (tblColRefs.isEmpty()) {
            sqlBuilder.append("1 ").append(sep);
        } else {
            String allColStr = tblColRefs.stream() //
                    .filter(colRef -> !colRef.getColumnDesc().isComputedColumn()) //
                    .map(colRef -> {
                        String s = colRef.getTableAlias() + PushDownUtil.UNDER_LINE + colRef.getName();
                        String colName = StringHelper.doubleQuote(s);
                        return colRef.getDoubleQuoteExp() + " as " + colName + sep;
                    }).collect(Collectors.joining(", "));
            sqlBuilder.append(allColStr);
        }

        sqlBuilder.append("FROM ").append(model.getRootFactTable().getTableDesc().getDoubleQuoteIdentity())
                .append(" as ").append(StringHelper.doubleQuote(model.getRootFactTable().getAlias()));
        appendJoinStatement(model, sqlBuilder, singleLine);

        sqlBuilder.append("WHERE ").append(sep);
        sqlBuilder.append("1 = 1").append(sep);
        if (StringUtils.isNotEmpty(model.getFilterCondition())) {
            String filterConditionWithCalciteFormat = QueryUtil.adaptCalciteSyntax(model.getFilterCondition());
            sqlBuilder.append(" AND (").append(filterConditionWithCalciteFormat).append(") ").append(sep);
        }

        return new EscapeTransformer().transform(sqlBuilder.toString());
    }

    public static String expandComputedColumnExp(NDataModel model, String project, String expression) {
        StringBuilder forCC = new StringBuilder();
        forCC.append("select ").append(expression).append(" ,").append(CC_SPLITTER) //
                .append(" FROM ") //
                .append(model.getRootFactTable().getTableDesc().getDoubleQuoteIdentity());
        appendJoinStatement(model, forCC, false);

        String ccSql = KeywordDefaultDirtyHack.transform(forCC.toString());
        try {
            // massage nested CC for drafted model
            Map<String, NDataModel> modelMap = Maps.newHashMap();
            modelMap.put(model.getUuid(), model);
            ccSql = new EscapeTransformer().transform(ccSql);
            ccSql = RestoreFromComputedColumn.convertWithGivenModels(ccSql, project, DEFAULT_SCHEMA, modelMap);
        } catch (Exception e) {
            logger.warn("Failed to massage SQL expression [{}] with input model {}", ccSql, model.getUuid(), e);
        }
        return ccSql;
    }

    public static void appendJoinStatement(NDataModel model, StringBuilder sql, boolean singleLine) {
        final String sep = singleLine ? " " : "\n";
        Set<TableRef> dimTableCache = Sets.newHashSet();
        sql.append(sep);
        for (JoinTableDesc lookupDesc : model.getJoinTables()) {
            JoinDesc join = lookupDesc.getJoin();
            TableRef dimTable = lookupDesc.getTableRef();
            if (join == null || org.apache.commons.lang3.StringUtils.isEmpty(join.getType())
                    || dimTableCache.contains(dimTable)) {
                continue;
            }

            TblColRef[] pk = join.getPrimaryKeyColumns();
            TblColRef[] fk = join.getForeignKeyColumns();
            if (pk.length != fk.length) {
                throw new IllegalStateException("Invalid join condition of lookup table: " + lookupDesc);
            }
            String joinType = join.getType().toUpperCase(Locale.ROOT);

            sql.append(joinType).append(" JOIN ").append(doubleQuote(dimTable)) //
                    .append(" as ").append(StringHelper.doubleQuote(dimTable.getAlias())) //
                    .append(sep).append("ON ");

            if (pk.length == 0 && join.getNonEquiJoinCondition() != null) {
                sql.append(join.getNonEquiJoinCondition().getExpr());
                dimTableCache.add(dimTable);
            } else {
                sql.append(concatEqualJoinCondition(pk, fk, sep));
                dimTableCache.add(dimTable);
            }
        }
    }

    private static String doubleQuote(TableRef tableRef) {
        TableDesc table = tableRef.getTableDesc();
        return StringHelper.doubleQuote(table.getDatabase()) + "." + StringHelper.doubleQuote(table.getName());
    }

    private static String concatEqualJoinCondition(TblColRef[] pk, TblColRef[] fk, String sep) {
        StringJoiner joiner = new StringJoiner(" AND ", "", sep);
        for (int i = 0; i < pk.length; i++) {
            String s = fk[i].getDoubleQuoteExp() + " = " + pk[i].getDoubleQuoteExp();
            joiner.add(s);
        }
        return joiner.toString();
    }

    /**
     * Replace the escaped quote {@code ''} with {@code \'}. <br/>
     * For example: <br/>
     * the origin sql is {@code select 'a''b' from t} <br/>
     * the replaced sql is {@code select 'a\'b' from t}
     * @param sql the input sql
     * @return replaced sql
     */
    static String replaceEscapedQuote(String sql) {
        boolean inStrVal = false;
        boolean needTransfer = false;
        char[] res = sql.toCharArray();
        for (int i = 0; i < res.length; i++) {
            if (res[i] == '\'') {
                if (inStrVal) {
                    if (needTransfer) {
                        res[i - 1] = '\\';
                        needTransfer = false;
                    } else {
                        needTransfer = true;
                    }
                } else {
                    inStrVal = true;
                }
            } else if (needTransfer) {
                inStrVal = false;
                needTransfer = false;
            }
        }
        return new String(res);
    }

    public static Pair<String, String> probeMinMaxTsWithTimeout(String partitionColumn, String table, String project)
            throws ExecutionException, InterruptedException {

        Future<Pair<String, String>> pushDownTask = asyncExecutor.submit(() -> {
            try {
                return probeMinMaxTs(partitionColumn, table, project);
            } catch (Exception e) {
                logger.error("Failed to get partition column latest data range by push down!", e);
                if (e instanceof KylinException) {
                    throw e;
                }
            }
            return null;
        });

        Pair<String, String> result;
        try {
            result = pushDownTask.get(30, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            pushDownTask.cancel(true);
            throw new KylinTimeoutException("The query exceeds the set time limit of "
                    + KylinConfig.getInstanceFromEnv().getQueryTimeoutSeconds()
                    + "s. Current step: Getting latest data range by push down. ");
        }

        return result;
    }

    public static Pair<String, String> probeMinMaxTs(String partitionColumn, String table, String project)
            throws SQLException {
        String t = String.join(".", backtickQuote(table.split("\\.")));
        String pc = String.join(".", backtickQuote(partitionColumn.split("\\.")));
        String sql = String.format(Locale.ROOT, "select min(%s), max(%s) from %s", pc, pc, t);

        Pair<String, String> result = new Pair<>();
        List<List<String>> returnRows = probePartitionColInfo(sql, table, project).getFirst();
        if (returnRows.isEmpty() || returnRows.get(0).get(0) == null || returnRows.get(0).get(1) == null)
            throw new BadRequestException(String.format(Locale.ROOT, MsgPicker.getMsg().getNoDataInTable(), table));

        result.setFirst(returnRows.get(0).get(0));
        result.setSecond(returnRows.get(0).get(1));

        return result;
    }

    public static boolean needPushdown(String start, String end) {
        return StringUtils.isEmpty(start) && StringUtils.isEmpty(end);
    }

    /**
     * Use push-down engine to detect partition column info.
     */
    public static Pair<List<List<String>>, List<SelectedColumnMeta>> probePartitionColInfo(String sql, String table,
            String project) throws SQLException {
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        final TableDesc tableDesc = tableMgr.getTableDesc(table);
        if (tableDesc.isView()) {
            throw new KylinException(VIEW_PARTITION_DATE_FORMAT_DETECTION_FORBIDDEN,
                    MsgPicker.getMsg().getViewDateFormatDetectionError());
        }

        List<List<String>> returnRows = Lists.newArrayList();
        List<SelectedColumnMeta> returnColumnMeta = Lists.newArrayList();

        KylinConfig projectConfig = NProjectManager.getProjectConfig(project);
        IPushDownRunner runner = (IPushDownRunner) ClassUtil
                .newInstance(projectConfig.getDefaultPartitionCheckerClassName());
        runner.init(projectConfig, project);
        runner.executeQuery(sql, returnRows, returnColumnMeta, project);

        return Pair.newPair(returnRows, returnColumnMeta);
    }

    public static void trySimplyExecute(String sql, String project) throws SQLException {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        IPushDownRunner runner = (IPushDownRunner) ClassUtil.newInstance(kylinConfig.getPushDownRunnerClassName());
        runner.init(kylinConfig, project);
        runner.executeUpdate(sql, project);
    }

    public static String probeColFormat(String table, String partitionColumn, String project) throws SQLException {
        String t = String.join(".", backtickQuote(table.split("\\.")));
        String sql = String.format(Locale.ROOT, "select %s from %s where %s is not null limit 1", partitionColumn, t,
                partitionColumn);
        return probe(sql, table, project);
    }

    public static String probeExpFormat(String project, String table, String expression) throws SQLException {
        String t = String.join(".", backtickQuote(table.split("\\.")));
        String sql = String.format(Locale.ROOT, "select %s from %s limit 1", expression, t);
        return probe(sql, table, project);
    }

    private static String probe(String sql, String table, String project) throws SQLException {
        List<List<String>> returnRows = probePartitionColInfo(sql, table, project).getFirst();
        if (CollectionUtils.isEmpty(returnRows) || CollectionUtils.isEmpty(returnRows.get(0))) {
            throw new KylinException(EMPTY_TABLE,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getNoDataInTable(), table));
        }
        return returnRows.get(0).get(0);
    }

    public static List<String> backtickQuote(String[] arr) {
        return Arrays.stream(arr).map(StringHelper::backtickQuote).collect(Collectors.toList());
    }

    private static boolean isExpectedCause(SQLException sqlException) {
        Preconditions.checkArgument(sqlException != null);
        Throwable rootCause = ExceptionUtils.getRootCause(sqlException);

        //SqlValidatorException is not an excepted exception in the origin design.But in the multi pass scene,
        //query pushdown may create tables, and the tables are not in the model, so will throw SqlValidatorException.
        if (rootCause instanceof KylinTimeoutException || rootCause instanceof AccessDeniedException) {
            return false;
        } else if (rootCause instanceof RoutingIndicatorException || rootCause instanceof CalciteNotSupportException
                || rootCause instanceof CompileException) {
            return true;
        }

        if (QueryContext.current().getQueryTagInfo().isWithoutSyntaxError()) {
            logger.warn("route to push down for met error when running the query: {}",
                    QueryContext.current().getMetrics().getCorrectedSql(), sqlException);
            return true;
        }

        //SqlValidatorException about TIMESTAMPADD and TIMESTAMPDIFF is expected
        if (rootCause.getMessage().contains("TIMESTAMPADD") || rootCause.getMessage().contains("TIMESTAMPDIFF")) {
            return true;
        }

        return false;
    }

    public static String calcStart(String start, SegmentRange<?> coveredRange) {
        if (coveredRange != null) {
            start = coveredRange.getEnd().toString();
        }
        return start;
    }
}
