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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.ws.rs.BadRequestException;

import io.kyligence.kap.query.util.KapQueryUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
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
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.metadata.realization.RoutingIndicatorException;
import org.apache.kylin.query.security.AccessDeniedException;
import org.apache.kylin.source.adhocquery.IPushDownRunner;
import org.apache.kylin.source.adhocquery.PushdownResult;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.query.exception.NoAuthorizedColsError;
import org.codehaus.commons.compiler.CompileException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import lombok.val;

public class PushDownUtil {
    private static final Logger logger = LoggerFactory.getLogger(PushDownUtil.class);

    private static final ExecutorService asyncExecutor = Executors.newCachedThreadPool();

    private PushDownUtil() {
    }

    public static PushdownResult tryPushDownQueryToIterator(QueryParams queryParams) throws Exception {

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        val prjManager = NProjectManager.getInstance(kylinConfig);
        val prj = prjManager.getProject(queryParams.getProject());
        String sql = queryParams.getSql();
        String project = queryParams.getProject();
        kylinConfig = prj.getConfig();
        if (!kylinConfig.isPushDownEnabled()) {
            SQLException sqlException = queryParams.getSqlException();
            if (queryParams.isForcedToPushDown() || (sqlException != null
                    && sqlException.getMessage().contains(QueryContext.ROUTE_USE_FORCEDTOTIEREDSTORAGE))) {
                throw new KylinException(QueryErrorCode.INVALID_PARAMETER_PUSH_DOWN,
                        MsgPicker.getMsg().getDisablePushDownPrompt());
            }
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

        IPushDownRunner runner = (IPushDownRunner) ClassUtil.newInstance(kylinConfig.getPushDownRunnerClassName());
        runner.init(kylinConfig);
        logger.debug("Query Pushdown runner {}", runner);

        // set pushdown engine in query context
        String pushdownEngine;
        // for file source
        int sourceType = kylinConfig.getManager(NProjectManager.class).getProject(queryParams.getProject())
                .getSourceType();
        if (sourceType == ISourceAware.ID_SPARK && KapConfig.getInstanceFromEnv().isCloud()) {
            pushdownEngine = QueryContext.PUSHDOWN_OBJECT_STORAGE;
        } else {
            pushdownEngine = runner.getName();
        }
        QueryContext.current().setPushdownEngine(pushdownEngine);

        queryParams.setKylinConfig(kylinConfig);
        queryParams.setSql(sql);
        try {
            sql = KapQueryUtil.massagePushDownSql(queryParams);
        } catch (NoAuthorizedColsError e) {
            // on no authorized cols found, return empty result
            return PushdownResult.emptyResult();
        }

        QueryContext.currentTrace().startSpan(QueryTrace.PREPARE_AND_SUBMIT_JOB);
        if (queryParams.isSelect()) {
            PushdownResult result = runner.executeQueryToIterator(sql, project);
            if (QueryContext.current().getQueryTagInfo().isAsyncQuery()) {
                AsyncQueryUtil.saveMetaDataAndFileInfo(QueryContext.current(), result.getColumnMetas());
            }
            return result;
        }

        return PushdownResult.emptyResult();
    }

    public static Pair<String, String> getMaxAndMinTimeWithTimeOut(String partitionColumn, String table, String project)
            throws Exception {

        Future<Pair<String, String>> pushDownTask = asyncExecutor.submit(() -> {
            try {
                return getMaxAndMinTime(partitionColumn, table, project);
            } catch (Exception e) {
                logger.error("Failed to get partition column latest data range by push down!", e);
                if (e instanceof KylinException) {
                    throw e;
                }
            }
            return null;
        });

        Pair<String, String> pushdownResult;
        try {
            pushdownResult = pushDownTask.get(30, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            pushDownTask.cancel(true);
            throw new KylinTimeoutException("The query exceeds the set time limit of "
                    + KylinConfig.getInstanceFromEnv().getQueryTimeoutSeconds()
                    + "s. Current step: Getting latest data range by push down. ");
        }

        return pushdownResult;
    }

    public static Pair<String, String> getMaxAndMinTime(String partitionColumn, String table, String project)
            throws Exception {
        Pair<String, String> pair = addBackTickForIdentity(table, partitionColumn);
        String sql = String.format(Locale.ROOT, "select min(%s), max(%s) from %s", pair.getSecond(), pair.getSecond(),
                pair.getFirst());
        Pair<String, String> result = new Pair<>();
        // pushdown
        List<List<String>> returnRows = PushDownUtil.selectPartitionColumn(sql, table, project).getFirst();

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
     * Use push down engine to select partition column
     */
    public static Pair<List<List<String>>, List<SelectedColumnMeta>> selectPartitionColumn(String sql, String table,
            String project) throws Exception {
        NTableMetadataManager tableMgr = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        final TableDesc tableDesc = tableMgr.getTableDesc(table);
        if (tableDesc.isView()) {
            throw new KylinException(VIEW_PARTITION_DATE_FORMAT_DETECTION_FORBIDDEN,
                    MsgPicker.getMsg().getViewDateFormatDetectionError());
        }
        val prjManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        val prj = prjManager.getProject(project);
        val kylinConfig = prj.getConfig();
        List<List<String>> returnRows = Lists.newArrayList();
        List<SelectedColumnMeta> returnColumnMeta = Lists.newArrayList();

        // pushdown
        IPushDownRunner runner = (IPushDownRunner) ClassUtil
                .newInstance(kylinConfig.getPartitionCheckRunnerClassNameWithDefaultValue());
        runner.init(kylinConfig);
        runner.executeQuery(sql, returnRows, returnColumnMeta, project);

        return Pair.newPair(returnRows, returnColumnMeta);
    }

    public static void trySimplePushDownExecute(String sql, String project) throws Exception {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        IPushDownRunner runner = (IPushDownRunner) ClassUtil.newInstance(kylinConfig.getPushDownRunnerClassName());
        runner.init(kylinConfig);
        runner.executeUpdate(sql, project);
    }

    public static String getFormatIfNotExist(String table, String partitionColumn, String project) throws Exception {
        Pair<String, String> pair = addBackTickForIdentity(table, partitionColumn);
        String sql = String.format(Locale.ROOT, "select %s from %s where %s is not null limit 1", pair.getSecond(),
                pair.getFirst(), pair.getSecond());

        // push down
        List<List<String>> returnRows = PushDownUtil.selectPartitionColumn(sql, table, project).getFirst();
        if (CollectionUtils.isEmpty(returnRows) || CollectionUtils.isEmpty(returnRows.get(0)))
            throw new KylinException(EMPTY_TABLE,
                    String.format(Locale.ROOT, MsgPicker.getMsg().getNoDataInTable(), table));

        return returnRows.get(0).get(0);
    }

    private static boolean isExpectedCause(SQLException sqlException) {
        Preconditions.checkArgument(sqlException != null);
        Throwable rootCause = ExceptionUtils.getRootCause(sqlException);

        //SqlValidatorException is not an excepted exception in the origin design.But in the multi pass scene,
        //query pushdown may create tables, and the tables are not in the model, so will throw SqlValidatorException.
        if (rootCause instanceof KylinTimeoutException) {
            return false;
        }

        if (rootCause instanceof AccessDeniedException) {
            return false;
        }

        if (rootCause instanceof RoutingIndicatorException) {
            return true;
        }

        if (rootCause instanceof CalciteNotSupportException) {
            return true;
        }

        if (rootCause instanceof CompileException) {
            return true;
        }

        if (QueryContext.current().getQueryTagInfo().isWithoutSyntaxError()) {
            logger.warn("route to push down for met error when running the query: {}",
                    QueryContext.current().getMetrics().getCorrectedSql(), sqlException);
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

    /**
     *
     * @param queryParams
     * @return
     * @throws Exception
     */
    public static Pair<List<List<String>>, List<SelectedColumnMeta>> tryPushDownQuery(QueryParams queryParams)
            throws Exception {
        val results = tryPushDownQueryToIterator(queryParams);
        if (results == null) {
            return null;
        }
        return new Pair<>(ImmutableList.copyOf(results.getRows()), results.getColumnMetas());
    }

    protected static Pair<String, String> addBackTickForIdentity(String table, String partitionColumn) {
        String tableName = Arrays.stream(table.split("\\.")).map(s -> "`" + s + "`").collect(Collectors.joining("."));
        String partitionColumnName = Arrays.stream(partitionColumn.split("\\.")).map(s -> "`" + s + "`")
                .collect(Collectors.joining("."));
        return Pair.newPair(tableName, partitionColumnName);
    }

}
