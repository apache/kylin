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

package org.apache.kylin.rest.service;

import static org.apache.kylin.common.exception.ServerErrorCode.GLUTEN_NOT_ENABLED_ERROR;
import static org.apache.kylin.common.exception.ServerErrorCode.INTERNAL_TABLE_ERROR;
import static org.apache.kylin.common.msg.Message.LOAD_GLUTEN_CACHE_EXECUTE_ERROR;
import static org.apache.kylin.metadata.model.SegmentStatusEnum.READY;
import static org.apache.kylin.metadata.model.SegmentStatusEnum.WARNING;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.constant.LogConstant;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.logging.SetLogCategory;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.rest.model.GlutenCacheExecuteResult;
import org.apache.kylin.rest.request.IndexGlutenCacheRequest;
import org.apache.kylin.rest.request.InternalTableGlutenCacheRequest;
import org.apache.kylin.rest.response.GlutenCacheResponse;
import org.apache.kylin.rest.util.GlutenCacheRequestLimits;
import org.apache.kylin.util.DataRangeUtils;
import org.apache.kylin.utils.GlutenCacheUtils;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component("glutenCacheService")
public class GlutenCacheService extends BasicService {

    @Autowired
    private ModelService modelService;
    @Autowired
    private RouteService routeService;

    public GlutenCacheResponse glutenCache(List<String> cacheCommands) {
        val executeResults = Lists.<GlutenCacheExecuteResult> newArrayList();
        try (SetLogCategory ignore = new SetLogCategory(LogConstant.BUILD_CATEGORY)) {
            val sparkSession = SparderEnv.getSparkSession();
            val successCount = new AtomicInteger(0);
            for (String cacheCommand : cacheCommands) {
                val glutenCacheExecuteResult = glutenCacheExecute(cacheCommand, sparkSession, successCount);
                executeResults.add(glutenCacheExecuteResult);
            }
            val result = successCount.get() == cacheCommands.size();
            return new GlutenCacheResponse(result, executeResults, StringUtils.EMPTY);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new GlutenCacheResponse(executeResults, e.getMessage());
        }
    }

    private GlutenCacheExecuteResult glutenCacheExecute(String cacheCommand, SparkSession sparkSession,
            AtomicInteger successCount) {
        try {
            val rows = sparkSession.sql(cacheCommand).collectAsList();

            if (CollectionUtils.isEmpty(rows)) {
                throw new KylinRuntimeException(
                        String.format(Locale.ROOT, LOAD_GLUTEN_CACHE_EXECUTE_ERROR, "Result rows is empty!!"));
            }
            val row = rows.get(0);
            if (row.getBoolean(0)) {
                successCount.incrementAndGet();
                return new GlutenCacheExecuteResult(true, StringUtils.EMPTY, cacheCommand);
            }

            val reason = row.getString(1);
            val exception = StringUtils.isBlank(reason)
                    ? String.format(Locale.ROOT, LOAD_GLUTEN_CACHE_EXECUTE_ERROR, "Result exception is empty!!")
                    : reason;
            throw new KylinRuntimeException(exception);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return new GlutenCacheExecuteResult(false, e.getMessage(), cacheCommand);
        }
    }

    public void internalTableGlutenCache(InternalTableGlutenCacheRequest request, HttpServletRequest servletRequest)
            throws Exception {
        val config = KylinConfig.getInstanceFromEnv();
        if (!config.queryUseGlutenEnabled()) {
            throw new KylinException(GLUTEN_NOT_ENABLED_ERROR, MsgPicker.getMsg().getGlutenDisabled());
        }
        val project = request.getProject();
        val projectConfig = NProjectManager.getProjectConfig(project);
        if (!projectConfig.isInternalTableEnabled()) {
            throw new KylinException(INTERNAL_TABLE_ERROR, MsgPicker.getMsg().getInternalTableDisabled());
        }
        val columns = request.getColumns().stream().map(StringUtils::upperCase).collect(Collectors.toList());
        val table = StringUtils.upperCase(request.getDatabase() + "." + request.getTable());
        val start = request.getStart();
        val cacheTableCommand = GlutenCacheUtils.generateCacheTableCommand(getConfig(), project, table, start, columns,
                true);
        log.info("InternalTable[{}] cache command is [{}]", table, cacheTableCommand);
        routeService.routeGlutenCacheAsync(Lists.newArrayList(cacheTableCommand), servletRequest);
    }

    public void indexGlutenCache(IndexGlutenCacheRequest request, HttpServletRequest servletRequest) throws Exception {
        val config = KylinConfig.getInstanceFromEnv();
        if (!config.queryUseGlutenEnabled()) {
            throw new KylinException(GLUTEN_NOT_ENABLED_ERROR, MsgPicker.getMsg().getGlutenDisabled());
        }
        val project = request.getProject();
        val modelAlias = request.getModel();
        val indexes = request.getIndexes();
        val start = request.getStart();
        val end = request.getEnd();
        val model = modelService.getModel(modelAlias, project);
        val cacheCommands = Lists.<String> newArrayList();
        switch (model.getStorageType()) {
        case V1:
            if (CollectionUtils.isNotEmpty(indexes)) {
                throw new KylinRuntimeException(String.format(Locale.ROOT,
                        "Model[%s] StorageType is V1, indexes need null or empty", modelAlias));
            }
            DataRangeUtils.validateDataRange(start, end, null);
            val segments = modelService.getSegmentsByRange(model.getId(), project, start, end).getSegments(READY,
                    WARNING);
            val resultBySegments = GlutenCacheUtils.generateModelCacheCommandsBySegments(getConfig(), project,
                    model.getId(), segments);
            cacheCommands.addAll(resultBySegments);
            break;
        case DELTA:
            if (StringUtils.isNotEmpty(start) || StringUtils.isNotEmpty(end)) {
                throw new KylinRuntimeException(
                        String.format(Locale.ROOT, "Model[%s] StorageType is V3, start/end need null", modelAlias));
            }
            val resultByIndexes = GlutenCacheUtils.generateModelCacheCommandsByIndexes(getConfig(), project,
                    model.getId(), indexes);
            cacheCommands.addAll(resultByIndexes);
            break;
        default:
            throw new KylinRuntimeException("Model StorageType not support load gluten cache");
        }
        log.info("Model[{}] cache command is [{}]", modelAlias, cacheCommands);
        routeService.routeGlutenCacheAsync(cacheCommands, servletRequest);
    }

    public void glutenCacheAsync(List<String> cacheCommands) {
        try (GlutenCacheRequestLimits ignored = new GlutenCacheRequestLimits();
                SetLogCategory ignore = new SetLogCategory(LogConstant.BUILD_CATEGORY)) {
            val sparkSession = SparderEnv.getSparkSession();
            for (String cacheCommand : cacheCommands) {
                glutenCacheExecute(cacheCommand, sparkSession);
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void glutenCacheExecute(String cacheCommand, SparkSession sparkSession) {
        try {
            sparkSession.sql(cacheCommand).collectAsList();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

}
