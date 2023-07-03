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

import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_DOWNLOAD_FILE;
import static org.apache.kylin.rest.service.AsyncTaskQueryHistorySupporter.CSV_UTF8_BOM;
import static org.apache.kylin.rest.service.AsyncTaskQueryHistorySupporter.DELETED_MODEL;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.metrics.MetricsCategory;
import org.apache.kylin.common.metrics.MetricsGroup;
import org.apache.kylin.common.metrics.MetricsName;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.query.NativeQueryRealization;
import org.apache.kylin.metadata.query.QueryHistory;
import org.apache.kylin.metadata.query.QueryHistoryDAO;
import org.apache.kylin.metadata.query.QueryHistoryInfo;
import org.apache.kylin.metadata.query.QueryHistoryRequest;
import org.apache.kylin.metadata.query.QueryHistorySql;
import org.apache.kylin.metadata.query.util.QueryHistoryUtil;
import org.apache.kylin.tool.garbage.CleanTaskExecutorService;
import org.apache.kylin.tool.garbage.StorageCleaner;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class AsyncTaskService implements AsyncTaskServiceSupporter {

    private static final String GLOBAL = "global";

    @Async
    public void cleanupStorage() throws Exception {

        long startAt = System.currentTimeMillis();
        try {
            CleanTaskExecutorService.getInstance()
                .submit(
                    new StorageCleaner().withTag(StorageCleaner.CleanerTag.SERVICE),
                    KylinConfig.getInstanceFromEnv().getStorageCleanTaskTimeout(), TimeUnit.MILLISECONDS)
                .get();
        } catch (Exception e) {
            MetricsGroup.hostTagCounterInc(MetricsName.STORAGE_CLEAN_FAILED, MetricsCategory.GLOBAL, GLOBAL);
            throw e;
        } finally {
            MetricsGroup.hostTagCounterInc(MetricsName.STORAGE_CLEAN, MetricsCategory.GLOBAL, GLOBAL);
            MetricsGroup.hostTagCounterInc(MetricsName.STORAGE_CLEAN_DURATION, MetricsCategory.GLOBAL, GLOBAL,
                    System.currentTimeMillis() - startAt);
        }
    }

    @Override
    @Async
    public Future<Long> runDownloadQueryHistory(QueryHistoryRequest request, HttpServletResponse response,
            ZoneOffset zoneOffset, Integer timeZoneOffsetHour, QueryHistoryDAO queryHistoryDao, boolean onlySql) {
        long start = System.currentTimeMillis();
        try (ServletOutputStream ops = response.getOutputStream()) {
            if (!onlySql) {
                ops.write(CSV_UTF8_BOM);
                ops.write(MsgPicker.getMsg().getQueryHistoryColumnMeta().getBytes(StandardCharsets.UTF_8));
            }
            batchDownload(request, zoneOffset, timeZoneOffsetHour, queryHistoryDao, onlySql, ops);
        } catch (IOException e) {
            throw new KylinException(FAILED_DOWNLOAD_FILE, e.getMessage());
        }
        return new AsyncResult<>((System.currentTimeMillis() - start) / 1000);
    }

    private void batchDownload(QueryHistoryRequest request, ZoneOffset zoneOffset, Integer timeZoneOffsetHour,
            QueryHistoryDAO queryHistoryDao, boolean onlySql, ServletOutputStream outputStream) throws IOException {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        int needDownload = Math.min((int) queryHistoryDao.getQueryHistoriesSize(request, request.getProject()),
                kylinConfig.getQueryHistoryDownloadMaxSize());
        int hadDownload = 0;
        while (hadDownload < needDownload) {
            int batchSize = Math.min(kylinConfig.getQueryHistoryDownloadBatchSize(), needDownload - hadDownload);
            List<QueryHistory> queryHistories = queryHistoryDao.getQueryHistoriesByConditionsWithOffset(request, batchSize, hadDownload);
            for (QueryHistory queryHistory : queryHistories) {
                fillingModelAlias(kylinConfig, request.getProject(), queryHistory);
                if (onlySql) {
                    QueryHistorySql queryHistorySql = queryHistory.getQueryHistorySql();
                    String sql = queryHistorySql.getNormalizedSql();
                    outputStream.write((sql.replaceAll("\n|\r", " ") + ";\n").getBytes(StandardCharsets.UTF_8));
                } else {
                    outputStream.write((QueryHistoryUtil.getDownloadData(queryHistory, zoneOffset, timeZoneOffsetHour) + "\n").getBytes(StandardCharsets.UTF_8));
                }
            }
            hadDownload = hadDownload + queryHistories.size();
        }
    }

    private void fillingModelAlias(KylinConfig kylinConfig, String project, QueryHistory qh) {
        if (isQueryHistoryInfoEmpty(qh)) {
            return;
        }
        val noBrokenModels = NDataflowManager.getInstance(kylinConfig, project).listUnderliningDataModels().stream()
                .collect(Collectors.toMap(NDataModel::getAlias, RootPersistentEntity::getUuid));
        val dataModelManager = NDataModelManager.getInstance(kylinConfig, project);
        List<NativeQueryRealization> realizations = qh.transformRealizations(project);

        realizations.forEach(realization -> {
            NDataModel nDataModel = dataModelManager.getDataModelDesc(realization.getModelId());
            if (noBrokenModels.containsValue(realization.getModelId())) {
                realization.setModelAlias(nDataModel.getFusionModelAlias());
            } else {
                val modelAlias = nDataModel == null ? DELETED_MODEL
                        : String.format(Locale.ROOT, "%s broken", nDataModel.getAlias());
                realization.setModelAlias(modelAlias);
            }
        });
        qh.setNativeQueryRealizations(realizations);
    }

    private boolean isQueryHistoryInfoEmpty(QueryHistory queryHistory) {
        QueryHistoryInfo qhInfo = queryHistory.getQueryHistoryInfo();
        return (qhInfo == null || qhInfo.getRealizationMetrics() == null || qhInfo.getRealizationMetrics().isEmpty())
                && StringUtils.isEmpty(queryHistory.getQueryRealizations());
    }
}
