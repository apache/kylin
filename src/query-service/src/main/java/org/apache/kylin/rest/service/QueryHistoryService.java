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

import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_NOT_EXIST;
import static org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO.fillZeroForQueryStatistics;

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletResponse;

import io.kyligence.kap.guava20.shaded.common.collect.ImmutableMap;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.favorite.QueryHistoryIdOffset;
import org.apache.kylin.metadata.favorite.QueryHistoryIdOffsetManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.query.NativeQueryRealization;
import org.apache.kylin.metadata.query.QueryHistory;
import org.apache.kylin.metadata.query.QueryHistoryDAO;
import org.apache.kylin.metadata.query.QueryHistoryInfo;
import org.apache.kylin.metadata.query.QueryHistoryRequest;
import org.apache.kylin.metadata.query.QueryStatistics;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;
import org.apache.kylin.rest.response.NDataModelResponse;
import org.apache.kylin.rest.response.QueryStatisticsResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.val;

@Component("queryHistoryService")
public class QueryHistoryService extends BasicService implements AsyncTaskQueryHistorySupporter {
    public static final String WEEK = "week";
    //    public static final String DELETED_MODEL = "Deleted Model";
    //    public static final byte[] CSV_UTF8_BOM = new byte[]{(byte)0xEF, (byte)0xBB, (byte)0xBF};
    public static final String DAY = "day";
    private static final Logger logger = LoggerFactory.getLogger("query");
    @Autowired
    private AclEvaluate aclEvaluate;
    @Autowired
    @Qualifier("asyncTaskService")
    private AsyncTaskServiceSupporter asyncTaskService;
    @Autowired
    @Qualifier("modelService")
    private ModelService modelService;

    public QueryHistoryDAO getQueryHistoryDao() {
        return RDBMSQueryHistoryDAO.getInstance();
    }

    public void downloadQueryHistories(QueryHistoryRequest request, HttpServletResponse response, ZoneOffset zoneOffset,
            Integer timeZoneOffsetHour, boolean onlySql) throws Exception {
        processRequestParams(request);
        if (haveSpaces(request.getSql())) {
            return;
        }
        splitModels(request);

        Future<Long> future = asyncTaskService.runDownloadQueryHistory(request, response, zoneOffset,
                timeZoneOffsetHour, getQueryHistoryDao(), onlySql);
        Long timeCost = future.get(KylinConfig.getInstanceFromEnv().getQueryHistoryDownloadTimeoutSeconds(),
                TimeUnit.SECONDS);
        logger.info("download query history cost {}s", timeCost);
    }

    public Map<String, Object> getQueryHistories(QueryHistoryRequest request, final int limit, final int page) {
        processRequestParams(request);

        HashMap<String, Object> data = new HashMap<>();
        List<QueryHistory> queryHistories = Lists.newArrayList();

        if (haveSpaces(request.getSql())) {
            data.put("query_histories", queryHistories);
            data.put("size", 0);
            return data;
        }

        splitModels(request);

        QueryHistoryDAO queryHistoryDAO = getQueryHistoryDao();
        queryHistories = queryHistoryDAO.getQueryHistoriesByConditions(request, limit, page);

        queryHistories.forEach(query -> {
            QueryHistoryInfo queryHistoryInfo = query.getQueryHistoryInfo();
            if ((queryHistoryInfo == null || queryHistoryInfo.getRealizationMetrics() == null
                    || queryHistoryInfo.getRealizationMetrics().isEmpty())
                    && StringUtils.isEmpty(query.getQueryRealizations())) {
                return;
            }
            query.setNativeQueryRealizations(parseQueryRealizationInfo(query, request.getProject()));
        });

        data.put("query_histories", queryHistories);
        data.put("size", queryHistoryDAO.getQueryHistoriesSize(request, request.getProject()));
        return data;
    }

    public Map<String, Long> queryTiredStorageMetric(QueryHistoryRequest request) {
        processRequestParams(request);

        if (haveSpaces(request.getSql())) {
            return ImmutableMap.of("total_scan_count", 0L);
        }

        QueryHistoryDAO queryHistoryDAO = getQueryHistoryDao();
        List<QueryHistory> queryHistories = queryHistoryDAO.getQueryHistoriesByConditions(request, 1, 0);

        if (queryHistories.isEmpty()) {
            return ImmutableMap.of("total_scan_count", 0L);
        }

        return ImmutableMap.of("total_scan_count", queryHistories.get(0).getTotalScanCount());
    }

    private void processRequestParams(QueryHistoryRequest request) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(request.getProject()));
        aclEvaluate.checkProjectReadPermission(request.getProject());

        request.setUsername(SecurityContextHolder.getContext().getAuthentication().getName());
        if (aclEvaluate.hasProjectAdminPermission(
                NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(request.getProject()))) {
            request.setAdmin(true);
        }

        if (request.getSql() == null) {
            request.setSql("");
        }

        if (request.getSql() != null) {
            request.setSql(request.getSql().trim());
        }
    }

    private List<NativeQueryRealization> parseQueryRealizationInfo(QueryHistory query, String project) {
        val noBrokenModels = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                .listUnderliningDataModels().stream()
                .collect(Collectors.toMap(NDataModel::getAlias, RootPersistentEntity::getUuid));

        val dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        List<NativeQueryRealization> realizations = query.transformRealizations();

        realizations.forEach(realization -> {
            NDataModel nDataModel = dataModelManager.getDataModelDesc(realization.getModelId());
            if (noBrokenModels.containsValue(realization.getModelId())) {
                NDataModelResponse model = (NDataModelResponse) modelService
                        .updateReponseAcl(new NDataModelResponse(nDataModel), project);
                realization.setModelAlias(model.getFusionModelAlias());
                realization.setAclParams(model.getAclParams());
                realization.setLayoutExist(
                        isLayoutExist(indexPlanManager, realization.getModelId(), realization.getLayoutId()));

            } else {
                val modelAlias = nDataModel == null ? DELETED_MODEL
                        : String.format(Locale.ROOT, "%s broken", nDataModel.getAlias());
                realization.setModelAlias(modelAlias);
                realization.setValid(false);
                realization.setLayoutExist(false);
            }
        });
        return realizations;
    }

    private boolean isLayoutExist(NIndexPlanManager indexPlanManager, String modelId, Long layoutId) {
        if (layoutId == null)
            return false;
        return indexPlanManager.getIndexPlan(modelId).getLayoutEntity(layoutId) != null;
    }

    private void splitModels(QueryHistoryRequest request) {
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), request.getProject());
        val modelAliasMap = dataflowManager.listUnderliningDataModels().stream()
                .collect(Collectors.toMap(NDataModel::getAlias, RootPersistentEntity::getUuid));
        List<String> realizations = request.getRealizations();
        if (realizations != null && !realizations.isEmpty() && !realizations.contains("modelName")) {
            List<String> modelNames = Lists.newArrayList(realizations);
            modelNames.remove(QueryHistory.EngineType.HIVE.name());
            modelNames.remove(QueryHistory.EngineType.CONSTANTS.name());
            modelNames.remove(QueryHistory.EngineType.RDBMS.name());

            request.setFilterModelIds(modelNames.stream().filter(modelAliasMap::containsKey).map(modelAliasMap::get)
                    .collect(Collectors.toList()));
        }
    }

    public List<String> getQueryHistoryUsernames(QueryHistoryRequest request, int size) {
        QueryHistoryDAO queryHistoryDAO = getQueryHistoryDao();
        request.setUsername(SecurityContextHolder.getContext().getAuthentication().getName());
        if (aclEvaluate.hasProjectAdminPermission(
                NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(request.getProject()))) {
            request.setAdmin(true);
        } else {
            throw new ForbiddenException(MsgPicker.getMsg().getExportResultNotAllowed());
        }
        List<QueryHistory> queryHistories = queryHistoryDAO.getQueryHistoriesSubmitters(request, size);
        return queryHistories.stream().map(QueryHistory::getQuerySubmitter).collect(Collectors.toList());
    }

    public List<String> getQueryHistoryModels(QueryHistoryRequest request, int size) {
        QueryHistoryDAO queryHistoryDAO = getQueryHistoryDao();
        request.setUsername(SecurityContextHolder.getContext().getAuthentication().getName());
        if (aclEvaluate.hasProjectAdminPermission(
                NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(request.getProject()))) {
            request.setAdmin(true);
        }
        List<QueryStatistics> queryStatistics = queryHistoryDAO.getQueryHistoriesModelIds(request, size);

        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), request.getProject());
        val modelAliasMap = dataflowManager.listUnderliningDataModels().stream()
                .collect(Collectors.toMap(RootPersistentEntity::getUuid, NDataModel::getAlias));

        return queryStatistics.stream().map(query -> {
            // engineType && modelId are both saved into queryStatistics
            if (!StringUtils.isEmpty(query.getEngineType())) {
                return query.getEngineType();
            } else if (!StringUtils.isEmpty(query.getModel()) && modelAliasMap.containsKey(query.getModel())) {
                return modelAliasMap.get(query.getModel());
            } else {
                return null;
            }
        }).filter(alias -> !StringUtils.isEmpty(alias) && (StringUtils.isEmpty(request.getFilterModelName())
                || alias.toLowerCase(Locale.ROOT).contains(request.getFilterModelName().toLowerCase(Locale.ROOT))))
                .limit(size).collect(Collectors.toList());
    }

    private boolean haveSpaces(String text) {
        if (text == null) {
            return false;
        }
        String regex = "[\r|\n|\\s]+";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(text);
        return matcher.find();
    }

    public QueryStatisticsResponse getQueryStatistics(String project, long startTime, long endTime) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        aclEvaluate.checkProjectReadPermission(project);
        QueryHistoryDAO queryHistoryDAO = getQueryHistoryDao();

        QueryStatistics queryStatistics = queryHistoryDAO.getQueryCountAndAvgDuration(startTime, endTime, project);
        return new QueryStatisticsResponse(queryStatistics.getCount(), queryStatistics.getMeanDuration());
    }

    public long getLastWeekQueryCount(String project) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        aclEvaluate.checkProjectReadPermission(project);
        QueryHistoryDAO queryHistoryDAO = getQueryHistoryDao();
        long endTime = TimeUtil.getDayStart(System.currentTimeMillis());
        long startTime = endTime - 7 * DateUtils.MILLIS_PER_DAY;
        QueryStatistics statistics = queryHistoryDAO.getQueryCountByRange(startTime, endTime, project);
        return statistics.getCount();
    }

    public long getQueryCountToAccelerate(String project) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        aclEvaluate.checkProjectReadPermission(project);
        QueryHistoryIdOffset queryHistoryIdOffset = QueryHistoryIdOffsetManager
                .getInstance(KylinConfig.getInstanceFromEnv(), project).get();
        long idOffset = queryHistoryIdOffset.getOffset();
        QueryHistoryDAO queryHistoryDao = getQueryHistoryDao();
        return queryHistoryDao.getQueryHistoryCountBeyondOffset(idOffset, project);
    }

    public Map<String, Object> getQueryCount(String project, long startTime, long endTime, String dimension) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        aclEvaluate.checkProjectReadPermission(project);
        QueryHistoryDAO queryHistoryDAO = getQueryHistoryDao();
        List<QueryStatistics> queryStatistics;

        if (dimension.equals("model")) {
            queryStatistics = queryHistoryDAO.getQueryCountByModel(startTime, endTime, project);
            return transformQueryStatisticsByModel(project, queryStatistics, "count");
        }

        queryStatistics = queryHistoryDAO.getQueryCountByTime(startTime, endTime, dimension, project);
        fillZeroForQueryStatistics(queryStatistics, startTime, endTime, dimension);
        return transformQueryStatisticsByTime(queryStatistics, "count", dimension);
    }

    public Map<String, Object> getAvgDuration(String project, long startTime, long endTime, String dimension) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        aclEvaluate.checkProjectReadPermission(project);
        QueryHistoryDAO queryHistoryDAO = getQueryHistoryDao();
        List<QueryStatistics> queryStatistics;

        if (dimension.equals("model")) {
            queryStatistics = queryHistoryDAO.getAvgDurationByModel(startTime, endTime, project);
            return transformQueryStatisticsByModel(project, queryStatistics, "meanDuration");
        }

        queryStatistics = queryHistoryDAO.getAvgDurationByTime(startTime, endTime, dimension, project);
        fillZeroForQueryStatistics(queryStatistics, startTime, endTime, dimension);
        return transformQueryStatisticsByTime(queryStatistics, "meanDuration", dimension);
    }

    private Map<String, Object> transformQueryStatisticsByModel(String project, List<QueryStatistics> statistics,
            String fieldName) {
        Map<String, Object> result = Maps.newHashMap();
        NDataModelManager modelManager = getManager(NDataModelManager.class, project);

        statistics.forEach(singleStatistics -> {
            NDataModel model = modelManager.getDataModelDesc(singleStatistics.getModel());
            if (model == null)
                return;
            result.put(model.getAlias(), getValueByField(singleStatistics, fieldName));
        });

        return result;
    }

    private Object getValueByField(QueryStatistics statistics, String fieldName) {
        Object object = null;
        try {
            Field field = statistics.getClass().getDeclaredField(fieldName);
            Unsafe.changeAccessibleObject(field, true);
            object = field.get(statistics);
        } catch (Exception e) {
            logger.error("Error caught when get value from query statistics {}", e.getMessage());
        }

        return object;
    }

    private Map<String, Object> transformQueryStatisticsByTime(List<QueryStatistics> statistics, String fieldName,
            String dimension) {
        Map<String, Object> result = Maps.newHashMap();

        statistics.forEach(singleStatistics -> {
            if (dimension.equals("month")) {
                TimeZone timeZone = TimeZone.getTimeZone(KylinConfig.getInstanceFromEnv().getTimeZone());
                LocalDate date = singleStatistics.getTime().atZone(timeZone.toZoneId()).toLocalDate();
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM",
                        Locale.getDefault(Locale.Category.FORMAT));
                result.put(date.withDayOfMonth(1).format(formatter), getValueByField(singleStatistics, fieldName));
                return;
            }
            long time = singleStatistics.getTime().toEpochMilli();
            Date date = new Date(time);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd", Locale.getDefault(Locale.Category.FORMAT));
            result.put(sdf.format(date), getValueByField(singleStatistics, fieldName));
        });

        return result;
    }

    public Map<String, String> getQueryHistoryTableMap(List<String> projects) {
        List<String> filterProjects = getManager(NProjectManager.class).listAllProjects().stream()
                .map(ProjectInstance::getName)
                .filter(s -> projects == null || projects.stream().map(str -> str.toLowerCase(Locale.ROOT))
                        .collect(Collectors.toList()).contains(s.toLowerCase(Locale.ROOT)))
                .collect(Collectors.toList());

        Map<String, String> result = Maps.newHashMap();
        for (String project : filterProjects) {
            aclEvaluate.checkProjectReadPermission(project);
            Preconditions.checkArgument(StringUtils.isNotEmpty(project));
            ProjectInstance projectInstance = getManager(NProjectManager.class).getProject(project);
            if (projectInstance == null) {
                throw new KylinException(PROJECT_NOT_EXIST, project);
            }
            result.put(project, getQueryHistoryDao().getQueryMetricMeasurement());
        }

        return result;
    }

}
