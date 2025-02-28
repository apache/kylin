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

import static org.apache.kylin.common.QueryContext.PUSHDOWN_OBJECT_STORAGE;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_NOT_EXIST;
import static org.apache.kylin.metadata.query.QueryHistory.EngineType.CONSTANTS;
import static org.apache.kylin.metadata.query.QueryHistory.EngineType.HIVE;
import static org.apache.kylin.metadata.query.QueryHistory.EngineType.RDBMS;
import static org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO.fillZeroForQueryStatistics;

import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.NativeQueryRealization;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.favorite.QueryHistoryIdOffset;
import org.apache.kylin.metadata.favorite.QueryHistoryIdOffsetManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.query.QueryHistory;
import org.apache.kylin.metadata.query.QueryHistoryDAO;
import org.apache.kylin.metadata.query.QueryHistoryInfo;
import org.apache.kylin.metadata.query.QueryHistoryRequest;
import org.apache.kylin.metadata.query.QueryStatistics;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;
import org.apache.kylin.rest.exception.ForbiddenException;
import org.apache.kylin.rest.response.NDataModelResponse;
import org.apache.kylin.rest.response.QueryHistoryFiltersResponse;
import org.apache.kylin.rest.response.QueryStatisticsResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import lombok.val;

@Component("queryHistoryService")
public class QueryHistoryService extends BasicService implements AsyncTaskQueryHistorySupporter {
    public static final String WEEK = "week";
    //    public static final String DELETED_MODEL = "Deleted Model";
    //    public static final byte[] CSV_UTF8_BOM = new byte[]{(byte)0xEF, (byte)0xBB, (byte)0xBF};
    public static final String DAY = "day";
    public static final String MODEL = "model";
    public static final String COUNT = "count";
    public static final String MEAN_DURATION = "meanDuration";
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
            return ImmutableMap.of("total_scan_count", 0L, "source_result_count", 0L, "total_scan_bytes", 0L);
        }

        QueryHistoryDAO queryHistoryDAO = getQueryHistoryDao();
        List<QueryHistory> queryHistories = queryHistoryDAO.getQueryHistoriesByConditions(request, 1, 0);

        if (queryHistories.isEmpty()) {
            return ImmutableMap.of("total_scan_count", 0L, "source_result_count", 0L, "total_scan_bytes", 0L);
        }

        return ImmutableMap.of("total_scan_count", queryHistories.get(0).getTotalScanCount(), "source_result_count",
                queryHistories.get(0).getQueryHistoryInfo().getSourceResultCount(), "total_scan_bytes",
                queryHistories.get(0).getTotalScanBytes());
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

        val config = KylinConfig.getInstanceFromEnv();
        val indexPlanManager = NIndexPlanManager.getInstance(config, project);
        val modelManager = NDataModelManager.getInstance(config, project);

        List<NativeQueryRealization> realizations = query.transformRealizations(project);

        realizations.forEach(realization -> {
            String modelId = realization.getModelId();
            NDataModel nDataModel = modelManager.getDataModelDesc(modelId);
            if (noBrokenModels.containsValue(modelId)) {
                NDataModelResponse model = (NDataModelResponse) modelService
                        .updateResponseAcl(new NDataModelResponse(nDataModel), project);
                realization.setModelAlias(model.getFusionModelAlias());
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

        if (realizations != null && realizations.contains("modelName")
                && !CollectionUtils.isEmpty(request.getExcludeRealization())) {
            List<String> excludeModelNames = Lists.newArrayList(request.getExcludeRealization());
            request.setExcludeFilterModelIds(excludeModelNames.stream().filter(modelAliasMap::containsKey)
                    .map(modelAliasMap::get).collect(Collectors.toList()));
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

    public QueryHistoryFiltersResponse getQueryHistoryModels(QueryHistoryRequest request, int size) {
        val dataFlowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), request.getProject());
        List<NDataflow> models = dataFlowManager.listAllDataflows();
        List<String> modelList = models.stream()
                .sorted(Comparator.comparing(NDataflow::getQueryHitCount, Comparator.reverseOrder()))
                .map(NDataflow::getModel).map(NDataModel::getAlias).filter(filterByName(request.getFilterModelName()))
                .collect(Collectors.toList());

        List<String> engineList = Stream.of(HIVE.name(), RDBMS.name(), CONSTANTS.name(), PUSHDOWN_OBJECT_STORAGE)
                .filter(filterByName(request.getFilterModelName())).collect(Collectors.toList());

        Integer count = engineList.size() + modelList.size();
        return new QueryHistoryFiltersResponse(count, models.size(), engineList,
                modelList.stream().limit(size).collect(Collectors.toList()));
    }

    private Predicate<String> filterByName(String name) {
        return alias -> !StringUtils.isEmpty(alias) && (StringUtils.isEmpty(name)
                || alias.toLowerCase(Locale.ROOT).contains(name.toLowerCase(Locale.ROOT)));
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

    public QueryStatisticsResponse getQueryStatisticsByRealization(String project, long startTime, long endTime) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        aclEvaluate.checkProjectReadPermission(project);

        QueryHistoryDAO queryHistoryDao = getQueryHistoryDao();
        QueryStatistics queryStatistics = queryHistoryDao.getQueryCountAndAvgDurationRealization(startTime, endTime,
                project);
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
        QueryHistoryIdOffset queryHistoryIdOffset = QueryHistoryIdOffsetManager.getInstance(project)
                .get(QueryHistoryIdOffset.OffsetType.ACCELERATE);
        long idOffset = queryHistoryIdOffset.getOffset();
        QueryHistoryDAO queryHistoryDao = getQueryHistoryDao();
        return queryHistoryDao.getQueryHistoryCountBeyondOffset(idOffset, project);
    }

    public Map<String, Object> getQueryCount(String project, long startTime, long endTime, String dimension) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        aclEvaluate.checkProjectReadPermission(project);
        QueryHistoryDAO queryHistoryDAO = getQueryHistoryDao();
        List<QueryStatistics> queryStatistics;

        if (dimension.equals(MODEL)) {
            queryStatistics = queryHistoryDAO.getQueryCountByModel(startTime, endTime, project);
            return transformQueryStatisticsByModel(project, queryStatistics, COUNT);
        }

        queryStatistics = queryHistoryDAO.getQueryCountByTime(startTime, endTime, dimension, project);
        fillZeroForQueryStatistics(queryStatistics, startTime, endTime, dimension);
        return transformQueryStatisticsByTime(queryStatistics, COUNT, dimension);
    }

    public Map<String, Object> getAvgDuration(String project, long startTime, long endTime, String dimension) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        aclEvaluate.checkProjectReadPermission(project);
        QueryHistoryDAO queryHistoryDAO = getQueryHistoryDao();
        List<QueryStatistics> queryStatistics;

        if (dimension.equals(MODEL)) {
            queryStatistics = queryHistoryDAO.getAvgDurationByModel(startTime, endTime, project);
            return transformQueryStatisticsByModel(project, queryStatistics, MEAN_DURATION);
        }

        queryStatistics = queryHistoryDAO.getAvgDurationByTime(startTime, endTime, dimension, project);
        fillZeroForQueryStatistics(queryStatistics, startTime, endTime, dimension);
        return transformQueryStatisticsByTime(queryStatistics, MEAN_DURATION, dimension);
    }

    public Map<String, Object> getAvgDurationByRealization(String project, long startTime, long endTime,
            String dimension) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        aclEvaluate.checkProjectReadPermission(project);
        QueryHistoryDAO queryHistoryDAO = getQueryHistoryDao();
        List<QueryStatistics> queryStatistics;

        if (dimension.equals(MODEL)) {
            queryStatistics = queryHistoryDAO.getAvgDurationByModel(startTime, endTime, project);
            return transformQueryStatisticsByModel(project, queryStatistics, MEAN_DURATION);
        }

        queryStatistics = queryHistoryDAO.getAvgDurationRealizationByTime(startTime, endTime, dimension, project);
        fillZeroForQueryStatistics(queryStatistics, startTime, endTime, dimension);
        return transformQueryStatisticsByTime(queryStatistics, MEAN_DURATION, dimension);
    }

    public Map<String, Object> getQueryCountByRealization(String project, long startTime, long endTime,
            String dimension) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(project));
        aclEvaluate.checkProjectReadPermission(project);
        QueryHistoryDAO queryHistoryDAO = getQueryHistoryDao();
        List<QueryStatistics> queryStatistics;

        if (dimension.equals(MODEL)) {
            queryStatistics = queryHistoryDAO.getQueryCountByModel(startTime, endTime, project);
            return transformQueryStatisticsByModel(project, queryStatistics, COUNT);
        }

        queryStatistics = queryHistoryDAO.getQueryCountRealizationByTime(startTime, endTime, dimension, project);
        fillZeroForQueryStatistics(queryStatistics, startTime, endTime, dimension);
        return transformQueryStatisticsByTime(queryStatistics, COUNT, dimension);
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
