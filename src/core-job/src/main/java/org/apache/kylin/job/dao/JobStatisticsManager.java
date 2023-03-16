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

package org.apache.kylin.job.dao;

import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;

import org.apache.kylin.guava30.shaded.common.collect.Maps;

public class JobStatisticsManager {

    public static JobStatisticsManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, JobStatisticsManager.class);
    }

    // called by reflection
    static JobStatisticsManager newInstance(KylinConfig conf, String project) {
        try {
            String cls = JobStatisticsManager.class.getName();
            Class<? extends JobStatisticsManager> clz = ClassUtil.forName(cls, JobStatisticsManager.class);
            return clz.getConstructor(KylinConfig.class, String.class).newInstance(conf, project);
        } catch (Exception e) {
            throw new RuntimeException("Failed to init DataModelManager from " + conf, e);
        }
    }

    // ============================================================================

    private KylinConfig config;
    private String project;

    private CachedCrudAssist<JobStatistics> crud;

    public JobStatisticsManager(KylinConfig config, String project) {
        init(config, project);
    }

    protected void init(KylinConfig cfg, final String project) {
        this.config = cfg;
        this.project = project;
        final ResourceStore store = ResourceStore.getKylinMetaStore(this.config);
        String resourceRootPath = "/" + this.project + ResourceStore.JOB_STATISTICS;
        this.crud = new CachedCrudAssist<JobStatistics>(store, resourceRootPath, JobStatistics.class) {
            @Override
            protected JobStatistics initEntityAfterReload(JobStatistics jobStatistics, String resourceName) {
                return jobStatistics;
            }
        };
    }

    public List<JobStatistics> getAll() {
        return crud.listAll();
    }

    public JobStatistics updateStatistics(long date, String model, long duration, long byteSize, int deltaCount) {
        JobStatistics jobStatistics = crud.get(String.valueOf(date));
        JobStatistics jobStatisticsToUpdate;
        if (jobStatistics == null) {
            jobStatisticsToUpdate = new JobStatistics(date, model, duration, byteSize);
            return crud.save(jobStatisticsToUpdate);
        }

        jobStatisticsToUpdate = crud.copyForWrite(jobStatistics);
        jobStatisticsToUpdate.update(model, duration, byteSize, deltaCount);
        return crud.save(jobStatisticsToUpdate);
    }

    public JobStatistics updateStatistics(long date, long duration, long byteSize, int deltaCount) {
        JobStatistics jobStatistics = crud.get(String.valueOf(date));
        JobStatistics jobStatisticsToUpdate;
        if (jobStatistics == null) {
            jobStatisticsToUpdate = new JobStatistics(date, duration, byteSize);
            return crud.save(jobStatisticsToUpdate);
        }

        jobStatisticsToUpdate = crud.copyForWrite(jobStatistics);
        jobStatisticsToUpdate.update(duration, byteSize, deltaCount);
        return crud.save(jobStatisticsToUpdate);
    }

    public Pair<Integer, JobStatistics> getOverallJobStats(final long startTime, final long endTime) {
        // filter
        List<JobStatistics> filteredJobStats = getFilteredJobStatsByTime(crud.listAll(), startTime, endTime);
        // aggregate all job stats
        JobStatistics aggregatedStats = aggregateJobStats(filteredJobStats);

        return new Pair<>(aggregatedStats.getCount(), aggregatedStats);
    }

    public Map<String, Integer> getJobCountByTime(final long startTime, final long endTime,
            final String timeDimension) {
        Map<String, Integer> result = Maps.newHashMap();
        aggregateJobStatsByTime(startTime, endTime, timeDimension).forEach((key, value) -> {
            result.put(key, value.getCount());
        });
        return result;
    }

    public Map<String, Integer> getJobCountByModel(long startTime, long endTime) {
        Map<String, Integer> result = Maps.newHashMap();

        aggregateStatsByModel(startTime, endTime).forEach((modelName, value) -> {
            String modelAlias = getModelAlias(modelName);
            if (modelAlias == null)
                return;
            result.put(modelAlias, value.getCount());
        });

        return result;
    }

    public Map<String, Double> getDurationPerByteByTime(final long startTime, final long endTime,
            final String timeDimension) {
        Map<String, JobStatisticsBasic> aggregateResult = aggregateJobStatsByTime(startTime, endTime, timeDimension);
        return calculateDurationPerByte(aggregateResult);
    }

    public Map<String, Double> getDurationPerByteByModel(long startTime, long endTime) {
        Map<String, JobStatisticsBasic> transformedResult = Maps.newHashMap();

        aggregateStatsByModel(startTime, endTime).forEach((modelName, value) -> {
            String modelAlias = getModelAlias(modelName);
            if (modelAlias == null)
                return;
            transformedResult.put(modelAlias,
                    new JobStatisticsBasic(value.getTotalDuration(), value.getTotalByteSize()));
        });

        return calculateDurationPerByte(transformedResult);
    }

    private String getModelAlias(String modelId) {
        NDataModelManager dataModelManager = NDataModelManager.getInstance(config, project);
        NDataModel model = dataModelManager.getDataModelDesc(modelId);
        if (model == null)
            return null;

        return model.getAlias();
    }

    private JobStatistics aggregateJobStats(List<JobStatistics> jobStatisticsToAggregate) {
        return jobStatisticsToAggregate.stream()
                .reduce((x, y) -> new JobStatistics(x.getCount() + y.getCount(),
                        x.getTotalDuration() + y.getTotalDuration(), x.getTotalByteSize() + y.getTotalByteSize()))
                .orElse(new JobStatistics());
    }

    // key is the date, value is the aggregated job stats
    private Map<String, JobStatisticsBasic> aggregateJobStatsByTime(final long startTime, final long endTime,
            final String timeDimension) {
        Map<String, JobStatisticsBasic> result = Maps.newHashMap();

        List<JobStatistics> qulifiedJobStats = getFilteredJobStatsByTime(crud.listAll(), startTime, endTime);

        long startDate = startTime;
        while (startDate <= endTime) {
            long nextDate = nextDate(startDate, timeDimension);
            List<JobStatistics> list = getFilteredJobStatsByTime(qulifiedJobStats, startDate, nextDate);
            result.put(formatDateTime(startDate), aggregateJobStats(list));
            startDate = nextDate;
        }

        return result;
    }

    // format epoch time to date string
    private String formatDateTime(long time) {
        ZoneId zoneId = TimeZone.getDefault().toZoneId();
        LocalDateTime localDateTime = Instant.ofEpochMilli(time).atZone(zoneId).toLocalDateTime();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd",
                Locale.getDefault(Locale.Category.FORMAT));
        return localDateTime.format(formatter);
    }

    private long nextDate(final long date, final String timeDimension) {
        ZoneId zoneId = TimeZone.getTimeZone(config.getTimeZone()).toZoneId();
        LocalDate localDate = Instant.ofEpochMilli(date).atZone(zoneId).toLocalDate();
        switch (timeDimension) {
        case "day":
            localDate = localDate.plusDays(1);
            break;
        case "week":
            localDate = localDate.with(TemporalAdjusters.next(DayOfWeek.MONDAY));
            break;
        case "month":
            localDate = localDate.with(TemporalAdjusters.firstDayOfNextMonth());
            break;
        default:
            localDate = localDate.plusDays(1);
            break;
        }

        return localDate.atStartOfDay(zoneId).toInstant().toEpochMilli();
    }

    private Map<String, JobStatisticsBasic> aggregateStatsByModel(long startTime, long endTime) {
        return getFilteredJobStatsByTime(crud.listAll(), startTime, endTime).stream()
                .map(JobStatistics::getJobStatisticsByModels).reduce((x, y) -> {
                    // merge two maps
                    Map<String, JobStatisticsBasic> mergedMap = Maps.newHashMap(x);
                    y.forEach((k, v) -> mergedMap.merge(k, v,
                            (value1, value2) -> new JobStatisticsBasic(value1.getCount() + value2.getCount(),
                                    value1.getTotalDuration() + value2.getTotalDuration(),
                                    value1.getTotalByteSize() + value2.getTotalByteSize())));
                    return mergedMap;
                }).orElse(Maps.newHashMap());
    }

    private List<JobStatistics> getFilteredJobStatsByTime(final List<JobStatistics> list, final long startTime,
            final long endTime) {
        return list.stream()
                .filter(singleStats -> singleStats.getDate() >= startTime && singleStats.getDate() < endTime)
                .collect(Collectors.toList());
    }

    private Map<String, Double> calculateDurationPerByte(Map<String, JobStatisticsBasic> totalMetricMap) {
        Map<String, Double> result = Maps.newHashMap();
        for (Map.Entry<String, JobStatisticsBasic> entry : totalMetricMap.entrySet()) {
            double totalDuration = entry.getValue().getTotalDuration();
            double totalByteSize = entry.getValue().getTotalByteSize();
            if (totalByteSize == 0)
                result.put(entry.getKey(), .0);
            else {
                result.put(entry.getKey(), totalDuration / totalByteSize);
            }
        }
        return result;
    }
}
