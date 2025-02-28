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

package org.apache.kylin.rec.util;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.collect.ArrayListMultimap;
import org.apache.kylin.guava30.shaded.common.collect.ListMultimap;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.query.QueryHistory;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
import org.apache.kylin.metadata.recommendation.entity.LayoutRecItemV2;
import org.apache.kylin.rec.AbstractContext;
import org.apache.kylin.rec.common.AccelerateInfo;
import org.apache.logging.log4j.status.StatusLogger;

import lombok.extern.slf4j.Slf4j;

/**
 * Collect query histories and the recommendations from query histories.
 */
@Slf4j
public class QueryRecStatsCollector {

    private static final String STORE_POSITION = "/query_rec/history/";
    private static final String TMP_STORE_POSITION = "/query_rec/tmp/";
    private static final String FILE_SUFFIX_SNAPPY = ".json.snappy";
    private static final String FILE_SUFFIX = ".json";

    public static QueryRecStatsCollector getInstance() {
        return Singletons.getInstance(QueryRecStatsCollector.class);
    }

    private static String getRandomFileSuffix() {
        return "_" + RandomUtil.randomUUIDStr();
    }

    public void doStatistics(ListMultimap<String, QueryHistory> sqlToQueryHistoryMap,
            Map<String, RawRecItem> uniqueIdToLayoutRecMap, AbstractContext semiContext) {
        ListMultimap<String, QueryRecItem> sqlToQueryRecItemMap = ArrayListMultimap.create();
        sqlToQueryHistoryMap.forEach((sql, queryHistory) -> {
            QueryRecItem queryRecItem = new QueryRecItem(queryHistory);
            sqlToQueryRecItemMap.put(sql, queryRecItem);
        });
        Map<String, RawRecItem> nonLayoutIdToRecMap = new HashMap<>();
        semiContext.getExistingNonLayoutRecItemMap().forEach((content, rec) -> {
            // It's a convention: recId < 0, id of model's column ≥ 0.
            String recId = "-" + rec.getId();
            nonLayoutIdToRecMap.put(recId, rec);
        });

        semiContext.getAccelerateInfoMap().forEach((sql, accelerateInfo) -> {
            for (AccelerateInfo.QueryLayoutRelation relation : accelerateInfo.getRelatedLayouts()) {
                List<QueryRecItem> queryRecItems = sqlToQueryRecItemMap.get(sql);
                if (accelerateInfo.isNotSucceed()) {
                    queryRecItems.forEach(queryRecItem -> {
                        queryRecItem.setAccelerated(false);
                        String failedReason = accelerateInfo.isFailed() //
                                ? accelerateInfo.getFailedCause().getMessage()
                                : accelerateInfo.getPendingMsg();
                        queryRecItem.setAccFailedReason(failedReason);
                    });
                    continue;
                }
                // If the uniqueIdToLayoutRecMap does not contain the uniqueId,
                // then the recommended layout is already existed on the model,
                // otherwise, the layout will definitely be converted into a RawRecItem.
                RawRecItem layoutRec = uniqueIdToLayoutRecMap.get(relation.getUniqueId());
                if (layoutRec == null) {
                    addLayoutInfo(queryRecItems, semiContext, relation);
                } else {
                    addLayoutRecInfo(queryRecItems, semiContext, relation, nonLayoutIdToRecMap, layoutRec);
                }
            }
        });
        saveAll(sqlToQueryRecItemMap.values());
    }

    private void addLayoutInfo(List<QueryRecItem> queryRecItems, AbstractContext semiContext,
            AccelerateInfo.QueryLayoutRelation layoutRelation) {
        IndexPlan indexPlan = semiContext.getOriginIndexPlan(layoutRelation.getModelId());
        if (indexPlan == null) {
            log.error("[UNLIKELY_THINGS_HAPPENED] Reusing an existing layout on the missing model({})",
                    layoutRelation.getModelId());
            return;
        }
        LayoutEntity layout = indexPlan.getLayoutEntity(layoutRelation.getLayoutId());
        for (QueryRecItem queryRecItem : queryRecItems) {
            queryRecItem.addLayout(layout, indexPlan.getModel());
        }
    }

    private void addLayoutRecInfo(List<QueryRecItem> queryRecItems, AbstractContext semiContext,
            AccelerateInfo.QueryLayoutRelation layoutRelation, Map<String, RawRecItem> nonLayoutRecItemIdMap,
            RawRecItem layoutRecItem) {
        NDataModel model = semiContext.getProposedModel(layoutRelation.getModelId());
        if (model == null) {
            log.error("[UNLIKELY_THINGS_HAPPENED] Reusing an existing layout on the missing model({})",
                    layoutRelation.getModelId());
            return;
        }
        LayoutEntity layout = ((LayoutRecItemV2) layoutRecItem.getRecEntity()).getLayout();
        for (QueryRecItem queryRecItem : queryRecItems) {
            queryRecItem.addLayoutRec(layout, model, nonLayoutRecItemIdMap);
        }
    }

    private void saveAll(Collection<QueryRecItem> queryRecItems) {
        if (CollectionUtils.isEmpty(queryRecItems)) {
            return;
        }
        List<QueryRecItem> sortedQueryRecItems = sortByQueryTime(queryRecItems);
        String day = sortedQueryRecItems.get(0).getDay();
        List<QueryRecItem> dailyHistory = new ArrayList<>();
        for (QueryRecItem queryRecItem : sortedQueryRecItems) {
            if (day.equals(queryRecItem.getDay())) {
                dailyHistory.add(queryRecItem);
            } else {
                saveDaily(dailyHistory);
                dailyHistory = new ArrayList<>();
                dailyHistory.add(queryRecItem);
                day = queryRecItem.getDay();
            }
        }
        saveDaily(dailyHistory);
    }

    private List<QueryRecItem> sortByQueryTime(Collection<QueryRecItem> queryHistoryForAnalyses) {
        return queryHistoryForAnalyses.stream().sorted((o1, o2) -> {
            if (o1.getQueryTime() - o2.getQueryTime() == 0) {
                return 0;
            }
            return (o1.getQueryTime() - o2.getQueryTime()) > 0 ? 1 : -1;
        }).collect(Collectors.toList());
    }

    private void saveDaily(Collection<QueryRecItem> queryHistoryForAnalyses) {
        if (CollectionUtils.isEmpty(queryHistoryForAnalyses)) {
            return;
        }
        String project = queryHistoryForAnalyses.iterator().next().getProject();
        String day = queryHistoryForAnalyses.iterator().next().getDay();
        String tmpPath = project + TMP_STORE_POSITION + "query_history_" + System.currentTimeMillis()
                + getRandomFileSuffix();
        String destPath = project + STORE_POSITION + day + "/query_history_" + System.currentTimeMillis()
                + getRandomFileSuffix();
        try {
            OutputStreamWriter writer = QueryHistoryFStreamFactory.getInstance().getOutputStreamWriter(tmpPath, true);
            for (QueryRecItem queryRecItem : queryHistoryForAnalyses) {
                String queryHistoryStr = JsonUtil.writeValueAsString(queryRecItem).replaceAll("[\n\r]", " ");
                writer.append(queryHistoryStr);
                writer.append(System.lineSeparator());
            }
            IOUtils.closeStream(writer);
            QueryHistoryFStreamFactory.getInstance().rename(tmpPath, destPath, true);
        } catch (IOException e) {
            log.error("export daily history fail due to ", e);
        } finally {
            QueryHistoryFStreamFactory.getInstance().deleteFileIfExists(tmpPath, true);
        }
    }

    public static class QueryHistoryFStreamFactory {
        private final FileSystem fileSystem;
        private final String workingDir;
        private CompressionCodec codec = null;

        private QueryHistoryFStreamFactory() {
            try {
                if (KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory().endsWith("/")) {
                    workingDir = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory();
                } else {
                    workingDir = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory() + "/";
                }
                fileSystem = FileSystem.newInstance(new Path(workingDir).toUri(), new Configuration());
                initSnappyCodec();
            } catch (IOException e) {
                StatusLogger.getLogger().error("Failed to create the file system, ", e);
                throw new KylinRuntimeException("Failed to create the file system, ", e);
            }
        }

        public static QueryHistoryFStreamFactory getInstance() {
            return Singletons.getInstance(QueryHistoryFStreamFactory.class);
        }

        private void initSnappyCodec() {
            try {
                NativeCodeLoader.buildSupportsSnappy();
                this.codec = ReflectionUtils.newInstance(SnappyCodec.class, new Configuration());
            } catch (UnsatisfiedLinkError e) {
                log.warn("snappy not support under this environment, disable snappy codec");
            }
        }

        public OutputStreamWriter getOutputStreamWriter(String path, boolean usingSnappy) throws IOException {
            OutputStream outputStream = getOutputStream(path, usingSnappy);
            return new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
        }

        private OutputStream getOutputStream(String path, boolean usingSnappy) throws IOException {
            Path fsPath = getFullPath(path, usingSnappy);
            FSDataOutputStream outputStream = fileSystem.exists(fsPath) ? fileSystem.append(fsPath)
                    : fileSystem.create(fsPath, true);
            if (usingSnappy && codec != null) {
                return codec.createOutputStream(outputStream);
            } else {
                return outputStream;
            }
        }

        public void rename(String path, String destPath, boolean usingSnappy) throws IOException {
            Path source = getFullPath(path, usingSnappy);
            Path target = getFullPath(destPath, usingSnappy);
            if (fileSystem.exists(source)) {
                if (!fileSystem.exists(target.getParent())) {
                    fileSystem.mkdirs(target.getParent());
                }
                fileSystem.rename(source, target);
            }
        }

        public void deleteFileIfExists(String path, boolean usingSnappy) {
            Path fsPath = getFullPath(path, usingSnappy);
            try {
                if (fileSystem.exists(fsPath)) {
                    fileSystem.delete(fsPath, true);
                }
            } catch (IOException e) {
                // ignore
            }
        }

        public Path getFullPath(String path, boolean usingSnappy) {
            return new Path(workingDir + path + (usingSnappy && codec != null ? FILE_SUFFIX_SNAPPY : FILE_SUFFIX));
        }
    }

}
