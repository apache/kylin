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

package org.apache.kylin.rec.query;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfig.SetAndUnsetThreadLocalConfig;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.guava30.shaded.common.cache.Cache;
import org.apache.kylin.guava30.shaded.common.cache.CacheBuilder;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.project.NProjectLoader;
import org.apache.kylin.query.relnode.ContextUtil;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.rec.query.mockup.AbstractQueryExecutor;
import org.apache.kylin.rec.query.mockup.MockupQueryExecutor;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractQueryRunner implements Closeable {

    private final String[] sqls;
    protected KylinConfig kylinConfig;
    protected final String project;

    private final Cache<String, QueryRecord> queryCache = CacheBuilder.newBuilder().maximumSize(20).build();
    @Getter
    private final Map<String, SQLResult> queryResults = new ConcurrentSkipListMap<>();
    @Getter
    private final Map<String, Collection<OlapContext>> olapContexts = Maps.newLinkedHashMap();

    private static final ExecutorService SUGGESTION_EXECUTOR_POOL = Executors.newFixedThreadPool(
            KylinConfig.getInstanceFromEnv().getProposingThreadNum(), new NamedThreadFactory("SuggestRunner"));

    AbstractQueryRunner(String project, String[] sqls) {
        this.project = Objects.requireNonNull(project);
        this.sqls = Objects.requireNonNull(sqls);
    }

    private void submitQueryExecute(final CountDownLatch counter, final AbstractQueryExecutor executor,
            final KylinConfig kylinConfig, final String project, final String sql) {

        SUGGESTION_EXECUTOR_POOL.execute(() -> {
            try {
                if (!isQueryCached(sql)) {
                    try (SetAndUnsetThreadLocalConfig autoUnset = KylinConfig
                            .setAndUnsetThreadLocalConfig(kylinConfig)) {
                        NTableMetadataManager.getInstance(autoUnset.get(), project);
                        NDataModelManager.getInstance(autoUnset.get(), project);
                        NDataflowManager.getInstance(autoUnset.get(), project);
                        NIndexPlanManager.getInstance(autoUnset.get(), project);
                        NProjectLoader.updateCache(project);
                        QueryRecord record = executor.execute(project, autoUnset.get(), sql);
                        queryCache.put(sql, record);
                    }
                }
                recordExecuteResult(sql);
            } finally {
                NProjectLoader.removeCache();
                ContextUtil.clearThreadLocalContexts();
                counter.countDown();
            }
        });
    }

    private void recordExecuteResult(String sql) {
        QueryRecord record = queryCache.getIfPresent(sql);
        if (record == null) {
            log.error("The analysis result missing for sql: {}", sql);
        } else {
            queryResults.put(sql, record.getSqlResult());
            olapContexts.get(sql).addAll(record.getOlapContexts());
        }
    }

    private boolean isQueryCached(String sql) {
        QueryRecord record = queryCache.getIfPresent(sql);
        return record != null && record.getSqlResult() != null
                && record.getSqlResult().getStatus() == SQLResult.Status.SUCCESS;
    }

    public void execute() throws IOException, InterruptedException {
        log.info("Mock query to generate OlapContexts applied to auto-modeling.");
        KylinConfig config = prepareConfig();
        try {
            AbstractQueryExecutor queryExecutor = new MockupQueryExecutor();
            List<String> distinctSqls = Arrays.stream(sqls).distinct().collect(Collectors.toList());
            CountDownLatch latch = new CountDownLatch(distinctSqls.size());
            distinctSqls.forEach(sql -> olapContexts.put(sql, Lists.newArrayList()));
            distinctSqls.forEach(sql -> submitQueryExecute(latch, queryExecutor, config, project, sql));
            latch.await();
        } finally {
            cleanupConfig(config);
        }
    }

    public abstract KylinConfig prepareConfig() throws IOException;

    public abstract void cleanupConfig(KylinConfig config) throws IOException;

    public Map<String, List<OlapContext>> filterModelViewOlapContexts() {
        List<OlapContext> modeViewOlapContextList = Lists.newArrayList();
        olapContexts.forEach((sql, olapContextList) -> {
            List<OlapContext> modelViewOlapContexts = olapContextList.stream()
                    .filter(e -> StringUtils.isNotEmpty(e.getBoundedModelAlias())).collect(Collectors.toList());
            if (CollectionUtils.isEmpty(modelViewOlapContexts)) {
                return;
            }
            modelViewOlapContexts.forEach(e -> e.setSql(sql));
            modeViewOlapContextList.addAll(modelViewOlapContexts);
        });
        return modeViewOlapContextList.stream().collect(Collectors.groupingBy(OlapContext::getBoundedModelAlias));
    }

    public Map<String, Collection<OlapContext>> filterNonModelViewOlapContexts() {
        return olapContexts.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, v -> v.getValue().stream()
                        .filter(e -> StringUtils.isEmpty(e.getBoundedModelAlias())).collect(Collectors.toList()),
                        (k1, k2) -> k1, LinkedHashMap::new));
    }

    @Override
    public void close() {
        queryCache.invalidateAll();
        queryResults.clear();
        olapContexts.clear();
    }
}
