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

package org.apache.kylin.rest.scheduler;

import static org.apache.kylin.common.constant.Constants.MARK;
import static org.apache.kylin.common.constant.Constants.VIEW_MAPPING;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpStatus;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.constant.LogConstant;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.logging.SetLogCategory;
import org.apache.kylin.common.response.RestResponse;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.job.snapshot.SnapshotJobUtils;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.rest.handler.KapNoOpResponseErrorHandler;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.type.TypeReference;

import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AutoRefreshSnapshotRunner implements Runnable {
    private static final String SNAPSHOT_VIEW_MAPPING_ERROR_MESSAGE = "Project[%s] Save View Mapping Failed";

    private static final Map<String, AutoRefreshSnapshotRunner> INSTANCE_MAP = Maps.newConcurrentMap();
    @Setter
    @Getter
    private ExecutorService jobPool;
    @Setter
    @Getter
    private KylinConfig projectConfig;
    @Getter
    private Queue<CheckSourceTableResult> checkSourceTableQueue = new LinkedBlockingQueue<>();
    @Getter
    private Map<Future<String>, Long> checkSourceTableFutures = Maps.newConcurrentMap();
    @Getter
    private final String project;
    private RestTemplate restTemplate;
    @Getter
    private Map<String, List<TableDesc>> sourceTableSnapshotMapping = Maps.newHashMap();
    @Getter
    private Map<String, AtomicInteger> buildSnapshotCount = Maps.newConcurrentMap();

    public static synchronized AutoRefreshSnapshotRunner getInstanceByProject(String project) {
        return INSTANCE_MAP.get(project);
    }

    public static synchronized AutoRefreshSnapshotRunner getInstance(String project) {
        return INSTANCE_MAP.computeIfAbsent(project, key -> {
            val projectConfig = NProjectManager.getInstance(KylinConfig.readSystemKylinConfig()).getProject(project)
                    .getConfig();
            return new AutoRefreshSnapshotRunner(projectConfig, project);
        });
    }

    private AutoRefreshSnapshotRunner(KylinConfig projectConfig, String project) {
        Preconditions.checkNotNull(project);
        if (INSTANCE_MAP.containsKey(project)) {
            throw new IllegalStateException(
                    "DefaultScheduler for project " + project + " has been initiated. Use getInstance() instead.");
        }

        this.project = project;
        init(projectConfig, project);
        log.debug("New AutoRefreshSnapshotRunner created by project '{}': {}", project,
                System.identityHashCode(AutoRefreshSnapshotRunner.this));
    }

    public void init(KylinConfig projectConfig, String project) {
        int corePoolSize = projectConfig.getSnapshotAutoRefreshMaxConcurrentJobLimit();
        this.jobPool = new ThreadPoolExecutor(corePoolSize, corePoolSize, Long.MAX_VALUE, TimeUnit.DAYS,
                new LinkedBlockingQueue<>(),
                new NamedThreadFactory("AutoRefreshSnapshotWorker(project:" + project + ")"));
        this.projectConfig = projectConfig;
        this.restTemplate = new RestTemplate();
        this.restTemplate.setErrorHandler(new KapNoOpResponseErrorHandler());
        log.info("AutoRefreshSnapshotRunner init project[{}] job pool size: {}", project, corePoolSize);
    }

    public void doRun() {
        try (SetLogCategory ignored = new SetLogCategory(LogConstant.SCHEDULE_CATEGORY)) {
            log.info("Project[{}] start check and refresh snapshot", project);
            if (log.isDebugEnabled()) {
                val poolExecutor = (ThreadPoolExecutor) jobPool;
                log.debug("job pool params: PoolSize[{}] CorePoolSize[{}] ActiveCount[{}] MaximumPoolSize[{}]",
                        poolExecutor.getPoolSize(), poolExecutor.getCorePoolSize(), poolExecutor.getActiveCount(),
                        poolExecutor.getMaximumPoolSize());
            }
            saveSnapshotViewMapping(project, restTemplate);
            val tables = SnapshotJobUtils.getSnapshotTables(projectConfig, project);
            val viewTableMapping = readViewTableMapping();
            sourceTableSnapshotMapping = getSourceTableSnapshotMapping(tables, viewTableMapping);

            val allSourceTable = sourceTableSnapshotMapping.keySet();

            checkSourceTable(allSourceTable);

            waitCheckSourceTableTaskDone();

            log.info("Project[{}] stop check and refresh snapshot", project);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new KylinRuntimeException(ie);
        } catch (Exception e) {
            throw new KylinRuntimeException(e);
        } finally {
            checkSourceTableQueue = new LinkedBlockingQueue<>();
            cancelFuture(checkSourceTableFutures);
            checkSourceTableFutures = Maps.newConcurrentMap();
            sourceTableSnapshotMapping = Maps.newHashMap();
            buildSnapshotCount = Maps.newConcurrentMap();
        }
    }
    
    private void checkJobPool() {
        projectConfig = NProjectManager.getInstance(KylinConfig.readSystemKylinConfig()).getProject(project)
                .getConfig();
        val pool = (ThreadPoolExecutor) getJobPool();
        val corePoolSize = pool.getCorePoolSize();
        val poolSizeFromConfig = projectConfig.getSnapshotAutoRefreshMaxConcurrentJobLimit();
        if (poolSizeFromConfig != corePoolSize) {
            pool.setCorePoolSize(poolSizeFromConfig);
            pool.setMaximumPoolSize(poolSizeFromConfig);
            log.info("update AutoRefreshSnapshotRunner job pool size : {} old pool size : {}", poolSizeFromConfig,
                    corePoolSize);
        }
    }

    public void cancelFuture(Map<Future<String>, Long> taskFutures) {
        taskFutures.keySet().forEach(future -> {
            if (!future.isDone()) {
                future.cancel(true);
            }
        });
    }

    public Map<String, Set<String>> readViewTableMapping() {
        val fileSystem = HadoopUtil.getWorkingFileSystem();
        val viewMappingPath = new Path(projectConfig.getSnapshotAutoRefreshDir(project) + VIEW_MAPPING);
        Map<String, Set<String>> result = Maps.newHashMap();
        try {
            if (fileSystem.exists(viewMappingPath)) {
                try (FSDataInputStream inputStream = fileSystem.open(viewMappingPath)) {
                    result.putAll(JsonUtil.readValue(inputStream, new TypeReference<Map<String, Set<String>>>() {
                    }));
                }
            }
        } catch (IOException e) {
            log.error("read viewMapping path[{}] has error", viewMappingPath, e);
        }
        return result;
    }

    public Map<String, List<TableDesc>> getSourceTableSnapshotMapping(List<TableDesc> tables,
            Map<String, Set<String>> viewTableMapping) {
        Map<String, List<TableDesc>> result = Maps.newHashMap();

        val tableManager = NTableMetadataManager.getInstance(projectConfig, project);
        for (Map.Entry<String, Set<String>> entry : viewTableMapping.entrySet()) {
            val snapshotTableIdentity = entry.getKey();
            for (String table : entry.getValue()) {
                val tableDesc = tableManager.getTableDesc(snapshotTableIdentity);
                val snapshots = result.getOrDefault(table, Lists.newArrayList());
                if (tableDesc != null) {
                    snapshots.add(tableDesc);

                }
                result.put(table, snapshots.stream().distinct().collect(Collectors.toList()));
            }
        }
        for (TableDesc tableDesc : tables) {
            if (!tableDesc.isView()) {
                val source = tableDesc.getIdentity().toLowerCase(Locale.ROOT);
                val snapshots = result.getOrDefault(source, Lists.newArrayList());
                snapshots.add(tableDesc);
                result.put(source, snapshots.stream().distinct().collect(Collectors.toList()));
            }
        }
        return result;
    }

    public void saveSnapshotViewMapping(String project, RestTemplate restTemplate) {
        try {
            val url = String.format(Locale.ROOT, "http://%s/kylin/api/snapshots/view_mapping",
                    projectConfig.getServerAddress());
            val req = Maps.newHashMap();
            req.put("project", project);
            log.debug("checkTableNeedRefresh request: {}", req);
            val httpHeaders = new HttpHeaders();
            httpHeaders.add(HttpHeaders.CONTENT_TYPE, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);
            httpHeaders.add(org.apache.http.HttpHeaders.TIMEOUT, "");
            val exchange = restTemplate.exchange(url, HttpMethod.POST,
                    new HttpEntity<>(JsonUtil.writeValueAsBytes(req), httpHeaders), String.class);
            val responseStatus = exchange.getStatusCodeValue();
            if (responseStatus != HttpStatus.SC_OK) {
                throw new KylinRuntimeException(
                        String.format(Locale.ROOT, SNAPSHOT_VIEW_MAPPING_ERROR_MESSAGE, project));
            }
            val responseBody = Optional.ofNullable(exchange.getBody()).orElse("");
            val response = JsonUtil.readValue(responseBody, new TypeReference<RestResponse<Boolean>>() {
            });
            if (!StringUtils.equals(response.getCode(), KylinException.CODE_SUCCESS)) {
                throw new KylinRuntimeException(
                        String.format(Locale.ROOT, SNAPSHOT_VIEW_MAPPING_ERROR_MESSAGE, project));
            }
            if (Boolean.FALSE.equals(response.getData())) {
                throw new KylinRuntimeException(
                        String.format(Locale.ROOT, SNAPSHOT_VIEW_MAPPING_ERROR_MESSAGE, project));
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            throw new KylinRuntimeException(String.format(Locale.ROOT, SNAPSHOT_VIEW_MAPPING_ERROR_MESSAGE, project),
                    e);
        }
    }

    public void checkSourceTable(Set<String> allSourceTable) {
        for (String table : allSourceTable) {
            val runnable = new CheckSourceTableRunnable();
            runnable.setProject(project);
            runnable.setConfig(projectConfig);
            runnable.setTableIdentity(table);
            runnable.setRestTemplate(restTemplate);
            runnable.setCheckSourceTableQueue(checkSourceTableQueue);
            sourceTableSnapshotMapping.get(table).stream()
                    .filter(tableDesc -> StringUtils.equalsIgnoreCase(table, tableDesc.getIdentity())).findFirst()
                    .ifPresent(tableDesc -> runnable.setPartitionColumn(tableDesc.getSelectedSnapshotPartitionCol()));
            val submit = jobPool.submit(runnable, "success");
            checkSourceTableFutures.put(submit, System.currentTimeMillis());
        }
    }

    public void waitCheckSourceTableTaskDone() throws InterruptedException {
        while (true) {
            while (checkSourceTableQueue.peek() != null) {
                val checkResult = checkSourceTableQueue.poll();
                if (checkResult.getNeedRefresh()) {
                    buildSnapshot(checkResult);
                }
            }
            val doneCount = checkSourceTableFutures.keySet().stream().filter(Future::isDone).count();
            if (checkSourceTableFutures.size() == doneCount) {
                break;
            }
            cancelTimeoutFuture(checkSourceTableFutures);
            TimeUnit.SECONDS.sleep(10);
        }
    }

    public void cancelTimeoutFuture(Map<Future<String>, Long> futures) {
        for (Map.Entry<Future<String>, Long> entry : futures.entrySet()) {
            val future = entry.getKey();
            if (!future.isDone()) {
                val runningTime = System.currentTimeMillis() - entry.getValue();
                if (runningTime > projectConfig.getSnapshotAutoRefreshTaskTimeout()) {
                    log.debug("cancel timeout future with timeout setting[{}]",
                            projectConfig.getSnapshotAutoRefreshTaskTimeout());
                    future.cancel(true);
                }
            }
        }
    }

    public void buildSnapshot(CheckSourceTableResult result) {
        val needBuildSnapshots = sourceTableSnapshotMapping.get(result.getTableIdentity());
        for (TableDesc tableDesc : needBuildSnapshots) {
            val sourceTableCount = buildSnapshotCount.getOrDefault(tableDesc.getIdentity(), new AtomicInteger(0));
            log.info("buildSnapshotCount is [{}], tableIdentity is [{}]", sourceTableCount, tableDesc.getIdentity());
            if (sourceTableCount.getAndIncrement() == 0) {
                val runnable = new BuildSnapshotRunnable();
                runnable.setProject(project);
                runnable.setConfig(projectConfig);
                runnable.setRestTemplate(restTemplate);
                runnable.setNeedRefresh(result.getNeedRefresh());
                runnable.setNeedRefreshPartitionsValue(result.getNeedRefreshPartitionsValue());
                runnable.setTableIdentity(tableDesc.getIdentity());
                runnable.setPartitionColumn(tableDesc.getSelectedSnapshotPartitionCol());
                runnable.run();
            }
            buildSnapshotCount.put(tableDesc.getIdentity(), sourceTableCount);
        }
    }

    public static synchronized void shutdown(String project) {
        val refreshSnapshotRunner = AutoRefreshSnapshotRunner.getInstanceByProject(project);
        if (null != refreshSnapshotRunner) {
            refreshSnapshotRunner.innerShutdown();
            log.info("update snapshot automatic refresh fetch pool size success");
        }
    }

    @SneakyThrows
    public void innerShutdown() {
        if (Thread.currentThread().isInterrupted()) {
            log.warn("shutdown->current thread is interrupted,{}", Thread.currentThread().getName());
            throw new InterruptedException();
        }

        log.info("Shutting down AutoRefreshSnapshotRunner for project {} ....", project);
        if (null != jobPool) {
            ExecutorServiceUtil.shutdownGracefully(jobPool, 60);
        }
        INSTANCE_MAP.remove(project);
    }

    @Override
    public void run() {
        try (SetLogCategory ignored = new SetLogCategory(LogConstant.SCHEDULE_CATEGORY)) {
            checkJobPool();
            saveMarkFile();
            doRun();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            deleteMarkFile();
        }
    }

    public void runWhenSchedulerInit() {
        try (SetLogCategory ignored = new SetLogCategory(LogConstant.SCHEDULE_CATEGORY)) {
            doRun();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            deleteMarkFile();
        }
    }

    public void saveMarkFile() {
        val markFilePath = new Path(projectConfig.getSnapshotAutoRefreshDir(project) + MARK);
        val fileSystem = HadoopUtil.getWorkingFileSystem();
        try (val out = fileSystem.create(markFilePath, true)) {
            out.write(new byte[] {});
        } catch (IOException e) {
            log.error("overwrite mark file [{}] failed!", markFilePath, e);
        }
    }

    public void deleteMarkFile() {
        val markFilePath = new Path(projectConfig.getSnapshotAutoRefreshDir(project) + MARK);
        val fileSystem = HadoopUtil.getWorkingFileSystem();
        try {
            if (fileSystem.exists(markFilePath)) {
                fileSystem.delete(markFilePath, true);
            }
        } catch (IOException e) {
            log.error("delete mark file [{}] failed!", markFilePath, e);
        }
    }
}
