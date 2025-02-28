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

import static org.apache.kylin.common.constant.Constants.MARK;
import static org.apache.kylin.common.constant.Constants.SOURCE_TABLE_STATS;
import static org.apache.kylin.common.constant.Constants.VIEW_MAPPING;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.util.FileSystemUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.engine.spark.source.SparkSqlUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.snapshot.SnapshotJobUtils;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.rest.model.SnapshotSourceTableStats;
import org.apache.kylin.rest.response.SnapshotSourceTableStatsResponse;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.type.TypeReference;

import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;
import scala.Option;
import scala.collection.JavaConverters;

@Slf4j
@Service("snapshotSourceTableStatsService")
public class SnapshotSourceTableStatsService extends BasicService {
    private static final String FILES_SIZE = "files_size";
    private static final String FILES_MODIFICATION_TIMES = "files_modification_times";

    public Boolean saveSnapshotViewMapping(String project) {
        try {
            val tables = SnapshotJobUtils.getSnapshotTables(getConfig(), project);
            val catalog = SparderEnv.getSparkSession().sessionState().catalog();
            val viewMapping = Maps.<String, Set<String>> newHashMap();
            for (TableDesc tableDesc : tables) {
                if (tableDesc.isView()) {
                    val tableIdentifier = TableIdentifier.apply(tableDesc.getName(),
                            Option.apply(tableDesc.getDatabase()));
                    val tableMetadata = catalog.getTempViewOrPermanentTableMetadata(tableIdentifier);
                    val sourceTablesTmp = getSnapshotSourceTables(tableMetadata);
                    val sourceTables = Sets.<String> newHashSet();
                    for (String sourceTable : sourceTablesTmp) {
                        val split = StringUtils.split(sourceTable, ".");
                        String source = split.length < 2 ? "default." + sourceTable : sourceTable;
                        sourceTables.add(source);
                    }
                    viewMapping.put(tableDesc.getIdentity(), sourceTables);
                }
            }
            val fileSystem = HadoopUtil.getWorkingFileSystem();
            val pathStr = getConfig().getSnapshotAutoRefreshDir(project) + VIEW_MAPPING;
            val snapshotTablesPath = new Path(pathStr);
            try (val out = fileSystem.create(snapshotTablesPath, true)) {
                out.write(JsonUtil.writeValueAsBytes(viewMapping));
            }
            log.debug("save snapshot view mapping path : {}", pathStr);
            return true;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }

    private Set<String> getSnapshotSourceTables(CatalogTable tableMetadata) {
        Set<String> viewSourceTables = Sets.newHashSet();
        try {
            viewSourceTables = SparkSqlUtil
                    .getViewOrignalTables(tableMetadata.qualifiedName(), SparderEnv.getSparkSession()) //
                    .stream().filter(StringUtils::isNotBlank).collect(Collectors.toSet());
            log.info("snapshot[{}] view original tables: [{}]", tableMetadata.qualifiedName(), viewSourceTables);
        } catch (Exception e) {
            log.error("snapshot[{}] get view original tables error", tableMetadata.qualifiedName(), e);
        }
        return viewSourceTables;
    }

    public SnapshotSourceTableStatsResponse checkSourceTableStats(String project, String database, String table,
            String snapshotPartitionCol) {
        return checkSourceTableStats(project, database, table, snapshotPartitionCol, null);
    }

    public SnapshotSourceTableStatsResponse checkSourceTableStats(String project, String database, String table,
            String snapshotPartitionCol, String catalogName) {
        try {
            val needRefreshPartitions = Lists.<CatalogTablePartition> newCopyOnWriteArrayList();
            boolean needRefresh;
            if (StringUtils.isEmpty(catalogName)) {
                val catalog = SparderEnv.getSparkSession().sessionState().catalog();
                val tableIdentifier = TableIdentifier.apply(table, Option.apply(database));
                val tableMetadata = catalog.getTempViewOrPermanentTableMetadata(tableIdentifier);
                needRefresh = checkTable(project, catalog, tableMetadata, needRefreshPartitions);
            } else {
                val projectConfig = NProjectManager.getInstance(getConfig()).getProject(project).getConfig();
                val identifier = Identifier.of(database.split("\\."), table);
                needRefresh = checkCatalogTable(project, projectConfig, catalogName, identifier);
            }

            val response = createSnapshotSourceTableStatsResponse(snapshotPartitionCol, needRefreshPartitions,
                    needRefresh);
            log.info(
                    "Project[{}] Snapshot[{}] refresh check and save snapshot table location files response:"
                            + " needRefresh[{}], needRefreshPartitions[{}]",
                    project, table, response.getNeedRefresh(), response.getNeedRefreshPartitionsValue());
            return response;
        } catch (Exception e) {
            log.info("Project[{}] [{}.{}] refresh check and save snapshot table location files failed", project,
                    database, table);
            log.error(e.getMessage(), e);
            return new SnapshotSourceTableStatsResponse(false);
        }
    }

    private SnapshotSourceTableStatsResponse createSnapshotSourceTableStatsResponse(String snapshotPartitionCol,
            List<CatalogTablePartition> needRefreshPartitions, boolean needRefresh) {
        val response = new SnapshotSourceTableStatsResponse(needRefresh);
        if (needRefresh && StringUtils.isNotBlank(snapshotPartitionCol)) {
            Set<String> partitionsValue = needRefreshPartitions.stream()
                    .map(partition -> getPrimaryPartitionValue(snapshotPartitionCol, partition))
                    .filter(Objects::nonNull).filter(Option::isDefined).map(Option::get).collect(Collectors.toSet());
            response.setNeedRefreshPartitionsValue(partitionsValue);
        }
        return response;
    }

    public static Option<String> getPrimaryPartitionValue(String snapshotPartitionCol,
            CatalogTablePartition partition) {
        val spec = partition.spec();
        if (spec.contains(snapshotPartitionCol)) {
            return spec.get(snapshotPartitionCol);
        } else if (spec.contains(snapshotPartitionCol.toLowerCase(Locale.ROOT))) {
            return spec.get(snapshotPartitionCol.toLowerCase(Locale.ROOT));
        } else if (spec.contains(snapshotPartitionCol.toUpperCase(Locale.ROOT))) {
            return spec.get(snapshotPartitionCol.toUpperCase(Locale.ROOT));
        }
        return null;
    }

    public Boolean checkTable(String project, SessionCatalog catalog, CatalogTable tableMetadata,
            List<CatalogTablePartition> needRefreshPartitions) throws IOException {
        val projectConfig = NProjectManager.getInstance(getConfig()).getProject(project).getConfig();
        val tableIdentity = tableMetadata.qualifiedName().toLowerCase(Locale.ROOT);
        if (!tableMetadata.partitionColumnNames().isEmpty()) {
            return checkPartitionHiveTable(project, catalog, tableMetadata, needRefreshPartitions, projectConfig,
                    tableIdentity);
        }
        return checkHiveTable(project, tableMetadata, projectConfig, tableIdentity);
    }

    public boolean checkCatalogTable(String project, KylinConfig projectConfig, String catalogName,
            Identifier identifier) throws IOException, NoSuchTableException {
        val catalog = SparderEnv.getSparkSession().sessionState().catalogManager().catalog(catalogName);
        if (catalog instanceof TableCatalog) {
            val tableCatalog = (TableCatalog) catalog;
            val table = tableCatalog.loadTable(identifier);
            var location = table.properties().get("location");
            if (tableCatalog.getClass().toString().contains("iceberg"))
                location = location + "/metadata";
            return checkTableLocation(project, location, projectConfig, catalogName + "." + identifier.toString());
        }
        throw new KylinRuntimeException("unsupported catalog:" + catalog);
    }

    public boolean checkHiveTable(String project, CatalogTable tableMetadata, KylinConfig projectConfig,
            String tableIdentity) throws IOException {
        val location = tableMetadata.location().getPath();
        return checkTableLocation(project, location, projectConfig, tableIdentity);
    }

    private boolean checkTableLocation(String project, String location, KylinConfig projectConfig, String tableIdentity)
            throws IOException {
        if (checkSnapshotSourceTableStatsJsonFile(project, tableIdentity)) {
            log.info("skip checkPartitionHiveTable: last cron task was stopped manually");
            return true;
        }
        val jsonFilePair = getSnapshotSourceTableStatsJsonFromHDFS(project, tableIdentity);
        val snapshotSourceTableStatsJsonExist = jsonFilePair.getFirst();
        val snapshotSourceTableStatsJson = jsonFilePair.getSecond();
        val filesStatus = Lists.<FileStatus> newArrayList();
        if (StringUtils.isBlank(location)) {
            return projectConfig.isSnapshotNullLocationAutoRefreshEnabled();
        }
        val needRefresh = checkLocation(location, filesStatus, snapshotSourceTableStatsJson, projectConfig);
        if (Boolean.FALSE.equals(snapshotSourceTableStatsJsonExist) || Boolean.TRUE.equals(needRefresh)) {
            val newSnapshotSourceTableStatsJson = createSnapshotSourceTableStats(location, projectConfig, filesStatus);
            writeSourceTableStats(project, tableIdentity, newSnapshotSourceTableStatsJson);
        }
        if (Boolean.FALSE.equals(snapshotSourceTableStatsJsonExist)) {
            return projectConfig.isSnapshotFirstAutoRefreshEnabled();
        }
        return needRefresh;
    }

    public boolean checkSnapshotSourceTableStatsJsonFile(String project, String tableIdentity) throws IOException {
        val fileSystem = HadoopUtil.getWorkingFileSystem();
        val snapshotTablesPath = sourceTableStatsFile(tableIdentity, project);
        val markFilePath = new Path(getConfig().getSnapshotAutoRefreshDir(project) + MARK);
        if (fileSystem.exists(snapshotTablesPath) && fileSystem.exists(markFilePath)) {
            val snapshotTableFileStatus = fileSystem.getFileStatus(snapshotTablesPath);
            val markFilePathFileStatus = fileSystem.getFileStatus(markFilePath);
            // Mark file is earlier than snapshot table json file and needs to be refreshed.
            // Maybe, last cron task was stopped manually
            return markFilePathFileStatus.getModificationTime() < snapshotTableFileStatus.getModificationTime();
        }
        return false;
    }

    public Pair<Boolean, Map<String, SnapshotSourceTableStats>> getSnapshotSourceTableStatsJsonFromHDFS(String project,
            String tableIdentity) throws IOException {
        val fileSystem = HadoopUtil.getWorkingFileSystem();
        val snapshotTablesPath = sourceTableStatsFile(tableIdentity, project);
        log.info("SnapshotSourceTableStats path : [{}]", snapshotTablesPath);
        Map<String, SnapshotSourceTableStats> result = Maps.newHashMap();
        if (fileSystem.exists(snapshotTablesPath)) {
            try (FSDataInputStream inputStream = fileSystem.open(snapshotTablesPath)) {
                result = JsonUtil.readValue(inputStream, new TypeReference<Map<String, SnapshotSourceTableStats>>() {
                });
            } catch (IOException e) {
                log.error("read SnapshotSourceTableStats path[{}] to SourceTableStats has error", snapshotTablesPath,
                        e);
            }
            return new Pair<>(true, result);
        } else {
            return new Pair<>(false, result);
        }
    }

    public Path sourceTableStatsFile(String tableIdentity, String project) {
        return new Path(getConfig().getSnapshotAutoRefreshDir(project) + SOURCE_TABLE_STATS + "/" + tableIdentity);
    }

    public boolean checkLocation(String location, List<FileStatus> filesStatus,
            Map<String, SnapshotSourceTableStats> snapshotSourceTableStatsJson, KylinConfig config) throws IOException {
        log.info("check table/partition location: {}", location);
        filesStatus.addAll(getLocationFileStatus(location, config));
        // check file count
        val sourceTableStats = snapshotSourceTableStatsJson.get(location);
        if (sourceTableStats == null) {
            log.debug("sourceTableStats is null, sourceTableStatsStatus length is [{}]", filesStatus.size());
            return CollectionUtils.isNotEmpty(filesStatus);
        }
        if (sourceTableStats.getFilesCount() != filesStatus.size()) {
            log.debug("sourceTableStats FileCount is [{}], sourceTableStatsStatus length is [{}]",
                    sourceTableStats.getFilesCount(), filesStatus.size());
            return true;
        }
        // check files modification times
        val tableFilesModifyTimesAndSize = getTableFilesModifyTimesAndSize(filesStatus, config);
        if (!CollectionUtils.containsAll(tableFilesModifyTimesAndSize.get(FILES_MODIFICATION_TIMES),
                sourceTableStats.getFilesModificationTime())
                || !CollectionUtils.containsAll(sourceTableStats.getFilesModificationTime(),
                        tableFilesModifyTimesAndSize.get(FILES_MODIFICATION_TIMES))) {
            log.debug("files_modification_times: sourceTableStats is [{}], sourceTableStatsStatus is [{}]",
                    sourceTableStats.getFilesModificationTime(),
                    tableFilesModifyTimesAndSize.get(FILES_MODIFICATION_TIMES));
            return true;
        }
        // check files size
        log.debug("files_size: sourceTableStats is [{}], sourceTableStatsStatus is [{}]",
                sourceTableStats.getFilesSize(), tableFilesModifyTimesAndSize.get(FILES_SIZE));
        return !CollectionUtils.containsAll(tableFilesModifyTimesAndSize.get(FILES_SIZE),
                sourceTableStats.getFilesSize())
                || !CollectionUtils.containsAll(sourceTableStats.getFilesSize(),
                        tableFilesModifyTimesAndSize.get(FILES_SIZE));
    }

    public Map<String, SnapshotSourceTableStats> createSnapshotSourceTableStats(String location, KylinConfig config,
            List<FileStatus> locationFilesStatus) {
        Map<String, SnapshotSourceTableStats> newSnapshotSourceTableStatsJson = Maps.newHashMap();
        val sourceTableStats = new SnapshotSourceTableStats();
        val filesSize = Lists.<Long> newArrayList();
        val filesModificationTime = Lists.<Long> newArrayList();
        locationFilesStatus.stream().limit(config.getSnapshotAutoRefreshFetchFilesCount()).forEach(fileStatus -> {
            filesSize.add(fileStatus.getLen());
            filesModificationTime.add(fileStatus.getModificationTime());
        });
        sourceTableStats.setFilesSize(filesSize);
        sourceTableStats.setFilesModificationTime(filesModificationTime);
        sourceTableStats.setFilesCount(locationFilesStatus.size());

        newSnapshotSourceTableStatsJson.put(location, sourceTableStats);
        return newSnapshotSourceTableStatsJson;
    }

    public void writeSourceTableStats(String project, String tableIdentity,
            Map<String, SnapshotSourceTableStats> snapshotSourceTableStatsJson) {
        val fileSystem = HadoopUtil.getWorkingFileSystem();
        val snapshotTablesPath = sourceTableStatsFile(tableIdentity, project);
        try (val out = fileSystem.create(snapshotTablesPath, true)) {
            out.write(JsonUtil.writeValueAsBytes(snapshotSourceTableStatsJson));
        } catch (IOException e) {
            log.error("overwrite SourceTableStats to path[{}] failed!", snapshotTablesPath, e);
            try {
                fileSystem.delete(snapshotTablesPath, true);
            } catch (IOException ignore) {
                log.error("Write SourceTableStats failed! Error for delete file: {}", snapshotTablesPath, e);
            }
        }
    }

    public boolean checkPartitionHiveTable(String project, SessionCatalog catalog, CatalogTable tableMetadata,
            List<CatalogTablePartition> needRefreshPartitions, KylinConfig projectConfig, String tableIdentity)
            throws IOException {
        val partitions = JavaConverters
                .seqAsJavaList(catalog.listPartitions(tableMetadata.identifier(), Option.empty()));
        if (checkSnapshotSourceTableStatsJsonFile(project, tableIdentity)) {
            val needCheckPartitions = partitions.stream()
                    .sorted((ctp1, ctp2) -> Long.compare(ctp2.createTime(), ctp1.createTime()))
                    .limit(projectConfig.getSnapshotAutoRefreshFetchPartitionsCount()).collect(Collectors.toList());
            needRefreshPartitions.addAll(needCheckPartitions);
            log.info("skip checkPartitionHiveTable: last cron task was stopped manually");
            return true;
        }
        val jsonFilePair = getSnapshotSourceTableStatsJsonFromHDFS(project, tableIdentity);
        val snapshotSourceTableStatsJsonExist = jsonFilePair.getFirst();
        val snapshotSourceTableStatsJson = jsonFilePair.getSecond();
        val needSavePartitionsFilesStatus = Maps.<String, List<FileStatus>> newHashMap();
        val needRefresh = checkPartitionsLocation(partitions, snapshotSourceTableStatsJson, needRefreshPartitions,
                needSavePartitionsFilesStatus, projectConfig);
        if (Boolean.FALSE.equals(snapshotSourceTableStatsJsonExist) || Boolean.TRUE.equals(needRefresh)) {
            Map<String, SnapshotSourceTableStats> newSnapshotSourceTableStatsJson = Maps.newHashMap();
            for (CatalogTablePartition partition : partitions) {
                createPartitionSnapshotSourceTableStats(partition, needSavePartitionsFilesStatus,
                        newSnapshotSourceTableStatsJson, projectConfig);
            }
            writeSourceTableStats(project, tableIdentity, newSnapshotSourceTableStatsJson);
        }
        if (Boolean.FALSE.equals(snapshotSourceTableStatsJsonExist)) {
            return projectConfig.isSnapshotFirstAutoRefreshEnabled();
        }
        return needRefresh;
    }

    public boolean checkPartitionsLocation(List<CatalogTablePartition> partitions,
            Map<String, SnapshotSourceTableStats> snapshotSourceTableStatsJson,
            List<CatalogTablePartition> needRefreshPartitions,
            Map<String, List<FileStatus>> needSavePartitionsFilesStatus, KylinConfig config) throws IOException {
        val needCheckPartitions = partitions.stream()
                .sorted((ctp1, ctp2) -> Long.compare(ctp2.createTime(), ctp1.createTime()))
                .limit(config.getSnapshotAutoRefreshFetchPartitionsCount()).collect(Collectors.toList());
        putNeedSavePartitionsFilesStatus(needCheckPartitions, needSavePartitionsFilesStatus, config);

        // check partition count
        if (partitions.size() != snapshotSourceTableStatsJson.size()) {
            needRefreshPartitions.addAll(needCheckPartitions);
            log.debug("sourceTableStats size is [{}], partitions size is [{}]", snapshotSourceTableStatsJson.size(),
                    partitions.size());
            return true;
        }
        // check partition create time
        var result = false;
        for (CatalogTablePartition partition : needCheckPartitions) {
            final String location = partition.location().getPath();
            val sourceTableStats = snapshotSourceTableStatsJson.get(location);
            if (sourceTableStats == null || sourceTableStats.getCreateTime() != partition.createTime()) {
                needRefreshPartitions.add(partition);
                log.debug("sourceTableStats is {}, partition create is [{}]", sourceTableStats, partition.createTime());
                result = true;
            }
        }
        if (result) {
            return true;
        }
        // check partition files
        for (CatalogTablePartition partition : needCheckPartitions) {
            val filesStatus = Lists.<FileStatus> newArrayList();
            if (checkLocation(partition.location().getPath(), filesStatus, snapshotSourceTableStatsJson, config)) {
                needRefreshPartitions.add(partition);
                result = true;
            }
        }
        return result;
    }

    public void putNeedSavePartitionsFilesStatus(List<CatalogTablePartition> partitions,
            Map<String, List<FileStatus>> locationsFileStatusMap, KylinConfig config) throws IOException {
        for (CatalogTablePartition partition : partitions) {
            val filesStatus = getLocationFileStatus(partition.location().getPath(), config);
            locationsFileStatusMap.put(partition.location().getPath(), filesStatus);
        }
    }

    public List<FileStatus> getLocationFileStatus(String location, KylinConfig config) throws IOException {
        var fileSystem = StringUtils.isBlank(config.getWriteClusterWorkingDir()) ? HadoopUtil.getWorkingFileSystem()
                : HadoopUtil.getWriteClusterFileSystem();

        val sourceTableStatsPath = new Path(location);
        val pathSchema = sourceTableStatsPath.toUri().getScheme();
        val fileSchema = fileSystem.getUri().getScheme();
        if (pathSchema != null && !pathSchema.equalsIgnoreCase(fileSchema)) {
            fileSystem = sourceTableStatsPath.getFileSystem(SparderEnv.getHadoopConfiguration());
        }
        if (!fileSystem.exists(sourceTableStatsPath)) {
            return Collections.emptyList();
        }
        val fileStatuses = FileSystemUtil.listStatus(fileSystem, sourceTableStatsPath);
        return Arrays.stream(fileStatuses)
                .sorted((fs1, fs2) -> Long.compare(fs2.getModificationTime(), fs1.getModificationTime()))
                .collect(Collectors.toList());
    }

    public Map<String, List<Long>> getTableFilesModifyTimesAndSize(List<FileStatus> fileStatuses, KylinConfig config) {
        val fileModificationTimes = Lists.<Long> newArrayList();
        val fileSizes = Lists.<Long> newArrayList();
        fileStatuses.stream().limit(config.getSnapshotAutoRefreshFetchFilesCount()).forEach(fileStatus -> {
            fileModificationTimes.add(fileStatus.getModificationTime());
            fileSizes.add(fileStatus.getLen());
        });
        val result = Maps.<String, List<Long>> newHashMap();
        result.put(FILES_MODIFICATION_TIMES, fileModificationTimes);
        result.put(FILES_SIZE, fileSizes);
        return result;
    }

    public void createPartitionSnapshotSourceTableStats(CatalogTablePartition partition,
            Map<String, List<FileStatus>> locationsFileStatusMap,
            Map<String, SnapshotSourceTableStats> snapshotSourceTableStatsJson, KylinConfig config) {
        val location = partition.location().getPath();
        val sourceTableStats = snapshotSourceTableStatsJson.computeIfAbsent(location,
                key -> new SnapshotSourceTableStats());

        val filesSize = Lists.<Long> newArrayList();
        val filesModificationTime = Lists.<Long> newArrayList();
        val partitionFileStatuses = locationsFileStatusMap.getOrDefault(location, Lists.newArrayList());
        partitionFileStatuses.stream().limit(config.getSnapshotAutoRefreshFetchFilesCount()).forEach(fileStatus -> {
            filesSize.add(fileStatus.getLen());
            filesModificationTime.add(fileStatus.getModificationTime());
        });
        sourceTableStats.setFilesSize(filesSize);
        sourceTableStats.setFilesModificationTime(filesModificationTime);
        sourceTableStats.setFilesCount(partitionFileStatuses.size());

        sourceTableStats.setCreateTime(partition.createTime());
        snapshotSourceTableStatsJson.put(location, sourceTableStats);
    }
}
