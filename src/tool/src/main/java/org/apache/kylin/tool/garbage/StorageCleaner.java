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
package org.apache.kylin.tool.garbage;

import static org.apache.kylin.common.util.HadoopUtil.DELTA_STORAGE_ROOT;
import static org.apache.kylin.common.util.HadoopUtil.FLAT_TABLE_STORAGE_ROOT;
import static org.apache.kylin.common.util.HadoopUtil.GLOBAL_DICT_STORAGE_ROOT;
import static org.apache.kylin.common.util.HadoopUtil.JOB_TMP_ROOT;
import static org.apache.kylin.common.util.HadoopUtil.PARQUET_STORAGE_ROOT;
import static org.apache.kylin.common.util.HadoopUtil.SNAPSHOT_STORAGE_ROOT;
import static org.apache.kylin.common.util.HadoopUtil.TABLE_EXD_STORAGE_ROOT;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.CliCommandExecutor.CliCmdExecResult;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.guava30.shaded.common.util.concurrent.RateLimiter;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.dao.JobInfoDao;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.LayoutPartition;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataLayoutDetails;
import org.apache.kylin.metadata.cube.model.NDataSegDetails;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.query.util.QueryHisStoreUtil;
import org.apache.kylin.query.util.ExtractFactory;
import org.apache.kylin.tool.constant.StringConstant;
import org.apache.kylin.tool.util.ProjectTemporaryTableCleanerHelper;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StorageCleaner implements GarbageCleaner {
    private final boolean cleanup;
    @Getter
    private final Collection<String> projectNames;
    private final KylinConfig kylinConfig;

    // for s3 https://olapio.atlassian.net/browse/AL-3154
    private static final RateLimiter rateLimiter = RateLimiter.create(Integer.MAX_VALUE);

    public enum CleanerTag {
        ROUTINE, CLI, SERVICE
    }

    @Getter
    private CleanerTag tag = CleanerTag.ROUTINE;

    @Getter
    private String traceId = "";

    public StorageCleaner() throws Exception {
        this(true);
    }

    public StorageCleaner(boolean cleanup) throws Exception {
        this(cleanup, Collections.emptyList());
    }

    public StorageCleaner(boolean cleanup, Collection<String> projects) throws Exception {
        this.cleanup = cleanup;
        this.projectNames = projects;
        this.kylinConfig = KylinConfig.getInstanceFromEnv();
    }

    public StorageCleaner(boolean cleanup, Collection<String> projects, double requestFSRate, int tRetryTimes)
            throws Exception {
        this(cleanup, projects);
        if (requestFSRate > 0.0) {
            rateLimiter.setRate(requestFSRate);
        }
        if (tRetryTimes > 0) {
            FileSystemDecorator.retryTimes = tRetryTimes;
        }
    }

    public StorageCleaner withTag(CleanerTag tag) {
        this.tag = tag;
        return this;
    }

    public StorageCleaner withTraceId(String id) {
        this.traceId = id;
        return this;
    }

    @Getter
    private Set<StorageItem> outdatedItems = Sets.newHashSet();

    private Set<StorageItem> allFileSystems = Sets.newHashSet();

    @Override
    public void execute() throws InterruptedException {
        long start = System.currentTimeMillis();
        val config = KylinConfig.getInstanceFromEnv();
        long startTime = System.currentTimeMillis();

        val projects = NProjectManager.getInstance(config).listAllProjects().stream()
                .filter(projectInstance -> projectNames.isEmpty() || projectNames.contains(projectInstance.getName()))
                .collect(Collectors.toList());

        projects.stream().map(project -> NDataflowManager.getInstance(config, project.getName()).listAllDataflows())
                .flatMap(Collection::stream).map(dataflow -> KapConfig.wrap(dataflow.getConfig()))
                .map(KapConfig::getMetadataWorkingDirectory).forEach(hdfsWorkingDir -> {
                    val fs = HadoopUtil.getWorkingFileSystem();
                    allFileSystems.add(new StorageItem(FileSystemDecorator.getInstance(fs), hdfsWorkingDir));
                });
        allFileSystems.add(new StorageItem(FileSystemDecorator.getInstance(HadoopUtil.getWorkingFileSystem()),
                config.getHdfsWorkingDirectory()));
        // Check if independent storage of flat tables under read/write separation is enabled
        // For build tasks it is a project-level parameter(Higher project-level priority),
        // but for cleaning up storage garbage,
        // WRITING_CLUSTER_WORKING_DIR is a system-level parameter
        if (kylinConfig.isBuildFilesSeparationEnabled()) {
            allFileSystems
                    .add(new StorageItem(FileSystemDecorator.getInstance(HadoopUtil.getWritingClusterFileSystem()),
                            config.getWritingClusterWorkingDir("")));
        }
        log.info("all file systems are {}", allFileSystems);
        for (StorageItem allFileSystem : allFileSystems) {
            log.debug("start to collect HDFS from {}", allFileSystem.getPath());
            try {
                collectFromHDFS(allFileSystem);
            } catch (IOException e) {
                log.warn("No garbage files collected from {}", allFileSystem.getPath(), e);
            }
            log.debug("folder {} is collected，detailed -> {}", allFileSystem.getPath(), allFileSystems);
        }
        collectDeletedProject();
        for (ProjectInstance project : projects) {
            collect(project.getName());
        }

        long configSurvivalTimeThreshold = config.getCuboidLayoutSurvivalTimeThreshold();
        long protectionTime = startTime - configSurvivalTimeThreshold;
        for (StorageItem item : allFileSystems) {
            for (FileTreeNode node : item.getAllNodes()) {
                val path = new Path(item.getPath(), node.getRelativePath());
                try {
                    log.debug("start to add item {}", path);
                    addItem(item.getFileSystemDecorator(), path, protectionTime);
                } catch (IOException e) {
                    log.warn("{} not found", path, e);
                }
            }
        }
        boolean allSuccess = cleanup();
        printConsole(allSuccess, System.currentTimeMillis() - start);
    }

    public void printConsole(boolean success, long duration) {
        System.out.println(StringConstant.ANSI_BLUE + "Kylin 5.0 garbage report: (cleanup=" + cleanup + ")"
                + StringConstant.ANSI_RESET);
        for (StorageItem item : outdatedItems) {
            System.out.println("  Storage File: " + item.getPath());
        }
        String jobName = "Storage GC cleanup job ";
        if (!cleanup) {
            System.out.println(
                    StringConstant.ANSI_BLUE + "Dry run mode, no data is deleted." + StringConstant.ANSI_RESET);
            jobName = "Storage GC check job ";
        }
        if (!success) {
            System.out.println(StringConstant.ANSI_RED + jobName + "FAILED." + StringConstant.ANSI_RESET);
            System.out.println(
                    StringConstant.ANSI_RED + jobName + "finished in " + duration + " ms." + StringConstant.ANSI_RESET);
        } else {
            System.out.println(StringConstant.ANSI_GREEN + jobName + "SUCCEED." + StringConstant.ANSI_RESET);
            System.out.println(StringConstant.ANSI_GREEN + jobName + "finished in " + duration + " ms."
                    + StringConstant.ANSI_RESET);
        }

    }

    public void collectDeletedProject() {
        val config = KylinConfig.getInstanceFromEnv();
        val projects = NProjectManager.getInstance(config).listAllProjects().stream().map(ProjectInstance::getName)
                .collect(Collectors.toSet());
        for (StorageItem item : allFileSystems) {
            item.getProjectNodes().removeIf(node -> projects.contains(node.getName()));
            log.info(String.valueOf(item.projectNodes.size()));
        }
    }

    public void collect(String project) {
        log.info("collect garbage for project: {}", project);
        new ProjectStorageCleaner(project).execute();
        log.info("clean temporary table for project: {}", project);
        new ProjectTemporaryTableCleaner(project).execute();
    }

    public boolean cleanup() throws InterruptedException {
        boolean success = true;
        if (cleanup) {
            Stats stats = new Stats() {
                @Override
                public void heartBeat() {
                    double percent = 100D * (successItems.size() + errorItems.size()) / allItems.size();
                    String logInfo = String.format(Locale.ROOT, "Progress: %2.1f%%, %d resource, %d error", percent,
                            allItems.size(), errorItems.size());
                    System.out.println(logInfo);
                }
            };
            stats.onAllStart(outdatedItems);
            for (StorageItem item : outdatedItems) {
                log.debug("try to delete {}", item.getPath());
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException();
                }
                try {
                    stats.onItemStart(item);
                    item.getFileSystemDecorator().delete(new Path(item.getPath()), true);
                    stats.onItemSuccess(item);
                } catch (IOException e) {
                    log.error("delete file " + item.getPath() + " failed", e);
                    stats.onItemError(item);
                    success = false;
                }
            }
        }
        return success;
    }

    private String getDataflowBaseDir(String project) {
        return project + PARQUET_STORAGE_ROOT + "/";
    }

    private String getDataflowDir(String project, String dataflowId) {
        return getDataflowBaseDir(project) + dataflowId;
    }

    private String getDfFlatTableDir(String project, String dataFlowId) {
        return project + FLAT_TABLE_STORAGE_ROOT + "/" + dataFlowId;
    }

    private void addItem(FileSystemDecorator fs, Path itemPath, long protectionTime) throws IOException {
        val status = fs.getFileStatus(itemPath);
        if (status.getPath().getName().startsWith(".")) {
            return;
        }
        if (status.getModificationTime() > protectionTime) {
            return;
        }

        outdatedItems.add(new StorageCleaner.StorageItem(fs, status.getPath().toString()));
    }

    private String getDictDir(String project) {
        return project + GLOBAL_DICT_STORAGE_ROOT;
    }

    private String getSegmentFlatTableDir(String project, NDataSegment segment) {
        return getDfFlatTableDir(project, segment.getDataflow().getId()) + "/" + segment.getId();
    }

    private String getDataLayoutDir(NDataLayout dataLayout) {
        NDataSegDetails segDetails = dataLayout.getSegDetails();
        return getDataflowDir(segDetails.getProject(), segDetails.getDataSegment().getDataflow().getId()) + "/"
                + segDetails.getUuid() + "/" + dataLayout.getLayoutId();
    }

    private String getDataPartitionDir(NDataLayout dataLayout, LayoutPartition dataPartition) {
        return getDataLayoutDir(dataLayout) + "/" + dataPartition.getBucketId();
    }

    private void collectFromHDFS(StorageItem item) throws IOException {
        val projectFolders = item.getFileSystemDecorator().listStatus(new Path(item.getPath()),
                path -> !path.getName().startsWith("_")
                        && (this.projectNames.isEmpty() || this.projectNames.contains(path.getName())));
        for (FileStatus projectFolder : projectFolders) {
            List<FileTreeNode> tableSnapshotParents = Lists.newArrayList();
            val projectNode = new ProjectFileTreeNode(projectFolder.getPath().getName());
            for (Pair<String, List<FileTreeNode>> pair : Arrays.asList(
                    Pair.newPair(JOB_TMP_ROOT.substring(1), projectNode.getJobTmps()),
                    Pair.newPair(GLOBAL_DICT_STORAGE_ROOT.substring(1), projectNode.getGlobalDictTables()),
                    Pair.newPair(PARQUET_STORAGE_ROOT.substring(1), projectNode.getDataflows()),
                    Pair.newPair(DELTA_STORAGE_ROOT.substring(1), projectNode.getDeltaDataFlows()),
                    Pair.newPair(TABLE_EXD_STORAGE_ROOT.substring(1), projectNode.getTableExds()),
                    Pair.newPair(SNAPSHOT_STORAGE_ROOT.substring(1), tableSnapshotParents),
                    Pair.newPair(FLAT_TABLE_STORAGE_ROOT.substring(1), projectNode.getDfFlatTables()))) {
                val treeNode = new FileTreeNode(pair.getFirst(), projectNode);
                try {
                    log.debug("collect files from {}", pair.getFirst());
                    Stream.of(item.getFileSystemDecorator()
                            .listStatus(new Path(item.getPath(), treeNode.getRelativePath())))
                            .forEach(x -> pair.getSecond().add(new FileTreeNode(x.getPath().getName(), treeNode)));
                } catch (FileNotFoundException e) {
                    log.info("folder {} not found", new Path(item.getPath(), treeNode.getRelativePath()));
                }
            }
            item.getProjectNodes().add(projectNode);
            item.getProjects().put(projectNode.getName(), projectNode);
            for (Pair<List<FileTreeNode>, List<FileTreeNode>> pair : Arrays.asList(
                    Pair.newPair(tableSnapshotParents, projectNode.getSnapshots()), //
                    Pair.newPair(projectNode.getGlobalDictTables(), projectNode.getGlobalDictColumns()), //
                    Pair.newPair(projectNode.getDataflows(), projectNode.getSegments()), //
                    Pair.newPair(projectNode.getDeltaDataFlows(), projectNode.getDeltaDataLayouts()),
                    Pair.newPair(projectNode.getSegments(), projectNode.getLayouts()),
                    Pair.newPair(projectNode.getDfFlatTables(), projectNode.getSegmentFlatTables()))) {
                val slot = pair.getSecond();
                for (FileTreeNode node : pair.getFirst()) {
                    log.debug("collect from {} -> {}", node.getName(), node);
                    Stream.of(
                            item.getFileSystemDecorator().listStatus(new Path(item.getPath(), node.getRelativePath())))
                            .forEach(x -> slot.add(new FileTreeNode(x.getPath().getName(), node)));
                }
            }
            projectNode.getBuckets()
                    .addAll(collectMultiPartitions(item, projectNode.getName(), projectNode.getLayouts()));
        }

    }

    private List<FileTreeNode> collectMultiPartitions(StorageItem item, String project, List<FileTreeNode> layouts)
            throws IOException {
        NDataflowManager manager = NDataflowManager.getInstance(kylinConfig, project);
        FileSystemDecorator fileSystemDecorator = item.getFileSystemDecorator();
        String itemPath = item.getPath();
        List<FileTreeNode> result = Lists.newArrayList();
        HashSet<String> cached = Sets.newHashSet();
        // Buckets do not certainly exist.
        // Only multi level partition model should do this.
        for (FileTreeNode node : layouts) {
            String dataflowId = node.getParent().getParent().getName(); // dataflow
            if (cached.contains(dataflowId)) {
                continue;
            }
            NDataflow dataflow = manager.getDataflow(dataflowId);
            if (Objects.nonNull(dataflow) //
                    && Objects.nonNull(dataflow.getModel()) //
                    && dataflow.getModel().isMultiPartitionModel()) {
                cached.add(dataflowId);
                result.addAll(Stream.of(fileSystemDecorator.listStatus(new Path(itemPath, node.getRelativePath())))
                        .filter(FileStatus::isDirectory) // Essential check in case of bad design.
                        .map(x -> new FileTreeNode(x.getPath().getName(), node)).collect(Collectors.toList()));
            } else {
                cached.add(dataflowId);
            }
        }
        return result;
    }

    @AllArgsConstructor
    public static class FileSystemDecorator {
        @NonNull
        private FileSystem fs;

        private static int retryTimes = 3;

        interface Action<T> {
            T run() throws IOException;
        }

        private <E> E sleepAndRetry(Action<E> action) throws IOException {
            rateLimiter.acquire();

            for (int i = 0; i < retryTimes - 1; i++) {
                try {
                    return action.run();
                } catch (FileNotFoundException e) {
                    throw e;
                } catch (Exception e) {
                    log.error("Failed to use fs api!", e);
                }

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    log.error("Failed to sleep!", ie);
                    Thread.currentThread().interrupt();
                }
            }

            return action.run();
        }

        public static FileSystemDecorator getInstance(FileSystem fs) {
            return new FileSystemDecorator(fs);
        }

        public FileStatus[] listStatus(Path f) throws IOException {
            return sleepAndRetry(() -> fs.listStatus(f));
        }

        public FileStatus[] listStatus(Path f, PathFilter filter) throws IOException {
            return sleepAndRetry(() -> fs.listStatus(f, filter));
        }

        public FileStatus getFileStatus(Path f) throws IOException {
            return sleepAndRetry(() -> fs.getFileStatus(f));
        }

        public boolean delete(Path f, boolean recursive) throws IOException {
            return sleepAndRetry(() -> fs.delete(f, recursive));
        }
    }

    @Data
    @RequiredArgsConstructor
    @AllArgsConstructor
    public static class StorageItem {

        @NonNull
        private FileSystemDecorator fileSystemDecorator;

        @NonNull
        private String path;

        /**
         * File hierarchy is
         *
         * /working_dir
         * |--/${project_name}
         *    |--/parquet
         *    |  +--/${dataflow_id}
         *    |     +--/${segment_id}
         *    |        +--/${layout_id}
         *    |           +--/${bucket_id} if multi level partition enabled.
         *    |        +--/${layout_id}_fast_bitmap  if enabled
         *    |--/job_tmp
         *    |  +--/${job_id}
         *    |--/table_exd
         *    |  +--/${table_identity}
         *    |--/dict/global_dict
         *    |  +--/${table_identity}
         *    |     +--/${column_name}
         *    |--/table_snapshot
         *    |  +--/${table_identity}
         *    |     +--/${snapshot_version}
         *    |--/flat_table
         *    |  +--/${dataflow_id}
         *    |     +--/${segment_id}
         */

        List<FileTreeNode> projectNodes = Lists.newArrayList();

        Map<String, ProjectFileTreeNode> projects = Maps.newHashMap();

        List<FileTreeNode> getAllNodes() {
            val allNodes = projects.values().stream().flatMap(p -> p.getAllCandidates().stream())
                    .flatMap(Collection::stream).collect(Collectors.toList());
            allNodes.addAll(projectNodes);
            return allNodes;
        }

        ProjectFileTreeNode getProject(String name) {
            return projects.getOrDefault(name, new ProjectFileTreeNode(name));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            StorageItem that = (StorageItem) o;
            return Objects.equals(fileSystemDecorator.fs, that.fileSystemDecorator.fs)
                    && Objects.equals(path, that.path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fileSystemDecorator.fs, path);
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @RequiredArgsConstructor
    public static class FileTreeNode {

        @NonNull
        String name;

        FileTreeNode parent;

        public String getRelativePath() {
            if (parent == null) {
                return name;
            }
            return parent.getRelativePath() + "/" + name;
        }
    }

    @Data
    @EqualsAndHashCode(callSuper = true)
    @ToString(onlyExplicitlyIncluded = true, callSuper = true)
    public static class ProjectFileTreeNode extends FileTreeNode {

        public ProjectFileTreeNode(String name) {
            super(name);
        }

        List<FileTreeNode> jobTmps = Lists.newLinkedList();

        List<FileTreeNode> tableExds = Lists.newLinkedList();

        List<FileTreeNode> globalDictTables = Lists.newLinkedList();

        List<FileTreeNode> globalDictColumns = Lists.newLinkedList();

        List<FileTreeNode> snapshotTables = Lists.newLinkedList();

        List<FileTreeNode> snapshots = Lists.newLinkedList();

        List<FileTreeNode> dataflows = Lists.newLinkedList();

        List<FileTreeNode> deltaDataFlows = Lists.newArrayList();

        List<FileTreeNode> deltaDataLayouts = Lists.newArrayList();

        List<FileTreeNode> segments = Lists.newLinkedList();

        List<FileTreeNode> layouts = Lists.newLinkedList();

        List<FileTreeNode> buckets = Lists.newLinkedList();

        List<FileTreeNode> dfFlatTables = Lists.newArrayList();

        List<FileTreeNode> segmentFlatTables = Lists.newArrayList();

        Collection<List<FileTreeNode>> getAllCandidates() {
            return Arrays.asList(jobTmps, tableExds, globalDictTables, globalDictColumns, snapshotTables, snapshots,
                    dataflows, deltaDataFlows, deltaDataLayouts, segments, layouts, buckets, dfFlatTables,
                    segmentFlatTables);
        }

    }

    public static class Stats {

        public final Set<StorageItem> allItems = Collections.synchronizedSet(new HashSet<>());
        public final Set<StorageItem> startItem = Collections.synchronizedSet(new HashSet<>());
        public final Set<StorageItem> successItems = Collections.synchronizedSet(new HashSet<>());
        public final Set<StorageItem> errorItems = Collections.synchronizedSet(new HashSet<>());

        private void reset() {
            allItems.clear();
            startItem.clear();
            successItems.clear();
            errorItems.clear();
        }

        void onAllStart(Set<StorageItem> outDatedItems) {
            // retry enters here too, reset everything first
            reset();

            log.debug("{} items to cleanup", outDatedItems.size());
            allItems.addAll(outDatedItems);
        }

        void onItemStart(StorageItem item) {
            heartBeat();
            startItem.add(item);
        }

        void onItemError(StorageItem item) {
            errorItems.add(item);
        }

        void onItemSuccess(StorageItem item) {
            successItems.add(item);
        }

        public void onRetry() {
            // for progress printing
        }

        public void heartBeat() {
            // for progress printing
        }

        public boolean hasError() {
            return !errorItems.isEmpty();
        }
    }

    class ProjectStorageCleaner {

        private final String project;

        private final Set<String> dependentFiles = Sets.newTreeSet();

        ProjectStorageCleaner(String project) {
            this.project = project;
        }

        public void execute() {
            collectJobTmp(project);
            collectDataflow(project);
            collectTable(project);

            for (StorageItem item : allFileSystems) {
                for (List<FileTreeNode> nodes : item.getProject(project).getAllCandidates()) {
                    for (FileTreeNode node : nodes) {
                        log.debug("find candidate /{}", node.getRelativePath());
                    }
                }
            }
            for (String dependentFile : dependentFiles) {
                log.debug("remove candidate {}", dependentFile);
            }
            removeDependentFiles();
        }

        private void removeDependentFiles() {
            for (StorageItem item : allFileSystems) {
                for (List<FileTreeNode> nodes : item.getProject(project).getAllCandidates()) {
                    // protect parent folder and
                    nodes.removeIf(
                            node -> dependentFiles.stream().anyMatch(df -> ("/" + node.getRelativePath()).startsWith(df)
                                    || df.startsWith("/" + node.getRelativePath())));
                }
            }
        }

        private void collectJobTmp(String project) {
            val config = KylinConfig.getInstanceFromEnv();
            val executableManager = ExecutableManager.getInstance(config, project);
            List<ExecutablePO> executablePOList = executableManager.getAllJobs();
            Set<String> activeJobs = executablePOList.stream().map(e -> project + JOB_TMP_ROOT + "/" + e.getId())
                    .collect(Collectors.toSet());
            for (StorageItem item : allFileSystems) {
                item.getProject(project).getJobTmps().removeIf(node -> activeJobs.contains(node.getRelativePath()));
            }
        }

        private void collectDataflow(String project) {
            val config = KylinConfig.getInstanceFromEnv();
            val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            val activeIndexDataPath = Sets.<String> newHashSet();
            val activeBucketDataPath = Sets.<String> newHashSet();
            val activeFastBitmapIndexDataPath = Sets.<String> newHashSet();
            val activeSegmentFlatTableDataPath = Sets.<String> newHashSet();
            val dataflows = NDataflowManager.getInstance(config, project).listAllDataflows().stream()
                    .map(RootPersistentEntity::getId).collect(Collectors.toSet());
            val deltaDataFlow = NDataflowManager.getInstance(config, project).listAllDataflows().stream()
                    .filter(df -> df.getModel().getStorageType().isDeltaStorage())
                    .map(RootPersistentEntity::getId).collect(Collectors.toSet());
            val activeDeltaLayoutData = NDataflowManager.getInstance(config, project).listAllDataflows().stream()
                    .flatMap(df -> df.listAllLayoutDetails().stream().map(NDataLayoutDetails::getRelativeStoragePath))
                    .collect(Collectors.toSet());
            // set activeSegmentFlatTableDataPath, by iterating segments
            dataflowManager.listAllDataflows().forEach(df -> df.getSegments().stream() //
                    .map(segment -> getSegmentFlatTableDir(project, segment))
                    .forEach(activeSegmentFlatTableDataPath::add));
            //set activeIndexDataPath
            dataflowManager.listAllDataflows().forEach(dataflow -> dataflow.getSegments().stream() //
                    .flatMap(segment -> segment.getLayoutsMap().values().stream()) //
                    .forEach(layout -> {
                        activeIndexDataPath.add(getDataLayoutDir(layout));
                        layout.getMultiPartition().forEach(partition -> //
                        activeBucketDataPath.add(getDataPartitionDir(layout, partition)));
                    }));
            activeIndexDataPath
                    .forEach(path -> activeFastBitmapIndexDataPath.add(path + HadoopUtil.FAST_BITMAP_SUFFIX));
            val activeSegmentPath = activeIndexDataPath.stream().map(s -> new File(s).getParent())
                    .collect(Collectors.toSet());
            for (StorageCleaner.StorageItem item : allFileSystems) {
                item.getProject(project).getDataflows().removeIf(node -> dataflows.contains(node.getName()));
                item.getProject(project).getDeltaDataFlows().removeIf(node -> deltaDataFlow.contains(node.getName()));
                item.getProject(project).getDeltaDataLayouts().removeIf(node ->
                        activeDeltaLayoutData.contains(node.getRelativePath()));
                item.getProject(project).getSegments()
                        .removeIf(node -> activeSegmentPath.contains(node.getRelativePath()));
                item.getProject(project).getLayouts()
                        .removeIf(node -> activeIndexDataPath.contains(node.getRelativePath())
                                || activeFastBitmapIndexDataPath.contains(node.getRelativePath()));
                item.getProject(project).getBuckets()
                        .removeIf(node -> activeBucketDataPath.contains(node.getRelativePath()));
                item.getProject(project).getDfFlatTables().removeIf(node -> dataflows.contains(node.getName()));
                item.getProject(project).getSegmentFlatTables()
                        .removeIf(node -> activeSegmentFlatTableDataPath.contains(node.getRelativePath()));
            }
        }

        private void collectTable(String project) {
            val config = KylinConfig.getInstanceFromEnv();
            val tableManager = NTableMetadataManager.getInstance(config, project);
            val activeDictDir = Sets.<String> newHashSet();
            val activeTableExdDir = Sets.<String> newHashSet();
            val activeDictTableDir = Sets.<String> newHashSet();
            val activeSnapshotTableDir = Sets.<String> newHashSet();
            val activeSnapshotDir = Sets.<String> newHashSet();
            tableManager.listAllTables().forEach(table -> {
                Arrays.stream(table.getColumns())
                        .map(column -> getDictDir(project) + "/" + table.getIdentity() + "/" + column.getName())
                        .forEach(activeDictDir::add);
                activeTableExdDir.add(project + ResourceStore.TABLE_EXD_RESOURCE_ROOT + "/" + table.getIdentity());
                activeSnapshotTableDir.add(project + SNAPSHOT_STORAGE_ROOT + "/" + table.getIdentity());
                if (table.getLastSnapshotPath() != null) {
                    activeSnapshotDir.add(table.getLastSnapshotPath());
                }
                activeDictTableDir.add(getDictDir(project) + "/" + table.getIdentity());
            });

            for (StorageCleaner.StorageItem item : allFileSystems) {
                item.getProject(project).getGlobalDictTables()
                        .removeIf(node -> activeDictTableDir.contains(node.getRelativePath()));
                item.getProject(project).getGlobalDictColumns()
                        .removeIf(node -> activeDictDir.contains(node.getRelativePath()));
                item.getProject(project).getSnapshots()
                        .removeIf(node -> activeSnapshotDir.contains(node.getRelativePath()));
                item.getProject(project).getSnapshotTables()
                        .removeIf(node -> activeSnapshotTableDir.contains(node.getRelativePath()));
                item.getProject(project).getTableExds()
                        .removeIf(node -> activeTableExdDir.contains(node.getRelativePath()));
            }
        }
    }

    class ProjectTemporaryTableCleaner {
        private final String project;
        private CliCommandExecutor cliCommandExecutor;
        private ProjectTemporaryTableCleanerHelper tableCleanerHelper;

        ProjectTemporaryTableCleaner(String project) {
            this.project = project;
            this.cliCommandExecutor = new CliCommandExecutor();
            this.tableCleanerHelper = new ProjectTemporaryTableCleanerHelper();
        }

        public void execute() {
            List<FileTreeNode> jobTemps = allFileSystems.iterator().next().getProject(project).getJobTmps();
            doExecuteCmd(collectDropTemporaryTransactionTable(jobTemps));
        }

        private void doExecuteCmd(String cmd) {
            try {
                CliCmdExecResult executeResult = cliCommandExecutor.execute(cmd, null);
                if (executeResult.getCode() != 0) {
                    log.error("execute drop intermediate table return fail, cmd : " + cmd);
                } else {
                    log.info("execute drop intermediate table succeeded, cmd: " + cmd);
                }
            } catch (ShellException e) {
                log.error("execute drop intermediate table error, cmd : " + cmd, e);
            }
        }

        public String collectDropTemporaryTransactionTable(List<FileTreeNode> jobTemps) {
            String result = "";
            try {
                KylinConfig config = KylinConfig.getInstanceFromEnv();
                FileSystem fs = HadoopUtil.getWorkingFileSystem();
                Set<String> jobTempTables = jobTemps.stream().filter(node -> !node.getName().endsWith(".zip"))
                        .map(node -> tableCleanerHelper.getJobTransactionalTable(project, node.getName(), fs))
                        .flatMap(Collection::stream).collect(Collectors.toSet());
                List<ExecutablePO> discardedExecutablePOs = ExecutableManager.getInstance(config, project)
                        .getExecutablePOsByStatus(Lists.newArrayList(ExecutableState.DISCARDED));
                ExecutableManager executableManager = ExecutableManager.getInstance(config, project);
                Set<String> discardTempTables = discardedExecutablePOs.stream()
                        .map(executablePO -> executableManager.fromPO(executablePO))
                        .map(e -> tableCleanerHelper.getJobTransactionalTable(project, e.getId(), fs))
                        .flatMap(Collection::stream).collect(Collectors.toSet());
                jobTempTables.addAll(discardTempTables);

                if (CollectionUtils.isNotEmpty(jobTempTables) && config.isReadTransactionalTableEnabled()) {
                    result = tableCleanerHelper.getDropTmpTableCmd(project, jobTempTables);
                }
            } catch (Exception exception) {
                log.error("Failed to delete temporary tables.", exception);
            }
            log.info("collectDropTemporaryTransactionTable end.");
            return result;
        }
    }

    /**
     * Sparder history dir hierarchy is
     *
     * /${kylin.storage.columnar.spark-conf.spark.eventLog.dir}
     * |--/${hostName_port}
     *    |--/{eventlog_v2_${appid}#${timestamp}}
     *    |  +--/${events_${fileindex}_${appid}_${starttime}_${endtime}}
     */

    @Slf4j
    public static class EventLogCleaner {
        private static final KylinConfig KYLIN_CONFIG = KylinConfig.getInstanceFromEnv();
        private static final FileSystem fs = HadoopUtil.getWorkingFileSystem();
        private static final String FIRST_EVENT_LOG_FILE_PREFIX = "events_1_";
        private static final String SPARK_EVENTLOG_DIR = "spark.eventLog.dir";
        private long queryExpirationTime;
        private long buildExpirationTime;

        private void init() {
            long eventLogCleanStartTime = System.currentTimeMillis();
            Long minQueryHistoryTime = QueryHisStoreUtil.getQueryHistoryMinQueryTime();
            long cleanTime = eventLogCleanStartTime - KYLIN_CONFIG.getQueryHistorySurvivalThreshold();
            queryExpirationTime = Objects.isNull(minQueryHistoryTime) ? cleanTime
                    : Math.min(cleanTime, minQueryHistoryTime);
            log.info(
                    "eventLogCleanStartTime is {}, queryHistorySurvivalThreshold is {}ms, minQueryHistoryTime is {}, queryExpirationTime is {}",
                    eventLogCleanStartTime, KYLIN_CONFIG.getQueryHistorySurvivalThreshold(), minQueryHistoryTime,
                    queryExpirationTime);

            JobInfoDao jobInfoDao = JobContextUtil.getJobInfoDao(KYLIN_CONFIG);
            Long earliest = jobInfoDao.getEarliestJobCreateTime();
            if (null == earliest) {
                earliest = Long.MAX_VALUE;
            }

            buildExpirationTime = Math.min(eventLogCleanStartTime - KYLIN_CONFIG.getExecutableSurvivalTimeThreshold(),
                    earliest);
            log.info(
                    "eventLogCleanStartTime is {}, executableSurvivalTimeThreshold is {}ms, earliest executable's createTime is {}, buildExpirationTime is {}",
                    eventLogCleanStartTime, KYLIN_CONFIG.getExecutableSurvivalTimeThreshold(), earliest,
                    buildExpirationTime);
        }

        public EventLogCleaner() {
            init();
        }

        public void cleanCurrentSparderEventLog() {
            log.info("Start to clean sparder event log");
            String currentSparderEvenLogDir = ExtractFactory.create().getSparderEvenLogDir();
            clean(currentSparderEvenLogDir, queryExpirationTime);
            log.info("End to clean sparder event log");
        }

        private void cleanSparkEventLogs() {
            log.info("Start to clean spark event log");
            String allSparkEventLogDir = KYLIN_CONFIG.getSparkConfigOverride().get(SPARK_EVENTLOG_DIR).trim();
            clean(allSparkEventLogDir, buildExpirationTime);

            NProjectManager prjManager = NProjectManager.getInstance(KYLIN_CONFIG);
            prjManager.listAllProjects().forEach(project -> {
                String sparkEventLogDir = project.getConfig().getExtendedOverrides()
                        .get("kylin.engine.spark-conf.spark.eventLog.dir");
                if (!StringUtils.isEmpty(sparkEventLogDir)) {
                    clean(sparkEventLogDir, buildExpirationTime);
                }
            });
            log.info("End to clean spark event log");
        }

        public void cleanSparkEventLogs(String projectName) {
            log.info("Start to clean spark event log for project {}", projectName);
            String allSparkEventLogDir = KYLIN_CONFIG.getSparkConfigOverride().get(SPARK_EVENTLOG_DIR).trim();
            clean(allSparkEventLogDir, buildExpirationTime);

            NProjectManager prjManager = NProjectManager.getInstance(KYLIN_CONFIG);
            ProjectInstance project = prjManager.getProject(projectName);
            String sparkEventLogDir = project.getConfig().getExtendedOverrides()
                    .get("kylin.engine.spark-conf.spark.eventLog.dir");
            if (!StringUtils.isEmpty(sparkEventLogDir)) {
                clean(sparkEventLogDir, buildExpirationTime);
            }

            log.info("End to clean spark event log for project {}", projectName);
        }

        // for RoutineTool & FastRoutineTool
        public void cleanAllEventLog() {
            log.info("Start to clean all event log");
            String rootSparderEvenLogDir = KapConfig.wrap(KYLIN_CONFIG).getSparkConf().get(SPARK_EVENTLOG_DIR);
            try {
                Arrays.stream(fs.listStatus(new Path(rootSparderEvenLogDir)))
                        .forEach(fileStatus -> clean(fileStatus.getPath().toString(), queryExpirationTime));
            } catch (IOException e) {
                log.warn("Failed to clean all sparder event log of [{}]", rootSparderEvenLogDir, e);
            }

            cleanSparkEventLogs();
            log.info("End to clean all event log");
        }

        private void clean(String dir, long expirationTime) {
            Path path = new Path(dir);
            try {
                FileStatus[] fileStatuses = fs.listStatus(path);
                for (FileStatus fileStatus : fileStatuses) {
                    deleteEventLogFile(fileStatus, expirationTime);
                }
            } catch (Exception e) {
                log.warn("Failed to clean the event log of [{}]", path.getName(), e);
            }
        }

        private void deleteEventLogFile(FileStatus fileStatus, long expirationTime) throws IOException {
            if (fileStatus.getModificationTime() <= expirationTime
                    && !fileStatus.getPath().getName().startsWith(FIRST_EVENT_LOG_FILE_PREFIX)) {
                log.info("Delete event log file: {}", fileStatus.getPath().toString());
                fs.delete(fileStatus.getPath(), true);
                return;
            }

            if (fileStatus.isFile()) {
                return;
            }

            clean(fileStatus.getPath().toString(), expirationTime);
        }
    }
}
