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

package org.apache.kylin.tool.upgrade;

import static org.apache.kylin.job.execution.AbstractExecutable.DEPENDENT_FILES;
import static org.apache.kylin.metadata.cube.model.NBatchConstants.P_DIST_META_URL;
import static org.apache.kylin.metadata.cube.model.NBatchConstants.P_OUTPUT_META_URL;
import static org.apache.kylin.metadata.cube.model.NBatchConstants.P_PROJECT_NAME;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.job.dao.ExecutableOutputPO;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.dao.JobStatistics;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.dao.NExecutableDao;
import org.apache.kylin.metadata.MetadataConstants;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.common.util.OptionBuilder;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NDataLoadingRange;
import org.apache.kylin.metadata.cube.model.NDataLoadingRangeManager;
import org.apache.kylin.metadata.cube.model.NDataSegDetails;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.favorite.QueryHistoryTimeOffset;
import org.apache.kylin.metadata.favorite.QueryHistoryTimeOffsetManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.project.NProjectManager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RenameProjectResourceTool extends ExecutableApplication {

    private static final Option OPTION_DIR = OptionBuilder.getInstance().hasArg().withArgName("dir")
            .withDescription("Specify the directory to operator").isRequired(true).create("dir");
    private static final Option OPTION_PROJECT = OptionBuilder.getInstance().hasArg().withArgName("project_name")
            .withDescription("Specify project (optional)").isRequired(false).withLongOpt("project").create("p");
    private static final Option OPTION_COLLECT_ONLY = OptionBuilder.getInstance().hasArg().withArgName("true/false")
            .withDescription("collect only, show rename resource.(default true)").isRequired(false)
            .withLongOpt("collect-only").create("collect");
    private static final Option OPTION_HELP = OptionBuilder.getInstance().hasArg(false)
            .withDescription("print help message.").isRequired(false).withLongOpt("help").create("h");
    private Set<String> projects = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    private boolean collectOnly = true;
    private Set<String> existsProjectNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    private Map<String, String> renameProjectMap = new HashMap<>();
    private KylinConfig fileSystemConfig = KylinConfig.getInstanceFromEnv();

    private KylinConfig config = KylinConfig.newKylinConfig();

    private ResourceStore resourceStore;

    public static void main(String[] args) {
        val tool = new RenameProjectResourceTool();
        tool.execute(args);
        System.out.println("Rename project resource finished.");
        Unsafe.systemExit(0);
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_DIR);
        options.addOption(OPTION_PROJECT);
        options.addOption(OPTION_COLLECT_ONLY);
        options.addOption(OPTION_HELP);
        return options;
    }

    private boolean printUsage(OptionsHelper optionsHelper) {
        boolean help = optionsHelper.hasOption(OPTION_HELP);
        if (help) {
            optionsHelper.printUsage(this.getClass().getName(), getOptions());
        }
        return help;
    }

    private void initOptionValues(OptionsHelper optionsHelper) {
        while (true) {
            System.out.println(
                    "This script will help you modify the duplicate project names. The system will add a number to the project name created later, for example, project_a-> project_a1\n"
                            + "Please confirm if you need to execute the script？(y/n)");
            Scanner scanner = new Scanner(System.in, Charset.defaultCharset().name());

            String prompt = scanner.nextLine();

            if (StringUtils.equals("y", prompt)) {
                break;
            }

            if (StringUtils.equals("n", prompt)) {
                Unsafe.systemExit(0);
            }
        }

        if (optionsHelper.hasOption(OPTION_PROJECT)) {
            projects.addAll(Arrays.asList(optionsHelper.getOptionValue(OPTION_PROJECT).split(",")));
        }

        if (optionsHelper.hasOption(OPTION_COLLECT_ONLY)) {
            collectOnly = Boolean.parseBoolean(optionsHelper.getOptionValue(OPTION_COLLECT_ONLY));
        }

        String metadataUrl = getMetadataUrl(optionsHelper.getOptionValue(OPTION_DIR));

        fileSystemConfig.setMetadataUrl(metadataUrl);

        resourceStore = ResourceStore.getKylinMetaStore(fileSystemConfig);

        List<ProjectInstance> allProjectInstanceList = NProjectManager.getInstance(fileSystemConfig).listAllProjects();
        existsProjectNames
                .addAll(allProjectInstanceList.stream().map(ProjectInstance::getName).collect(Collectors.toList()));
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        if (printUsage(optionsHelper)) {
            return;
        }

        initOptionValues(optionsHelper);

        if (optionsHelper.hasOption(OPTION_PROJECT)) {
            String originProjectName = optionsHelper.getOptionValue(OPTION_PROJECT);
            String destProjectName;
            int index = originProjectName.indexOf(':');
            if (index > 0) {
                destProjectName = originProjectName.substring(index + 1);
                originProjectName = originProjectName.substring(0, index);
            } else {
                destProjectName = generateAvailableProjectName(originProjectName);
            }

            renameProjectMap.put(originProjectName, destProjectName);

            NProjectManager projectManager = NProjectManager.getInstance(fileSystemConfig);
            ProjectInstance projectInstance = projectManager.getProject(originProjectName);
            if (projectInstance == null) {
                System.out.printf(Locale.ROOT, "project %s does not exists%n", originProjectName);
                Unsafe.systemExit(1);
            }

        } else {
            collectDuplicateProject();
        }

        List<RenameEntity> renameEntities = new ArrayList<>();

        for (Map.Entry<String, String> entry : renameProjectMap.entrySet()) {
            renameEntities.addAll(renameProject(entry.getKey(), entry.getValue()));
        }

        for (RenameEntity renameEntity : renameEntities) {
            System.out.println(renameEntity);
        }

        if (!collectOnly) {
            for (RenameEntity renameEntity : renameEntities) {
                renameEntity.updateMetadata();
            }

            for (Map.Entry<String, String> entry : renameProjectMap.entrySet()) {
                updateHDFSMetadata(entry.getKey(), entry.getValue());
            }
        }
    }

    private void collectDuplicateProject() {
        List<ProjectInstance> allProjectInstanceList = NProjectManager.getInstance(fileSystemConfig).listAllProjects();

        ConcurrentSkipListMap<String, List<ProjectInstance>> duplicateProjectNameProjectMap = allProjectInstanceList
                .stream().filter(projectInstance -> projects.isEmpty() || projects.contains(projectInstance.getName()))
                .collect(Collectors.groupingByConcurrent(ProjectInstance::getName,
                        () -> new ConcurrentSkipListMap<>(String.CASE_INSENSITIVE_ORDER), Collectors.toList()));

        for (Map.Entry<String, List<ProjectInstance>> entry : duplicateProjectNameProjectMap.entrySet()) {
            List<ProjectInstance> projectInstances = entry.getValue().stream()
                    .sorted(Comparator.comparingLong(ProjectInstance::getCreateTime)).collect(Collectors.toList());

            if (projectInstances.size() == 1) {
                continue;
            }

            for (int i = 1; i < projectInstances.size(); i++) {
                ProjectInstance projectInstance = projectInstances.get(i);
                renameProjectMap.put(projectInstance.getName(),
                        generateAvailableProjectName(projectInstance.getName()));
            }
        }
    }

    private List<RenameEntity> renameProject(String originProjectName, String destProjectName) {
        List<RenameEntity> results = new ArrayList<>();

        // dataflow
        results.addAll(updateDataflow(originProjectName, destProjectName));

        // execute
        results.addAll(updateExecute(originProjectName, destProjectName));

        // index_plan
        results.addAll(updateIndexPlan(originProjectName, destProjectName));

        // job_stats
        results.addAll(updateJobStat(originProjectName, destProjectName));

        // model_desc
        results.addAll(updateModelDesc(originProjectName, destProjectName));

        // favorite rule
        results.addAll(updateFavoriteRule(originProjectName, destProjectName));

        // data loading
        results.addAll(updateDataLoadingRange(originProjectName, destProjectName));

        // query_history_time_offset
        results.addAll(updateQueryHistoryTimeOffset(originProjectName, destProjectName));

        // table table_exd
        results.addAll(updateTable(originProjectName, destProjectName));

        // user acl
        results.addAll(updateUserGroupAcl(originProjectName, destProjectName));

        // saved queries
        results.addAll(updateSavedQueries(originProjectName, destProjectName));

        // project
        results.add(updateProject(originProjectName, destProjectName));
        return results;
    }

    /**
     * @param originProjectName
     * @param destProjectName
     * @return
     */
    private List<RenameEntity> updateDataflow(String originProjectName, String destProjectName) {
        List<RenameEntity> results = new ArrayList<>();
        NDataflowManager dataflowManager = NDataflowManager.getInstance(fileSystemConfig, originProjectName);
        List<NDataflow> dataflows = dataflowManager.listAllDataflows(true);
        for (NDataflow dataflow : dataflows) {
            dataflow = dataflowManager.copy(dataflow);
            String srcResourcePath = dataflow.getResourcePath();
            dataflow.setProject(destProjectName);
            String destResourcePath = dataflow.getResourcePath();
            results.add(new RenameEntity(srcResourcePath, destResourcePath, dataflow, NDataflow.class));

            // dataflow_details
            NavigableSet<String> daflowDetails = resourceStore.listResources(String.format(Locale.ROOT, "/%s%s/%s",
                    originProjectName, NDataSegDetails.DATAFLOW_DETAILS_RESOURCE_ROOT, dataflow.getUuid()));

            if (daflowDetails != null) {
                for (String daflowDetail : daflowDetails) {
                    results.add(new RenameEntity(daflowDetail,
                            daflowDetail.replace(String.format(Locale.ROOT, "/%s/", originProjectName),
                                    String.format(Locale.ROOT, "/%s/", destProjectName))));
                }
            }
        }
        return results;
    }

    /**
     * @param originProjectName
     * @param destProjectName
     * @return
     */
    private List<RenameEntity> updateIndexPlan(String originProjectName, String destProjectName) {
        List<RenameEntity> results = new ArrayList<>();
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(fileSystemConfig, originProjectName);
        List<IndexPlan> indexPlans = indexPlanManager.listAllIndexPlans(true);
        for (IndexPlan indexPlan : indexPlans) {
            String srcResourcePath = indexPlan.getResourcePath();
            indexPlan.setProject(destProjectName);
            String destResourcePath = IndexPlan.concatResourcePath(indexPlan.resourceName(), destProjectName);
            results.add(new RenameEntity(srcResourcePath, destResourcePath));
        }

        return results;
    }

    /**
     * @param originProjectName
     * @param destProjectName
     * @return
     */
    private List<RenameEntity> updateJobStat(String originProjectName, String destProjectName) {
        List<RenameEntity> results = new ArrayList<>();
        JobStatisticsManager jobStatisticsManager = JobStatisticsManager.getInstance(fileSystemConfig,
                originProjectName);
        List<JobStatistics> jobStatistics = jobStatisticsManager.getAll();
        for (JobStatistics jobStatistic : jobStatistics) {
            String srcResourcePath = "/" + originProjectName + ResourceStore.JOB_STATISTICS + "/"
                    + jobStatistic.resourceName() + MetadataConstants.FILE_SURFIX;
            String destResourcePath = "/" + destProjectName + ResourceStore.JOB_STATISTICS + "/"
                    + jobStatistic.resourceName() + MetadataConstants.FILE_SURFIX;

            results.add(new RenameEntity(srcResourcePath, destResourcePath));
        }
        return results;
    }

    /**
     * @param originProjectName
     * @param destProjectName
     * @return
     */
    private List<RenameEntity> updateModelDesc(String originProjectName, String destProjectName) {
        List<RenameEntity> results = new ArrayList<>();

        NDataModelManager dataModelManager = NDataModelManager.getInstance(fileSystemConfig, originProjectName);
        List<NDataModel> dataModels = dataModelManager.listAllModels();
        for (NDataModel dataModel : dataModels) {
            String srcResourcePath = NDataModel.concatResourcePath(dataModel.resourceName(), originProjectName);
            String destResourcePath = NDataModel.concatResourcePath(dataModel.resourceName(), destProjectName);
            results.add(new RenameEntity(srcResourcePath, destResourcePath));
        }

        return results;
    }

    /**
     * @param originProjectName
     * @param destProjectName
     * @return
     */
    private List<RenameEntity> updateFavoriteRule(String originProjectName, String destProjectName) {
        List<RenameEntity> results = new ArrayList<>();

        NavigableSet<String> favoriteRulePaths = resourceStore
                .listResources("/" + originProjectName + ResourceStore.QUERY_FILTER_RULE_RESOURCE_ROOT);

        if (favoriteRulePaths != null) {
            for (String favoriteRulePath : favoriteRulePaths) {

                String destFavoriteRulePath = favoriteRulePath.replace("/" + originProjectName + "/",
                        "/" + destProjectName + "/");
                results.add(new RenameEntity(favoriteRulePath, destFavoriteRulePath));
            }
        }
        return results;
    }

    /**
     * @param originProjectName
     * @param destProjectName
     * @return
     */
    private List<RenameEntity> updateDataLoadingRange(String originProjectName, String destProjectName) {
        List<RenameEntity> results = new ArrayList<>();

        NDataLoadingRangeManager dataLoadingRange = NDataLoadingRangeManager.getInstance(fileSystemConfig,
                originProjectName);

        List<NDataLoadingRange> dataLoadingRanges = dataLoadingRange.getDataLoadingRanges();
        for (NDataLoadingRange loadingRange : dataLoadingRanges) {
            String srcResourcePath = loadingRange.getResourcePath();
            String destResourcePath = srcResourcePath.replace("/" + originProjectName + "/",
                    "/" + destProjectName + "/");
            results.add(new RenameEntity(srcResourcePath, destResourcePath));
        }
        return results;
    }

    /**
     * @param originProjectName
     * @param destProjectName
     * @return
     */
    private List<RenameEntity> updateQueryHistoryTimeOffset(String originProjectName, String destProjectName) {
        List<RenameEntity> results = new ArrayList<>();

        QueryHistoryTimeOffsetManager queryHistoryTimeOffsetManager = QueryHistoryTimeOffsetManager
                .getInstance(fileSystemConfig, originProjectName);

        QueryHistoryTimeOffset queryHistoryTimeOffset = queryHistoryTimeOffsetManager.get();

        String srcQueryHistoryTimeOffsetPath = "/" + originProjectName + ResourceStore.QUERY_HISTORY_TIME_OFFSET + "/"
                + queryHistoryTimeOffset.getUuid() + MetadataConstants.FILE_SURFIX;

        String destQueryHistoryTimeOffsetPath = "/" + destProjectName + ResourceStore.QUERY_HISTORY_TIME_OFFSET + "/"
                + queryHistoryTimeOffset.getUuid() + MetadataConstants.FILE_SURFIX;

        results.add(new RenameEntity(srcQueryHistoryTimeOffsetPath, destQueryHistoryTimeOffsetPath));

        return results;
    }

    /**
     * @param originProjectName
     * @param destProjectName
     * @return
     */
    private List<RenameEntity> updateTable(String originProjectName, String destProjectName) {
        List<RenameEntity> results = new ArrayList<>();

        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(fileSystemConfig,
                originProjectName);
        List<TableDesc> tables = tableMetadataManager.listAllTables();
        for (TableDesc table : tables) {
            String srcTableResourcePath = table.getResourcePath();
            table.init(destProjectName);
            String destTableResourcePath = table.getResourcePath();
            String lastSnapshotPath = table.getLastSnapshotPath();
            if (lastSnapshotPath != null) {
                table.setLastSnapshotPath(lastSnapshotPath.replace(originProjectName + "/", destProjectName + "/"));
                results.add(new RenameEntity(srcTableResourcePath, destTableResourcePath, table, TableDesc.class));
            } else {
                results.add(new RenameEntity(srcTableResourcePath, destTableResourcePath));
            }

            TableExtDesc tableExtDesc = tableMetadataManager.getTableExtIfExists(table);
            if (tableExtDesc != null) {
                String srcTableExtResourcePath = tableExtDesc.getResourcePath();
                tableExtDesc.init(destProjectName);
                String destTableExtResourcePath = tableExtDesc.getResourcePath();
                results.add(new RenameEntity(srcTableExtResourcePath, destTableExtResourcePath));
            }
        }

        return results;
    }

    /**
     * @param originProjectName
     * @param destProjectName
     * @return
     */
    private List<RenameEntity> updateUserGroupAcl(String originProjectName, String destProjectName) {
        List<RenameEntity> results = new ArrayList<>();
        NavigableSet<String> userAclPaths = resourceStore
                .listResources(String.format(Locale.ROOT, "/%s/acl/user", originProjectName));
        if (userAclPaths != null) {
            for (String userAclPath : userAclPaths) {
                String destUserAclPath = userAclPath.replace("/" + originProjectName + "/",
                        "/" + destProjectName + "/");
                results.add(new RenameEntity(userAclPath, destUserAclPath));
            }
        }

        NavigableSet<String> groupAclPaths = resourceStore
                .listResources(String.format(Locale.ROOT, "/%s/acl/group", originProjectName));
        if (groupAclPaths != null) {
            for (String groupAclPath : groupAclPaths) {
                String destGroupAclPath = groupAclPath.replace("/" + originProjectName + "/",
                        "/" + destProjectName + "/");
                results.add(new RenameEntity(groupAclPath, destGroupAclPath));
            }
        }

        return results;
    }

    /**
     * @param originProjectName
     * @param destProjectName
     * @return
     */
    private List<RenameEntity> updateSavedQueries(String originProjectName, String destProjectName) {
        List<RenameEntity> results = new ArrayList<>();
        NavigableSet<String> savedQueries = resourceStore.listResources("/" + originProjectName + "/query/");
        if (savedQueries == null) {
            return results;
        }
        for (String savedQuery : savedQueries) {
            RawResource rs = resourceStore.getResource(savedQuery);
            if (rs != null) {
                String destResourcePath = savedQuery.replace("/" + originProjectName + "/",
                        "/" + destProjectName + "/");
                try (InputStream is = rs.getByteSource().openStream()) {
                    JsonNode savedQueryNode = JsonUtil.readValue(is, JsonNode.class);
                    if (savedQueryNode.has("queries")) {
                        ArrayNode queries = (ArrayNode) savedQueryNode.get("queries");
                        for (JsonNode query : queries) {
                            String projectFiledName = "project";
                            if (query.has(projectFiledName)) {
                                ((ObjectNode) query).put(projectFiledName, destProjectName);
                            }
                        }
                    }

                    ByteArrayOutputStream buf = new ByteArrayOutputStream();
                    DataOutputStream dout = new DataOutputStream(buf);
                    JsonUtil.writeValue(dout, savedQueryNode);
                    dout.close();
                    buf.close();

                    ByteSource byteSource = ByteSource.wrap(buf.toByteArray());

                    rs = new RawResource(destResourcePath, byteSource, System.currentTimeMillis(), rs.getMvcc());
                } catch (IOException e) {
                    log.warn("read resource {} failed", savedQuery);
                }
                results.add(new RenameEntity(savedQuery, destResourcePath, rs));
            }
        }
        return results;
    }

    /**
     * @param originProjectName
     * @param destProjectName
     * @return
     */
    private List<RenameEntity> updateExecute(String originProjectName, String destProjectName) {
        List<RenameEntity> results = new ArrayList<>();
        NExecutableDao executableDao = NExecutableDao.getInstance(fileSystemConfig, originProjectName);
        List<ExecutablePO> jobs = executableDao.getJobs();
        for (ExecutablePO job : jobs) {
            String srcResourcePath = job.getResourcePath();
            job.setProject(destProjectName);
            String destResourcePath = job.getResourcePath();

            Map<String, String> params = job.getParams();
            if (params.get(P_PROJECT_NAME) != null) {
                params.put(P_PROJECT_NAME, destProjectName);
            }
            List<ExecutablePO> tasks = job.getTasks();
            for (ExecutablePO task : tasks) {
                Map<String, String> taskParams = task.getParams();
                if (taskParams.get(P_PROJECT_NAME) != null) {
                    taskParams.put(P_PROJECT_NAME, destProjectName);
                }

                String distMetaUrl = taskParams.get(P_DIST_META_URL);
                if (distMetaUrl != null) {
                    distMetaUrl = distMetaUrl.replace(String.format(Locale.ROOT, "/%s/", originProjectName),
                            String.format(Locale.ROOT, "/%s/", destProjectName));
                    taskParams.put(P_DIST_META_URL, distMetaUrl);
                }

                String outputMetaUrl = taskParams.get(P_OUTPUT_META_URL);
                if (outputMetaUrl != null) {
                    outputMetaUrl = outputMetaUrl.replace(String.format(Locale.ROOT, "/%s/", originProjectName),
                            String.format(Locale.ROOT, "/%s/", destProjectName));
                    taskParams.put(P_OUTPUT_META_URL, outputMetaUrl);
                }
            }

            ExecutableOutputPO jobOutput = job.getOutput();
            if (jobOutput != null) {
                Map<String, String> info = jobOutput.getInfo();
                String dependentFiles = info.get(DEPENDENT_FILES);
                if (dependentFiles != null) {
                    dependentFiles = dependentFiles.replace(String.format(Locale.ROOT, "/%s/", originProjectName),
                            String.format(Locale.ROOT, "/%s/", destProjectName));
                    info.put(DEPENDENT_FILES, dependentFiles);
                }
            }
            results.add(new RenameEntity(srcResourcePath, destResourcePath, job, ExecutablePO.class));
        }
        return results;
    }

    /**
     * @param originProjectName
     * @param destProjectName
     * @return
     */
    public RenameEntity updateProject(String originProjectName, String destProjectName) {
        NProjectManager projectManager = NProjectManager.getInstance(fileSystemConfig);
        ProjectInstance projectInstance = projectManager.getProject(originProjectName);
        String srcResourcePath = projectInstance.getResourcePath();

        projectInstance.setName(destProjectName);
        String destResourcePath = projectInstance.getResourcePath();

        return new RenameEntity(srcResourcePath, destResourcePath, projectInstance, ProjectInstance.class);
    }

    /**
     * rename originPath to destPath
     *
     * @param originPath
     * @param destPath
     * @throws Exception
     */
    private void updateHDFSMetadata(String originPath, String destPath) throws Exception {
        String hdfsWorkingDirectory = config.getHdfsWorkingDirectory();
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        Path src = new Path(hdfsWorkingDirectory, originPath);
        if (!fs.exists(src)) {
            System.out.printf(Locale.ROOT, "src file %s not exists%n", src);
            return;
        }
        Path dst = new Path(hdfsWorkingDirectory, destPath);
        if (fs.exists(dst)) {
            System.out.printf(Locale.ROOT, "dst file %s already exists%n", dst);
            return;
        }

        System.out.printf(Locale.ROOT, "move file from  %s to %s%n", src, dst);
        boolean success = HadoopUtil.getWorkingFileSystem().rename(src, dst);
        if (!success) {
            System.out.printf(Locale.ROOT, "move file from  %s to %s failed%n", src, dst);
            Unsafe.systemExit(1);
        }
    }

    private String generateAvailableProjectName(String originProjectName) {
        if (renameProjectMap.get(originProjectName) != null) {
            return renameProjectMap.get(originProjectName);
        }

        String destName = generateAvailableResourceName(originProjectName, existsProjectNames);
        existsProjectNames.add(destName);
        renameProjectMap.put(originProjectName, destName);
        return destName;
    }

    private String generateAvailableResourceName(String originName, Set<String> existsResourceNames) {
        int suffix = 1;
        while (true) {
            String destName = String.format(Locale.ROOT, "%s%s", originName, suffix);
            if (!existsResourceNames.contains(destName)) {
                return destName;
            }
            suffix++;
        }
    }

    private String getMetadataUrl(String rootPath) {
        if (rootPath.startsWith("file://")) {
            rootPath = rootPath.replace("file://", "");
        }
        return StringUtils.appendIfMissing(rootPath, "/");
    }
}
