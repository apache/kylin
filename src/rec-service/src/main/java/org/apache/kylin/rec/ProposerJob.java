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

package org.apache.kylin.rec;

import static org.apache.kylin.common.KylinExternalConfigLoader.KYLIN_CONF_PROPERTIES_FILE;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.exception.ServerErrorCode;
import org.apache.kylin.common.persistence.MetadataType;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.OptionBuilder;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.common.util.ZipFileUtils;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.guava30.shaded.common.util.concurrent.ExecutionError;
import org.apache.kylin.guava30.shaded.common.util.concurrent.SimpleTimeLimiter;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rec.common.SmartConfig;
import org.apache.kylin.rec.runner.JobRunnerFactory;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProposerJob extends ExecutableApplication {

    private static final String CONTEXT_CLASS = "contextClass";
    private static final String CONTEXT_PARAMS = "contextParams";
    private static final String CONTEXT_OUTPUT = "contextOutput";

    private static final String PATH_DELIMITER = "/";

    @VisibleForTesting
    public static AbstractContext proposeForAutoMode(KylinConfig config, String project, String[] sqls) {
        AbstractContext context = new SmartContext(config, project, sqls);
        return propose(context);
    }

    public static AbstractContext propose(AbstractContext context) {
        return propose(context, JobRunnerFactory::createRunner);
    }

    public static AbstractContext propose(AbstractContext context, RunnerFactoryBuilder factoryBuilder) {
        SmartConfig smartConfig = context.getSmartConfig();
        KylinConfig config = smartConfig.getKylinConfig();
        String project = context.getProject();

        ProposerJobEnv jobEnv = new ProposerJobEnv(config, project, context);
        String jobId = jobEnv.getJobId();

        JobRunnerFactory.AbstractJobRunner runner = null;
        try {

            jobEnv.buildJobEnv();

            runner = factoryBuilder.build(config, smartConfig.getProposeRunnerImpl(), project, jobEnv.getResource());
            runner.init(jobId);
            runner.start(new ProposerJob(), jobEnv.getParams());

            mergeResultIntoContext(jobEnv);
        } catch (Exception e) {
            throw new KylinException(ServerErrorCode.EXEC_JOB_FAILED, "Failed to exec job " + jobId, e);
        } finally {
            uploadLogs(jobEnv);
            if (runner != null) {
                runner.cleanupEnv();
            }
        }
        return context;
    }

    private static class ProposerJobEnv {
        String project;
        KylinConfig config;
        AbstractContext context;
        String jobId;

        // init after buildJobEnv
        Set<String> onlineModelIdSet = Sets.newHashSet();
        Set<String> allModelNames = Sets.newHashSet();
        List<String> resources = Lists.newArrayList();
        Map<String, String> params = Maps.newHashMap();
        String contextOutputFile;

        public ProposerJobEnv(KylinConfig config, String project, AbstractContext context) {
            this.project = project;
            this.config = config;
            this.context = context;
            this.jobId = generateJobId(project);
        }

        public void buildJobEnv() throws IOException {
            FileUtils.forceMkdir(new File(getJobTmpDir()));
            extractDumpResource();
            generateParams();
        }

        public String getJobTmpDir() {
            return KylinConfigBase.getKylinHome() + "/tmp/" + jobId;
        }

        public void extractDumpResource() {
            extractProjectResources();
            extractModelAndIndexPlanResources();
            extractTableResources();
            extractKafkaResources();
        }

        private void extractProjectResources() {
            String projectPath = MetadataType.mergeKeyWithType(project, MetadataType.PROJECT);
            resources.add(projectPath);
        }

        private void extractModelAndIndexPlanResources() {
            val baseModels = context.getRelatedModels();
            Set<String> allModelIdSet = baseModels.stream().map(NDataModel::getUuid).collect(Collectors.toSet());
            NDataflowManager.getInstance(config, project).listAllDataflows(true).forEach(df -> {
                NDataModel model = df.getModel();
                allModelNames.add(model.getAlias().toLowerCase(Locale.ROOT));
                if (model.isBroken() || !allModelIdSet.contains(model.getUuid())) {
                    return;
                }
                if (model.isFusionModel()) {
                    return;
                }
                resources.add(model.getResourcePath());
                resources.addAll(model.getComputedColumnDescs().stream().map(RootPersistentEntity::getResourcePath)
                        .collect(Collectors.toList()));
                resources.add(MetadataType.mergeKeyWithType(model.getId(), MetadataType.INDEX_PLAN));
                resources.addAll(df.getSegments().stream().map(RootPersistentEntity::getResourcePath)
                        .collect(Collectors.toList()));
                if (df.getStatus() == RealizationStatusEnum.ONLINE) {
                    onlineModelIdSet.add(model.getUuid());
                }
            });
        }

        private void extractTableResources() {
            val baseTables = context.getRelatedTables();
            NTableMetadataManager tableManager = NTableMetadataManager.getInstance(config, project);
            baseTables.forEach(tableIdentity -> {
                TableDesc tableDesc = tableManager.getTableDesc(tableIdentity);
                if (tableDesc != null) {
                    String tablePath = tableDesc.getResourcePath();
                    if (StringUtils.isNotEmpty(tablePath)) {
                        resources.add(tablePath);
                    }
                    TableExtDesc tableExtDesc = tableManager.getTableExtIfExists(tableDesc);
                    if (tableExtDesc != null) {
                        String resourcePath = tableExtDesc.getResourcePath();
                        if (StringUtils.isNotEmpty(resourcePath)) {
                            resources.add(resourcePath);
                        }
                    }
                }
            });
        }

        private void extractKafkaResources() {
            val baseTables = context.getRelatedTables();
            NTableMetadataManager tableManager = NTableMetadataManager.getInstance(config, project);
            Set<String> kafkaResources = Sets.newHashSet();
            baseTables.forEach(tableIdentity -> {
                TableDesc tableDesc = tableManager.getTableDesc(tableIdentity);
                if (tableDesc == null) {
                    return;
                }
                if (tableDesc.getSourceType() == ISourceAware.ID_STREAMING) {
                    String tableKafkaPath = tableDesc.getKafkaConfig().getResourcePath();
                    if (StringUtils.isNotEmpty(tableKafkaPath)) {
                        resources.add(tableKafkaPath);
                    }
                }
            });
            resources.addAll(kafkaResources);
        }

        public void generateParams() throws IOException {
            String jobTmpDir = getJobTmpDir();
            params.put(CONTEXT_CLASS, context.getClass().getName());

            val contextParamsFile = jobTmpDir + "/context_params.json";
            writeContextParams(contextParamsFile);
            params.put(CONTEXT_PARAMS, contextParamsFile);

            contextOutputFile = jobTmpDir + "/context_output.json";
            params.put(CONTEXT_OUTPUT, contextOutputFile);
        }

        private void writeContextParams(String contextParamsFile) throws IOException {
            ContextParams contextParams = new ContextParams(context, allModelNames, onlineModelIdSet);
            JsonUtil.writeValue(new File(contextParamsFile), contextParams);
        }

        public List<String> getResource() {
            return resources;
        }

        public Map<String, String> getParams() {
            return params;
        }

        public String getJobId() {
            return jobId;
        }

        private String generateJobId(String project) {
            return project + "-"
                    + LocalDateTime.now(Clock.systemDefaultZone()).format(DateTimeFormatter
                            .ofPattern("yyyy-MM-dd-HH-mm-ss-SSS", Locale.getDefault(Locale.Category.FORMAT)))
                    + "-" + RandomUtil.randomUUIDStr();
        }

        public String getProject() {
            return project;
        }

        public String getContextOutputFile() {
            return contextOutputFile;
        }

        public KylinConfig getKylinConfig() {
            return config;
        }

        public AbstractContext getContext() {
            return context;
        }
    }

    public static void mergeResultIntoContext(ProposerJobEnv env) throws IOException {
        val output = JsonUtil.readValue(new File(env.getContextOutputFile()), ContextOutput.class);
        initModelsForOutput(env.getProject(), env.getKylinConfig(), output);
        ContextOutput.merge(env.getContext(), output);
    }

    public static void uploadLogs(ProposerJobEnv env) {
        String jobContentZip = env.getJobTmpDir() + ".zip";
        try {
            SimpleTimeLimiter.create(ForkJoinPool.commonPool()).callWithTimeout(() -> uploadJobLog(env, jobContentZip),
                    Duration.ofSeconds(10));
        } catch (InterruptedException e) {
            log.warn("Upload job Interrupted! The job id is: {}", env.getJobId(), e);
            Thread.currentThread().interrupt();
        } catch (ExecutionError | Exception e) {
            log.warn("Upload Job Evidence failed {}", env.getJobId(), e);
        } finally {
            FileUtils.deleteQuietly(new File(jobContentZip));
        }
    }

    private static void initModelsForOutput(String project, KylinConfig config, ContextOutput output) {
        output.getModelContextOutputs().forEach(modelOutput -> {
            val originModel = modelOutput.getOriginModel();
            if (originModel != null) {
                originModel.init(config, project, Lists.newArrayList());
            }
            val targetModel = modelOutput.getTargetModel();
            if (targetModel != null) {
                Map<String, ComputedColumnDesc> usedCCMap = modelOutput.getUsedCC().values().stream()
                        .collect(Collectors.toMap(RootPersistentEntity::getUuid, cc -> cc));
                targetModel.setComputedColumnDescs(
                        targetModel.getComputedColumnUuids().stream().map(usedCCMap::get).collect(Collectors.toList()));
                targetModel.init(config, project, Lists.newArrayList());
            }
            modelOutput.getMeasureRecItemMap().forEach((key, m) -> {
                NDataModel.Measure measure = m.getMeasure();
                measure.getFunction().init(targetModel);
            });
        });
    }

    private static boolean uploadJobLog(ProposerJobEnv env, String jobContentZip) throws IOException {
        ZipFileUtils.compressZipFile(env.getJobTmpDir(), jobContentZip);
        String jobDir = KylinConfig.getInstanceFromEnv().getJobTmpDir(env.getProject(), true);
        FileSystem fs = HadoopUtil.getFileSystem(jobDir);

        try (InputStream in = new FileInputStream(jobContentZip);
                FSDataOutputStream out = fs.create(new Path(jobDir + env.getJobId() + ".zip"), true)) {
            IOUtils.copy(in, out);
        }
        return true;
    }

    static final Option OPTION_META_DIR = OptionBuilder.getInstance().withArgName("meta").hasArg().isRequired(true)
            .withDescription("metadata input directory").create("meta");
    static final Option OPTION_CONTEXT_PARAMS_FILE = OptionBuilder.getInstance().withArgName(CONTEXT_PARAMS).hasArg()
            .isRequired(true).withDescription("context params file").create(CONTEXT_PARAMS);
    static final Option OPTION_META_OUTPUT_DIR = OptionBuilder.getInstance().withArgName("metaOutput").hasArg()
            .isRequired(true).withDescription("metadata output directory").create("metaOutput");
    static final Option OPTION_CONTEXT_CLASS = OptionBuilder.getInstance().withArgName(CONTEXT_CLASS).hasArg()
            .isRequired(true).withDescription("context implement").create(CONTEXT_CLASS);
    static final Option OPTION_CONTEXT_OUTPUT_FILE = OptionBuilder.getInstance().withArgName(CONTEXT_OUTPUT).hasArg()
            .isRequired(true).withDescription("context output file").create(CONTEXT_OUTPUT);

    protected final Options options;

    public ProposerJob() {
        options = new Options();
        options.addOption(OPTION_META_DIR);
        options.addOption(OPTION_CONTEXT_PARAMS_FILE);
        options.addOption(OPTION_META_OUTPUT_DIR);
        options.addOption(OPTION_CONTEXT_CLASS);
        options.addOption(OPTION_CONTEXT_OUTPUT_FILE);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        val metaDir = optionsHelper.getOptionValue(OPTION_META_DIR);
        val contextParamsFile = optionsHelper.getOptionValue(OPTION_CONTEXT_PARAMS_FILE);
        val contextOutputFile = optionsHelper.getOptionValue(OPTION_CONTEXT_OUTPUT_FILE);
        val contextClass = optionsHelper.getOptionValue(OPTION_CONTEXT_CLASS);
        val contextParams = JsonUtil.readValue(new File(contextParamsFile), ContextParams.class);
        val sqls = contextParams.getSqls();
        val project = contextParams.getProject();
        val modelName = contextParams.getModelName();
        String propertiesFilePath = metaDir + PATH_DELIMITER + KYLIN_CONF_PROPERTIES_FILE;
        try (KylinConfig.SetAndUnsetThreadLocalConfig config = KylinConfig
                .setAndUnsetThreadLocalConfig(KylinConfig.createInstanceFromUri(propertiesFilePath))) {
            AbstractContext context;
            if (StringUtils.isNotEmpty(modelName)) {
                val contextConstructor = Class.forName(contextClass).getConstructor(KylinConfig.class, String.class,
                        String[].class, String.class);
                context = (AbstractContext) contextConstructor.newInstance(KylinConfig.getInstanceFromEnv(), project,
                        sqls.toArray(new String[0]), modelName);
            } else {
                val contextConstructor = Class.forName(contextClass).getConstructor(KylinConfig.class, String.class,
                        String[].class);
                context = (AbstractContext) contextConstructor.newInstance(KylinConfig.getInstanceFromEnv(), project,
                        sqls.toArray(new String[0]));
            }

            Unsafe.setProperty("needCheckCC", "true");
            context.getExtraMeta().setAllModels(contextParams.getAllModels());
            context.getExtraMeta().setOnlineModelIds(contextParams.getOnlineModelIds());
            context.getExtraMeta().setModelOptRule(contextParams.getModelOptRule());
            context.setCanCreateNewModel(contextParams.isCanCreateNewModel());
            context.setRestoredProposeContext(true);
            new SmartMaster(context).runWithContext(null);
            val output = ContextOutput.from(context);
            JsonUtil.writeValue(new File(contextOutputFile), output);
            ResourceStore.clearCache(config.get());
        }
    }

    @Data
    @NoArgsConstructor
    static class ContextParams implements Serializable {

        private String project;

        private boolean canCreateNewModel;

        private String modelOptRule;

        private List<String> sqls = Lists.newArrayList();

        private Set<String> allModels = Sets.newHashSet();

        private Set<String> onlineModelIds = Sets.newHashSet();

        private String modelName;

        public ContextParams(AbstractContext context, Set<String> allModelNames, Set<String> onlineModelIds) {
            this.project = context.getProject();
            this.modelOptRule = context.getSmartConfig().getModelOptRule();
            this.canCreateNewModel = context.canCreateNewModel;
            this.sqls = Arrays.asList(context.getSqlArray());
            this.allModels.addAll(allModelNames);
            this.onlineModelIds.addAll(onlineModelIds);
            this.modelName = context.getModelName();
        }
    }

    public interface RunnerFactoryBuilder {
        JobRunnerFactory.AbstractJobRunner build(KylinConfig config, String runnerType, String project,
                List<String> resources);
    }

    public static void setKylinConf(String[] args) {
        String metaDir = "";
        for (String arg : args) {
            if (arg.startsWith("--meta=")) {
                metaDir = arg.substring("--meta=".length());
                break;
            }
        }
        Unsafe.setProperty("KYLIN_CONF", metaDir);
    }

    public static void main(String[] args) {
        val tool = new ProposerJob();
        try {
            setKylinConf(args);
            tool.execute(args);
        } catch (Exception e) {
            log.warn("Propose {} failed", args, e);
            Unsafe.systemExit(1);
        }
        log.info("Propose finished");
        Unsafe.systemExit(0);
    }
}
