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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.OptionBuilder;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.metadata.project.NProjectManager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.kyligence.kap.guava20.shaded.common.io.ByteSource;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MigrateJobTool extends ExecutableApplication {
    private static final Option OPTION_HELP = OptionBuilder.getInstance().hasArg(false)
            .withDescription("print help message.").isRequired(false).withLongOpt("help").create("h");

    private static final Option OPTION_DIR = OptionBuilder.getInstance().hasArg().withArgName("dir")
            .withDescription("Specify the directory to operator").isRequired(true).create("dir");

    private static final Map<String, String> JOB_TYPE_HANDLER_MAP = new HashMap<>();

    static {
        JOB_TYPE_HANDLER_MAP.put("INDEX_BUILD", "org.apache.kylin.engine.spark.job.ExecutableAddCuboidHandler");
        JOB_TYPE_HANDLER_MAP.put("INC_BUILD", "org.apache.kylin.engine.spark.job.ExecutableAddSegmentHandler");
        JOB_TYPE_HANDLER_MAP.put("INDEX_MERGE", "org.apache.kylin.engine.spark.job.ExecutableMergeOrRefreshHandler");
        JOB_TYPE_HANDLER_MAP.put("INDEX_REFRESH", "org.apache.kylin.engine.spark.job.ExecutableMergeOrRefreshHandler");
    }

    private KylinConfig config = KylinConfig.getInstanceFromEnv();

    private ResourceStore resourceStore;

    public static void main(String[] args) {
        val tool = new MigrateJobTool();
        tool.execute(args);
        System.out.println("Migrate job finished.");
        Unsafe.systemExit(0);
    }

    @Override
    protected Options getOptions() {
        Options options = new Options();
        options.addOption(OPTION_DIR);
        options.addOption(OPTION_HELP);
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        if (printUsage(optionsHelper)) {
            return;
        }

        String metadataUrl = getMetadataUrl(optionsHelper.getOptionValue(OPTION_DIR));

        config.setMetadataUrl(metadataUrl);

        resourceStore = ResourceStore.getKylinMetaStore(config);

        NProjectManager projectManager = NProjectManager.getInstance(config);
        for (ProjectInstance project : projectManager.listAllProjects()) {
            updateExecute(project);
        }
    }

    /**
     *
     * @param projectInstance
     */
    private void updateExecute(ProjectInstance projectInstance) {
        NExecutableManager executableManager = NExecutableManager.getInstance(config, projectInstance.getName());

        List<AbstractExecutable> executeJobs = executableManager.getAllExecutables().stream()
                .filter(executable -> JobTypeEnum.INC_BUILD == executable.getJobType()
                        || JobTypeEnum.INDEX_BUILD == executable.getJobType()
                        || JobTypeEnum.SUB_PARTITION_BUILD == executable.getJobType()
                        || JobTypeEnum.INDEX_REFRESH == executable.getJobType()
                        || JobTypeEnum.SUB_PARTITION_REFRESH == executable.getJobType()
                        || JobTypeEnum.INDEX_MERGE == executable.getJobType())
                .filter(executable -> ExecutableState.RUNNING == executable.getStatus()
                        || ExecutableState.ERROR == executable.getStatus()
                        || ExecutableState.PAUSED == executable.getStatus())
                .collect(Collectors.toList());

        for (AbstractExecutable executeJob : executeJobs) {
            String executePath = "/" + projectInstance.getName() + ResourceStore.EXECUTE_RESOURCE_ROOT + "/"
                    + executeJob.getId();
            RawResource rs = resourceStore.getResource(executePath);
            if (rs == null) {
                continue;
            }

            try (InputStream in = rs.getByteSource().openStream()) {
                JsonNode executeNode = JsonUtil.readValue(in, JsonNode.class);

                if (executeNode.has("name")) {
                    String name = executeNode.get("name").textValue();
                    String handlerType = JOB_TYPE_HANDLER_MAP.get(name);
                    if (handlerType != null && !executeNode.has("handler_type")) {
                        ((ObjectNode) executeNode).put("handler_type", handlerType);
                    }
                }

                addUpdateMetadataTask(executeNode);

                ByteArrayOutputStream buf = new ByteArrayOutputStream();
                DataOutputStream dout = new DataOutputStream(buf);
                JsonUtil.writeValue(dout, executeNode);
                dout.close();
                buf.close();

                ByteSource byteSource = ByteSource.wrap(buf.toByteArray());

                rs = new RawResource(executePath, byteSource, System.currentTimeMillis(), rs.getMvcc() + 1);

                System.out.println("update execute " + executePath);
                resourceStore.getMetadataStore().putResource(rs, null, UnitOfWork.DEFAULT_EPOCH_ID);

            } catch (Exception e) {
                log.warn("read {} failed", executePath, e);
            }
        }
    }

    /**
     *
     * @param executeNode
     */
    private void addUpdateMetadataTask(JsonNode executeNode) {
        if (executeNode.has("tasks")) {
            ArrayNode tasks = (ArrayNode) executeNode.get("tasks");
            if (tasks.size() == 2) {
                ObjectNode taskNode = tasks.get(0).deepCopy();

                String uuid = taskNode.get("uuid").textValue().replace("_00", "_02");
                taskNode.put("uuid", uuid);

                taskNode.put("name", "Update Metadata");

                taskNode.put("type", "org.apache.kylin.engine.spark.job.NSparkUpdateMetadataStep");

                if (taskNode.has("params")) {
                    ObjectNode paramsNode = (ObjectNode) taskNode.get("params");
                    paramsNode.remove("distMetaUrl");

                    paramsNode.remove("className");

                    paramsNode.remove("outputMetaUrl");
                }

                if (taskNode.has("output")) {
                    ObjectNode outputNode = (ObjectNode) taskNode.get("output");
                    if (outputNode.has("status")) {
                        outputNode.put("status", "READY");
                    }
                }
                tasks.add(taskNode);
            }
        }
    }

    private boolean printUsage(OptionsHelper optionsHelper) {
        boolean help = optionsHelper.hasOption(OPTION_HELP);
        if (help) {
            optionsHelper.printUsage(this.getClass().getName(), getOptions());
        }
        return help;
    }

    private String getMetadataUrl(String rootPath) {
        if (rootPath.startsWith("file://")) {
            rootPath = rootPath.replace("file://", "");
            return org.apache.commons.lang3.StringUtils.appendIfMissing(rootPath, "/");
        } else {
            return org.apache.commons.lang3.StringUtils.appendIfMissing(rootPath, "/");

        }
    }
}
