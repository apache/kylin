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
package org.apache.kylin.tool;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.PROJECT_NOT_EXIST;
import static org.apache.kylin.common.exception.code.ErrorCodeTool.PARAMETER_EMPTY;
import static org.apache.kylin.common.exception.code.ErrorCodeTool.PARAMETER_NOT_SPECIFY;
import static org.apache.kylin.common.exception.code.ErrorCodeTool.PATH_NOT_EXISTS;
import static org.apache.kylin.tool.util.ScreenPrintUtil.printlnGreen;
import static org.apache.kylin.tool.util.ScreenPrintUtil.printlnRed;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.common.util.OptionBuilder;
import org.apache.kylin.common.util.Unsafe;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.recommendation.candidate.JdbcRawRecStore;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.val;

public class RecCandidateTool extends ExecutableApplication {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    private static final Option OPERATE_BACKUP = OptionBuilder.getInstance()
            .withDescription("Backup rec candidate to local path").isRequired(false).create("backup");

    private static final Option OPTION_DIR = OptionBuilder.getInstance().hasArg().withArgName("DIRECTORY_PATH")
            .withDescription("Specify the target directory for backup and restore").isRequired(false).create("dir");

    private static final Option OPTION_MODEL_ID = OptionBuilder.getInstance().hasArg().withArgName("MODEL_ID")
            .withDescription("Specify model id for backup (optional)").isRequired(false).create("model");

    private static final Option OPTION_PROJECT = OptionBuilder.getInstance().hasArg().withArgName("PROJECT_NAME")
            .withDescription("Specify project name for backup (optional)").isRequired(false).create("project");

    private static final Option OPTION_TABLE = OptionBuilder.getInstance().hasArg().withArgName("TABLE_NAME")
            .withDescription("Specify the table for restore (optional)").isRequired(false).create("table");

    private final Options options;
    private final KylinConfig kylinConfig;

    public RecCandidateTool() {
        kylinConfig = KylinConfig.getInstanceFromEnv();
        this.options = new Options();
        initOptions();
    }

    public static void main(String[] args) {
        val tool = new RecCandidateTool();
        try {
            tool.execute(args);
        } catch (Exception e) {
            printlnRed("Rec candidate task failed. Detailed Message is at ${KYLIN_HOME}/logs/shell.stderr");
            logger.error("Rec candidate", e);
            Unsafe.systemExit(1);
        }
        printlnGreen("OK");
        Unsafe.systemExit(0);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    private void backup(OptionsHelper optionsHelper) throws Exception {
        String path = optionsHelper.getOptionValue(OPTION_DIR);
        if (StringUtils.isEmpty(path)) {
            path = KylinConfigBase.getKylinHome() + File.separator + "rec_candidate";
        }
        String time = LocalDateTime.now(Clock.systemDefaultZone())
                .format(DateTimeFormatter.ofPattern("yyyy_MM_dd_HH_mm_ss", Locale.getDefault(Locale.Category.FORMAT)));
        if (optionsHelper.hasOption(OPTION_PROJECT)) {
            String project = optionsHelper.getOptionValue(OPTION_PROJECT);
            String folder = String.format(Locale.ROOT, "project_%s", time);
            File dir = new File(path, folder);
            extractProject(project, dir);
        } else if (optionsHelper.hasOption(OPTION_MODEL_ID)) {
            String modelId = optionsHelper.getOptionValue(OPTION_MODEL_ID);
            String folder = String.format(Locale.ROOT, "model_%s", time);
            File dir = new File(path, folder);
            extractModel(getProjectByModelId(modelId), modelId, dir);
        } else {
            String folder = String.format(Locale.ROOT, "full_%s", time);
            File dir = new File(path, folder);
            extractFull(dir);
        }
    }

    private void restore(OptionsHelper optionsHelper) throws Exception {
        val table = optionsHelper.getOptionValue(OPTION_TABLE);
        if (StringUtils.isEmpty(table)) {
            throw new KylinException(PARAMETER_EMPTY, "table");
        }
        String path = optionsHelper.getOptionValue(OPTION_DIR);
        if (StringUtils.isEmpty(path)) {
            throw new KylinException(PARAMETER_NOT_SPECIFY, "-dir");
        }
        File dirFile = Paths.get(path).toFile();
        if (!dirFile.exists() || !dirFile.isDirectory()) {
            throw new KylinException(PATH_NOT_EXISTS, path);
        }
        File[] projects = dirFile.listFiles();
        if (projects == null) {
            logger.warn("No project found, skip restore.");
            return;
        }
        JdbcRawRecStore jdbcRawRecStore = new JdbcRawRecStore(kylinConfig, table);
        jdbcRawRecStore.deleteAll();
        for (val project : projects) {
            if (!project.isDirectory()) {
                logger.warn("{} is not directory.", project.getAbsolutePath());
                continue;
            }
            File[] models = project.listFiles();
            if (models == null) {
                logger.warn("No model fount in project {}, skip restore.", project.getName());
                continue;
            }
            for (val model : models) {
                List<RawRecItem> data = new ArrayList<>();
                try (InputStream in = new FileInputStream(model);
                        BufferedReader br = new BufferedReader(new InputStreamReader(in, Charset.defaultCharset()))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        try {
                            data.add(JsonUtil.readValue(line, RawRecItem.class));
                        } catch (Exception e) {
                            logger.error("Rec candidate deserialize error >>> {}", line, e);
                        }
                    }
                    jdbcRawRecStore.save(data, true);
                }
            }
        }
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        if (optionsHelper.hasOption(OPERATE_BACKUP)) {
            backup(optionsHelper);
        } else {
            throw new KylinException(PARAMETER_NOT_SPECIFY, "-backup");
        }
    }

    private void initOptions() {
        OptionGroup optionGroup1 = new OptionGroup();
        optionGroup1.setRequired(true);
        optionGroup1.addOption(OPERATE_BACKUP);
        OptionGroup optionGroup2 = new OptionGroup();
        optionGroup2.setRequired(false);
        optionGroup2.addOption(OPTION_MODEL_ID);
        optionGroup2.addOption(OPTION_PROJECT);
        options.addOptionGroup(optionGroup1);
        options.addOptionGroup(optionGroup2);
        options.addOption(OPTION_DIR);
        options.addOption(OPTION_TABLE);
    }

    public void extractFull(File dir) throws Exception {
        logger.info("Extract full rec candidate.");

        val projects = NProjectManager.getInstance(kylinConfig).listAllProjects();
        for (val project : projects) {
            extractProject(project.getName(), dir);
        }
    }

    public void extractProject(String project, File dir) throws Exception {
        logger.info("Extract project rec candidate.");
        if (!NProjectManager.getInstance(kylinConfig).listAllProjects().stream().map(ProjectInstance::getName)
                .collect(Collectors.toSet()).contains(project)) {
            throw new KylinException(PROJECT_NOT_EXIST, project);
        }
        val modelIds = NDataModelManager.getInstance(kylinConfig, project).listAllModelIds();
        for (val modelId : modelIds) {
            extractModel(project, modelId, dir);
        }
    }

    public void extractModel(String project, String modelId, File dir) throws Exception {
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Rec candidate interrupted.");
        }
        logger.info("Extract rec candidate, project {}, modelId {}.", project, modelId);
        File projectDir = new File(dir, project);
        FileUtils.forceMkdir(projectDir);
        File modelFile = new File(projectDir, modelId);

        JdbcRawRecStore jdbcRawRecStore = new JdbcRawRecStore(kylinConfig);
        List<RawRecItem> result = jdbcRawRecStore.listAll(project, modelId, Integer.MAX_VALUE);
        try (OutputStream os = new FileOutputStream(modelFile);
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os, Charset.defaultCharset()))) {
            for (RawRecItem line : result) {
                try {
                    bw.write(JsonUtil.writeValueAsString(line));
                    bw.newLine();
                } catch (Exception e) {
                    logger.error("Write error, id is {}", line.getId(), e);
                }
            }
        }
    }

    private String getProjectByModelId(String modelId) {
        if (StringUtils.isEmpty(modelId)) {
            throw new KylinException(PARAMETER_EMPTY, "model");
        }
        val projects = NProjectManager.getInstance(kylinConfig).listAllProjects();
        for (val project : projects) {
            if (NDataModelManager.getInstance(kylinConfig, project.getName()).getDataModelDesc(modelId) != null) {
                return project.getName();
            }
        }
        throw new KylinException(PARAMETER_EMPTY, "model");
    }
}
