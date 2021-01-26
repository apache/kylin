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

package org.apache.kylin.tool.metrics.systemcube;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.tool.metrics.systemcube.def.MetricsSinkDesc;
import org.apache.kylin.tool.metrics.systemcube.streamingv2.KafkaTopicCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.shaded.com.google.common.base.Strings;
import org.apache.kylin.shaded.com.google.common.collect.Lists;

/**
 * System Cube Metadata Creator CLI
 */
public class SCCreator extends AbstractApplication {

    private static final Logger logger = LoggerFactory.getLogger(SCCreator.class);

    private static final Option OPTION_OWNER = OptionBuilder.withArgName("owner").hasArg().isRequired(false)
            .withDescription("Specify the owner who creates the metadata").create("owner");
    private static final Option OPTION_INPUT_CONFIG = OptionBuilder.withArgName("inputConfig").hasArg()
            .isRequired(false).withDescription("Specify the input configuration file").create("inputConfig");
    private static final Option OPTION_OUTPUT = OptionBuilder.withArgName("output").hasArg().isRequired(true)
            .withDescription("Specify the output where the generated metadata will be saved").create("output");

    private static final String D_CUBE_INSTANCE = "cube/";
    private static final String D_CUBE_DESC = "cube_desc/";
    private static final String D_PROJECT = "project/";
    private static final String D_TABLE = "table/";
    private static final String D_MODEL_DESC = "model_desc/";
    private static final String D_STREAMING_V2 = "streaming_v2/";

    private static final String F_HIVE_SQL = "create_hive_tables_for_system_cubes";
    private static final String F_KAFKA_TOPIC = "create_kafka_topic_for_system_cubes";

    protected final Options options;

    private final KylinConfig config;

    public SCCreator() {
        config = KylinConfig.getInstanceFromEnv();

        options = new Options();
        options.addOption(OPTION_OWNER);
        options.addOption(OPTION_OUTPUT);
        options.addOption(OPTION_INPUT_CONFIG);
    }

    public static void main(String[] args) {
        SCCreator cli = new SCCreator();
        cli.execute(args);
    }

    protected Options getOptions() {
        return options;
    }

    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String owner = optionsHelper.getOptionValue(OPTION_OWNER);
        String output = optionsHelper.getOptionValue(OPTION_OUTPUT);
        String inputConfig = optionsHelper.getOptionValue(OPTION_INPUT_CONFIG);
        if (Strings.isNullOrEmpty(inputConfig)) {
            throw new FileNotFoundException("Input configuration file should be specified!!!");
        }

        execute(owner, output, inputConfig);
    }

    public void execute(String owner, String output, String inputConfig) throws Exception {
        if (Strings.isNullOrEmpty(owner)) {
            owner = "ADMIN";
        }
        if (!output.endsWith("/")) {
            output += "/";
        }

        TypeReference<List<MetricsSinkDesc>> typeRef = new TypeReference<List<MetricsSinkDesc>>() {
        };
        List<MetricsSinkDesc> metricsSinkDescList = JsonUtil.readValue(FileUtils.readFileToString(new File(inputConfig)), typeRef);
        run(owner, output, metricsSinkDescList);
    }

    private void run(String owner, String output, List<MetricsSinkDesc> sinkToolSet) throws IOException {
        List<TableDesc> kylinTables = Lists.newArrayList();
        List<DataModelDesc> kylinModels = Lists.newArrayList();
        List<CubeDesc> cubeDescList = Lists.newArrayList();
        List<CubeInstance> kylinCubeInstances = Lists.newArrayList();


        boolean hasHive = false;
        boolean hasKafka = false;
        for (MetricsSinkDesc sinkDesc : sinkToolSet) {
            if (sinkDesc.useHive()) {
                hasHive = true;
            } else if (sinkDesc.useKafka()) {
                hasKafka = true;
            }
            kylinTables.addAll(generateKylinTableForSystemCube(sinkDesc));
            kylinModels.addAll(generateKylinModelForSystemCube(owner, sinkDesc));
            cubeDescList.addAll(generateKylinCubeDescForSystemCube(sinkDesc));
            kylinCubeInstances.addAll(generateKylinCubeInstanceForSystemCube(owner, sinkDesc));

            if (sinkDesc.useKafka()) {
//                streamingSourceConfigs.add(StreamingMetadataCreator.generateKylinTableForMetricsJob(config, sinkDesc));
//                streamingSourceConfigs.add(StreamingMetadataCreator.generateKylinTableForMetricsJobException(config, sinkDesc));
//                streamingSourceConfigs.add(StreamingMetadataCreator.generateKylinTableForMetricsQuery(config, sinkDesc));
//                streamingSourceConfigs.add(StreamingMetadataCreator.generateKylinTableForMetricsQueryRpcCall(config, sinkDesc));
//                streamingSourceConfigs.add(StreamingMetadataCreator.generateKylinTableForMetricsQueryCube(config, sinkDesc));
            }
        }

        if (hasHive) {
            generateHiveTableSQLFileForSystemCube(output);
        }
        if (hasKafka) {
            generateKafkaTopicFileForSystemCube(output);
        }

        ProjectInstance projectInstance = ProjectCreator.generateKylinProjectInstance(owner, kylinTables, kylinModels,
                cubeDescList);
        generateKylinProjectFileForSystemCube(output, projectInstance);
        for (TableDesc tableDesc : kylinTables) {
            generateKylinTableFileForSystemCube(output, tableDesc);
        }
        for (DataModelDesc dataModelDesc : kylinModels) {
            generateKylinModelFileForSystemCube(output, dataModelDesc);
        }
        for (CubeDesc cubeDesc : cubeDescList) {
            generateKylinCubeDescFileForSystemCube(output, cubeDesc);
        }
        for (CubeInstance cubeInstance : kylinCubeInstances) {
            generateKylinCubeInstanceFileForSystemCube(output, cubeInstance);
        }

//        for (StreamingSourceConfig sourceConfig : streamingSourceConfigs) {
//            generateKylinStreamingConfigForSystemCube(output, sourceConfig);
//        }
    }

    private List<TableDesc> generateKylinTableForSystemCube(MetricsSinkDesc sinkDesc) {
        List<TableDesc> result = Lists.newLinkedList();
        result.add(KylinTableCreator.generateKylinTableForMetricsQueryExecution(config, sinkDesc));
        result.add(KylinTableCreator.generateKylinTableForMetricsQuerySparkJob(config, sinkDesc));
        result.add(KylinTableCreator.generateKylinTableForMetricsQuerySparkStage(config, sinkDesc));
        result.add(KylinTableCreator.generateKylinTableForMetricsJob(config, sinkDesc));
        result.add(KylinTableCreator.generateKylinTableForMetricsJobException(config, sinkDesc));

        return result;
    }

    private List<DataModelDesc> generateKylinModelForSystemCube(String owner, MetricsSinkDesc sinkDesc) {
        List<DataModelDesc> result = Lists.newLinkedList();
        result.add(ModelCreator.generateKylinModelForMetricsQuery(owner, config, sinkDesc));
        result.add(ModelCreator.generateKylinModelForMetricsQueryCube(owner, config, sinkDesc));
        result.add(ModelCreator.generateKylinModelForMetricsQueryRPC(owner, config, sinkDesc));
        result.add(ModelCreator.generateKylinModelForMetricsJob(owner, config, sinkDesc));
        result.add(ModelCreator.generateKylinModelForMetricsJobException(owner, config, sinkDesc));

        return result;
    }

    private List<CubeDesc> generateKylinCubeDescForSystemCube(MetricsSinkDesc sinkDesc) {
        List<CubeDesc> result = Lists.newLinkedList();
        result.add(CubeDescCreator.generateKylinCubeDescForMetricsQueryExecution(config, sinkDesc));
        result.add(CubeDescCreator.generateKylinCubeDescForMetricsQuerySparkJob(config, sinkDesc));
        result.add(CubeDescCreator.generateKylinCubeDescForMetricsQuerySparkStage(config, sinkDesc));
        result.add(CubeDescCreator.generateKylinCubeDescForMetricsJob(config, sinkDesc));
        result.add(CubeDescCreator.generateKylinCubeDescForMetricsJobException(config, sinkDesc));

        return result;
    }

    private List<CubeInstance> generateKylinCubeInstanceForSystemCube(String owner, MetricsSinkDesc sinkDesc) {
        List<CubeInstance> result = Lists.newLinkedList();
        result.add(CubeInstanceCreator.generateKylinCubeInstanceForMetricsQueryExecution(owner, config, sinkDesc));
        result.add(CubeInstanceCreator.generateKylinCubeInstanceForMetricsQuerySparkJob(owner, config, sinkDesc));
        result.add(CubeInstanceCreator.generateKylinCubeInstanceForMetricsQuerySparkStage(owner, config, sinkDesc));
        result.add(CubeInstanceCreator.generateKylinCubeInstanceForMetricsJob(owner, config, sinkDesc));
        result.add(CubeInstanceCreator.generateKylinCubeInstanceForMetricsJobException(owner, config, sinkDesc));

        return result;
    }

    private void generateHiveTableSQLFileForSystemCube(String output) throws IOException {
        String contents = HiveTableCreator.generateAllSQL(config);
        saveToFile(output + F_HIVE_SQL + ".sql", contents);
    }

    private void generateKafkaTopicFileForSystemCube(String output) throws IOException {
        String contents = KafkaTopicCreator.generateCreateCommand(config);
        saveToFile(output + F_KAFKA_TOPIC + ".sh", contents);
    }

    private void generateKylinTableFileForSystemCube(String output, TableDesc kylinTable) throws IOException {
        saveSystemCubeMetadataToFile(output + D_TABLE + kylinTable.getIdentity() + ".json", kylinTable,
                TableMetadataManager.TABLE_SERIALIZER);
    }

    private void generateKylinModelFileForSystemCube(String output, DataModelDesc modelDesc) throws IOException {
        saveSystemCubeMetadataToFile(output + D_MODEL_DESC + modelDesc.getName() + ".json", modelDesc,
                ModelCreator.MODELDESC_SERIALIZER);
    }

//    private void generateKylinStreamingConfigForSystemCube(String output, StreamingSourceConfig streamingConfig) throws IOException {
//        saveSystemCubeMetadataToFile(output + D_STREAMING_V2 + streamingConfig.getName() + ".json", streamingConfig,
//                StreamingMetadataCreator.STREAMING_SOURCE_CONFIG_SERIALIZER);
//    }

    private void generateKylinCubeInstanceFileForSystemCube(String output, CubeInstance cubeInstance)
            throws IOException {
        saveSystemCubeMetadataToFile(output + D_CUBE_INSTANCE + cubeInstance.getName() + ".json", cubeInstance,
                CubeManager.CUBE_SERIALIZER);
    }

    private void generateKylinCubeDescFileForSystemCube(String output, CubeDesc cubeDesc) throws IOException {
        saveSystemCubeMetadataToFile(output + D_CUBE_DESC + cubeDesc.getName() + ".json", cubeDesc,
                CubeDescManager.CUBE_DESC_SERIALIZER);
    }

    private void generateKylinProjectFileForSystemCube(String output, ProjectInstance projectInstance)
            throws IOException {
        saveSystemCubeMetadataToFile(output + D_PROJECT + projectInstance.getName() + ".json", projectInstance,
                CubeDescManager.CUBE_DESC_SERIALIZER);
    }

    private <T extends RootPersistentEntity> void saveSystemCubeMetadataToFile(String fileName, T metadata,
                                                                               Serializer serializer) throws IOException {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(buf);
        serializer.serialize(metadata, dout);
        dout.close();
        buf.close();

        saveToFile(fileName, buf.toString("UTF-8"));
    }

    private void saveToFile(String fileName, String contents) throws IOException {
        File parentDir = new File(fileName).getParentFile();
        if (!parentDir.exists()) {
            parentDir.mkdirs();
        }

        try (BufferedWriter bufferedWriter = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(fileName), StandardCharsets.UTF_8))) {
            bufferedWriter.append(contents);
        }
    }
}
