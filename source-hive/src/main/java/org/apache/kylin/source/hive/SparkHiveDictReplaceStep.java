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
package org.apache.kylin.source.hive;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.SerializableConfiguration;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

public class SparkHiveDictReplaceStep extends AbstractApplication implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(SparkHiveDictReplaceStep.class);

    public static final Option OPTION_CUBE_NAME = OptionBuilder.withArgName(BatchConstants.ARG_CUBE_NAME).hasArg().isRequired(true).withDescription("Cube Name").create(BatchConstants.ARG_CUBE_NAME);
    public static final Option OPTION_META_URL = OptionBuilder.withArgName("metaUrl").hasArg().isRequired(true).withDescription("Meta Url").create("metaUrl");
    public static final Option OPTION_SEGMENT_ID = OptionBuilder.withArgName("segmentId").hasArg().isRequired(true).withDescription("Segment Id").create("segmentId");
    public static final Option OPTION_FLOW_JOB_ID = OptionBuilder.withArgName("flowJobId").hasArg().isRequired(true).withDescription("Flow Job Id").create("flowJobId");
    public static final Option OPTION_WAREHOUSE_DIR = OptionBuilder.withArgName("wareHouseDir").hasArg().isRequired(true).withDescription("WareHouse Dir").create("wareHouseDir");
    public static final Option OPTION_DICT_COLUMNS = OptionBuilder.withArgName("dictColumns").hasArg().isRequired(true).withDescription("Dict Columns").create("dictColumns");
    public static final Option OPTION_GLOBAL_DICT_TABLE = OptionBuilder.withArgName("globalDictTable").hasArg().isRequired(true).withDescription("Global Dict Table").create("globalDictTable");
    public static final Option OPTION_DICT_SUFFIX = OptionBuilder.withArgName("dictSuffix").hasArg().isRequired(true).withDescription("Dict Suffix").create("dictSuffix");

    private String cubeName;
    private String segmentId;
    private String metaUrl;
    private String wareHouseDir;
    private String dictColumns;
    private String globalDictTable;
    private String dictSuffix;
    private String flowJobId;

    private Options options;

    public SparkHiveDictReplaceStep() {
        options = new Options();
        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_DICT_COLUMNS);
        options.addOption(OPTION_META_URL);
        options.addOption(OPTION_SEGMENT_ID);
        options.addOption(OPTION_FLOW_JOB_ID);
        options.addOption(OPTION_WAREHOUSE_DIR);
        options.addOption(OPTION_GLOBAL_DICT_TABLE);
        options.addOption(OPTION_DICT_SUFFIX);
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        initParam(optionsHelper);

        try {
            SparkSession spark = SparkSession.builder().appName("Spark Hive Global Dict").master("yarn").config("spark.sql.warehouse.dir", wareHouseDir).enableHiveSupport().getOrCreate();

            SerializableConfiguration sConf = new SerializableConfiguration(spark.sparkContext().hadoopConfiguration());
            KylinConfig envConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(sConf, metaUrl);

            CubeInstance cubeInstance = CubeManager.getInstance(envConfig).getCube(cubeName);
            CubeSegment seg = cubeInstance.getSegmentById(segmentId);
            IJoinedFlatTableDesc flatDesc = EngineFactory.getJoinedFlatTableDesc(seg);

            Map<String, String> dictRef = seg.getConfig().getMrHiveDictRefColumns();
            for (String dictColumn : dictColumns.split(",")) {
                dictRef.put(dictColumn, "");
            }

            Map<String, String> replaceSqlMap = MRHiveDictUtil.generateReplaceSql(flatDesc, dictRef, seg.getConfig().getHiveDatabaseForIntermediateTable(), seg.getConfig().getMrHiveDictDB(), globalDictTable, dictSuffix);
            for (Map.Entry<String, String> entry : replaceSqlMap.entrySet()) {
                logger.info("field:" + entry.getKey() + ":\n" + entry.getValue());
                String replaceSql = entry.getValue();
                replaceSql = replaceSql.substring(0, replaceSql.lastIndexOf(";"));
                spark.sql(replaceSql);
            }
        } catch (Exception e) {
            logger.error("", e);
            throw e;
        }
    }

    private void initParam(OptionsHelper optionsHelper) {
        cubeName = optionsHelper.getOptionValue(OPTION_CUBE_NAME);
        segmentId = optionsHelper.getOptionValue(OPTION_SEGMENT_ID);
        metaUrl = optionsHelper.getOptionValue(OPTION_META_URL);
        flowJobId = optionsHelper.getOptionValue(OPTION_FLOW_JOB_ID);
        wareHouseDir = optionsHelper.getOptionValue(OPTION_WAREHOUSE_DIR);
        dictColumns = optionsHelper.getOptionValue(OPTION_DICT_COLUMNS);
        globalDictTable = optionsHelper.getOptionValue(OPTION_GLOBAL_DICT_TABLE);
        dictSuffix = optionsHelper.getOptionValue(OPTION_DICT_SUFFIX);
    }

}
