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
import org.apache.kylin.common.lock.DistributedLock;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.SerializableConfiguration;
import org.apache.kylin.job.common.PatternedLogger;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Maps;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class CreateSparkHiveDictStep extends AbstractApplication implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(CreateSparkHiveDictStep.class);

    private transient final PatternedLogger stepLogger = new PatternedLogger(logger);

    public static final Option OPTION_CUBE_NAME = OptionBuilder.withArgName(BatchConstants.ARG_CUBE_NAME).hasArg().isRequired(true).withDescription("Cube Name").create(BatchConstants.ARG_CUBE_NAME);
    public static final Option OPTION_HIVE_DICT_COLUMNS = OptionBuilder.withArgName("hiveDictColumns").hasArg().isRequired(true).withDescription("Hive Dict Columns").create("hiveDictColumns");
    public static final Option OPTION_META_URL = OptionBuilder.withArgName("metaUrl").hasArg().isRequired(true).withDescription("Meta Url").create("metaUrl");
    public static final Option OPTION_SEGMENT_ID = OptionBuilder.withArgName("segmentId").hasArg().isRequired(true).withDescription("Segment Id").create("segmentId");
    public static final Option OPTION_FLOW_JOB_ID = OptionBuilder.withArgName("flowJobId").hasArg().isRequired(true).withDescription("Flow Job Id").create("flowJobId");
    public static final Option OPTION_WAREHOUSE_DIR = OptionBuilder.withArgName("wareHouseDir").hasArg().isRequired(true).withDescription("WareHouse Dir").create("wareHouseDir");

    private final Lock threadLock = new ReentrantLock();

    private Options options;

    public CreateSparkHiveDictStep() {
        options = new Options();
        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_HIVE_DICT_COLUMNS);
        options.addOption(OPTION_META_URL);
        options.addOption(OPTION_SEGMENT_ID);
        options.addOption(OPTION_FLOW_JOB_ID);
        options.addOption(OPTION_WAREHOUSE_DIR);
    }

    private String cubeName;
    private String segmentId;
    private String[] hiveDictColumns;
    private String metaUrl;
    private String flowJobId;
    private String wareHouseDir;

    private void initParam(OptionsHelper optionsHelper) {
        cubeName = optionsHelper.getOptionValue(OPTION_CUBE_NAME);
        segmentId = optionsHelper.getOptionValue(OPTION_SEGMENT_ID);
        hiveDictColumns = optionsHelper.getOptionValue(OPTION_HIVE_DICT_COLUMNS).split(",");
        metaUrl = optionsHelper.getOptionValue(OPTION_META_URL);
        flowJobId = optionsHelper.getOptionValue(OPTION_FLOW_JOB_ID);
        wareHouseDir = optionsHelper.getOptionValue(OPTION_WAREHOUSE_DIR);
    }

    @Override
    protected void execute(OptionsHelper optionsHelper) {
        initParam(optionsHelper);

        DistributedLock lock = null;
        try {
            lock = KylinConfig.getInstanceFromEnv().getDistributedLockFactory().lockForCurrentThread();

            createSparkHiveDict(lock);

        } catch (Exception e) {
            logger.error("", e);
        } finally {
            if (lock != null) {
                MRHiveDictUtil.unLock(cubeName, flowJobId, lock, stepLogger);
            }
        }
    }

    private void createSparkHiveDict(DistributedLock lock) throws InterruptedException {
        logger.info("Start to run createSparkHiveDict {}", flowJobId);

        MRHiveDictUtil.getLock(cubeName, flowJobId, lock, threadLock, stepLogger);

        SparkSession spark = null;
        try {
            spark = SparkSession.builder().appName("Spark Hive Global Dict").master("yarn").config("spark.sql.warehouse.dir", wareHouseDir).enableHiveSupport().getOrCreate();

            SerializableConfiguration sConf = new SerializableConfiguration(spark.sparkContext().hadoopConfiguration());
            KylinConfig envConfig = AbstractHadoopJob.loadKylinConfigFromHdfs(sConf, metaUrl);

            CubeInstance cubeInstance = CubeManager.getInstance(envConfig).getCube(cubeName);
            CubeSegment seg = cubeInstance.getSegmentById(segmentId);
            IJoinedFlatTableDesc flatDesc = EngineFactory.getJoinedFlatTableDesc(seg);

            spark.sqlContext().setConf("hive.exec.compress.output", "false");
            spark.sqlContext().setConf("hive.mapred.mode", "unstrict");

            String globalDictTable = MRHiveDictUtil.globalDictTableName(flatDesc, cubeName);
            String createGlobalDictTableHql = MRHiveDictUtil.generateDictionaryDdl(globalDictTable);
            createGlobalDictTableHql = createGlobalDictTableHql.substring(0, createGlobalDictTableHql.lastIndexOf(";"));
            logger.info("create global dict table:" + createGlobalDictTableHql);
            spark.sql(createGlobalDictTableHql);

            String distinctValueTable = MRHiveDictUtil.distinctValueTable(flatDesc);
            String dropDistinctValueTableHql = MRHiveDictUtil.generateDropTableStatement(distinctValueTable);
            dropDistinctValueTableHql = dropDistinctValueTableHql.substring(0, dropDistinctValueTableHql.lastIndexOf(";"));
            logger.info("drop distinct value table:" + dropDistinctValueTableHql);
            spark.sql(dropDistinctValueTableHql);

            String createDistinctValueTableHql = MRHiveDictUtil.generateDistinctValueTableStatement(flatDesc);
            createDistinctValueTableHql = createDistinctValueTableHql.substring(0, createDistinctValueTableHql.lastIndexOf(";"));
            logger.info("create distinct value table:" + createDistinctValueTableHql);
            spark.sql(createDistinctValueTableHql);

            String segmentLevelDictTable = MRHiveDictUtil.segmentLevelDictTableName(flatDesc);
            String dropSegmentLevelDictTableHql = MRHiveDictUtil.generateDropTableStatement(segmentLevelDictTable);
            dropSegmentLevelDictTableHql = dropSegmentLevelDictTableHql.substring(0, dropSegmentLevelDictTableHql.lastIndexOf(";"));
            logger.info("drop segment dict table:" + dropSegmentLevelDictTableHql);
            spark.sql(dropSegmentLevelDictTableHql);

            String createSegmentLevelDictTableHql = MRHiveDictUtil.generateDictTableStatement(segmentLevelDictTable);
            createSegmentLevelDictTableHql = createSegmentLevelDictTableHql.substring(0, createSegmentLevelDictTableHql.lastIndexOf(";"));
            logger.info("create segment dict table:" + createSegmentLevelDictTableHql);
            spark.sql(createSegmentLevelDictTableHql);

            for (String dictColumn : hiveDictColumns) {
                String insertDataToDictIntermediateTable = MRHiveDictUtil.generateInsertDataStatement(flatDesc, dictColumn, globalDictTable);
                insertDataToDictIntermediateTable = insertDataToDictIntermediateTable.substring(0, insertDataToDictIntermediateTable.lastIndexOf(";"));
                logger.info("insertDataToDictIntermediateTable, column:" + dictColumn + "\n" + insertDataToDictIntermediateTable);
                spark.sql(insertDataToDictIntermediateTable);
            }

            String maxDictValSql = "select dict_column, max(dict_val) from " + globalDictTable + " group by dict_column";
            logger.info("maxDictValSql:" + maxDictValSql);
            Map<String, Integer> fieldMaxDictValMap = getFieldMaxDictValMap(spark, maxDictValSql);
            logger.info("fieldMaxDictValMap:" + fieldMaxDictValMap);

            for (String hiveDictColumn : hiveDictColumns) {
                Dataset<Row> fieldValues = spark.sql("select dict_key from " + distinctValueTable + " where dict_column='" + hiveDictColumn + "'");
                fieldValues.cache();

                Map<Integer, Integer> partitionDataCountMap = getPartitionDataCountMap(fieldValues);
                logger.info("partitionDataCountMap:" + partitionDataCountMap);

                Dataset<Row> dictDataSet = getDictDataSet(fieldMaxDictValMap, hiveDictColumn, fieldValues, partitionDataCountMap);

                fieldValues.unpersist();

                String tempViewName = generateTempViewName();
                Dataset<Row> dataFrame = spark.createDataFrame(dictDataSet.rdd(), getSchema());
                dataFrame.createOrReplaceTempView(tempViewName);

                spark.sql("insert into " + segmentLevelDictTable + " partition (dict_column='" + hiveDictColumn + "') select dict_key, dict_val from " + tempViewName);

                spark.sql("insert into " + globalDictTable + " partition (dict_column='" + hiveDictColumn + "') select dict_key, dict_val from " + segmentLevelDictTable + " union select dict_key, dict_val from " + globalDictTable + " where dict_column='" + hiveDictColumn + "'");
            }

        } catch (Exception e) {
            logger.error("", e);
            throw e;
        }
    }

    private String generateTempViewName() {
        return UUID.randomUUID().toString().replaceAll("-", "_");
    }

    private Map<String, Integer> getFieldMaxDictValMap(SparkSession spark, String maxDictValSql) {
        Dataset<Row> maxDictVal = spark.sql(maxDictValSql);

        Map<String, Integer> fieldMaxDictValMap = Maps.newHashMap();
        List<Row> maxDictValList = maxDictVal.collectAsList();
        if (maxDictValList != null && maxDictValList.size() > 0) {
            maxDictValList.forEach(row -> {
                fieldMaxDictValMap.put(row.get(0).toString().toUpperCase(Locale.CHINA), row.getInt(1));
            });
        }
        return fieldMaxDictValMap;
    }

    private Dataset<Row> getDictDataSet(Map<String, Integer> fieldMaxDictValMap, String hiveDictColumn, Dataset<Row> fieldValues, Map<Integer, Integer> partitionDataCountMap) {
        return fieldValues.mapPartitions(new MapPartitionsFunction<Row, Row>() {
                        @Override
                        public Iterator<Row> call(Iterator<Row> it) throws Exception {
                            Integer maxDictVal = fieldMaxDictValMap.get(hiveDictColumn.toUpperCase(Locale.CHINA));
                            maxDictVal = maxDictVal == null ? 0 : maxDictVal;

                            int partitionId = TaskContext.getPartitionId();
                            int offset = getPartitionCountOffset(partitionDataCountMap, partitionId);
                            int baseVal = maxDictVal + offset;
                            int partitionCount = partitionDataCountMap.get(partitionId);
                            List<Row> rowList = Lists.newArrayListWithCapacity(partitionCount);
                            while (it.hasNext()) {
                                Row row = it.next();
                                Object keyObj = row.get(0);
                                if (keyObj == null) {
                                    continue;
                                }
                                String dictKey = keyObj.toString();
                                Integer dictVal = ++baseVal;
                                rowList.add(RowFactory.create(dictKey, dictVal.toString()));
                            }
                            return rowList.iterator();
                        }
                    }, Encoders.kryo(Row.class));
    }

    private Map<Integer, Integer> getPartitionDataCountMap(Dataset<Row> fieldValues) {
        Dataset<String> partitionDataCount = fieldValues.mapPartitions(new MapPartitionsFunction<Row, String>() {
            @Override
            public Iterator<String> call(Iterator<Row> it) throws Exception {
                int partitionId = TaskContext.getPartitionId();
                int partitionDataCount = 0;
                while (it.hasNext()) {
                    it.next();
                    partitionDataCount++;
                }
                return Lists.newArrayList(partitionId + ":" + partitionDataCount).iterator();
            }
        }, Encoders.STRING());
        List<String> partitionDataCountList = partitionDataCount.collectAsList();
        Map<Integer, Integer> partitionDataCountMap = Maps.newHashMapWithExpectedSize(partitionDataCountList.size());
        for (String partitionCountStr : partitionDataCountList) {
            String[] arr = partitionCountStr.split(":");
            partitionDataCountMap.put(Integer.valueOf(arr[0]), Integer.valueOf(arr[1]));
        }
        return partitionDataCountMap;
    }

    @Override
    protected Options getOptions() {
        return options;
    }

    public static StructType getSchema() {
        List<StructField> schemaFields = Lists.newArrayList();
        schemaFields.add(DataTypes.createStructField("dict_key", DataTypes.StringType, true));
        schemaFields.add(DataTypes.createStructField("dict_val", DataTypes.StringType, true));
        return DataTypes.createStructType(schemaFields);
    }

    private int getPartitionCountOffset(Map<Integer, Integer> partitionDataCountMap, int partitionId) {
        int offset = 0;
        for (Map.Entry<Integer, Integer> entry : partitionDataCountMap.entrySet()) {
            if (entry.getKey() < partitionId) {
                offset = offset + entry.getValue();
            }
        }
        return offset;
    }
}
