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
package org.apache.kylin.engine.spark;

import java.io.File;
import java.io.Serializable;
import java.net.BindException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.TempMetadataBuilder;
import org.apache.kylin.engine.spark.job.NSparkMergingJob;
import org.apache.kylin.engine.spark.merger.AfterMergeOrRefreshResourceMerger;
import org.apache.kylin.engine.spark.utils.SparkJobFactoryUtils;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.job.engine.JobEngineConfig;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.optimizer.ConvertInnerJoinToSemiJoin;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.sparkproject.guava.collect.Sets;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NLocalWithSparkSessionTest extends NLocalFileMetadataTestCase implements Serializable {

    private static final String CSV_TABLE_DIR = TempMetadataBuilder.TEMP_TEST_METADATA + "/data/%s.csv";

    protected static final String KYLIN_SQL_BASE_DIR = "../kylin-it/src/test/resources/query";

    protected static SparkConf sparkConf;
    protected static SparkSession ss;
    private TestingServer zkTestServer;

    protected static void ensureSparkConf() {
        if (sparkConf == null) {
            sparkConf = new SparkConf().setAppName(RandomUtil.randomUUIDStr()).setMaster("local[4]");
        }
    }

    @BeforeClass
    public static void beforeClass() {

        if (Shell.MAC)
            overwriteSystemPropBeforeClass("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");//for snappy

        ensureSparkConf();
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        sparkConf.set(StaticSQLConf.CATALOG_IMPLEMENTATION().key(), "in-memory");
        sparkConf.set("spark.sql.shuffle.partitions", "1");
        sparkConf.set("spark.memory.fraction", "0.1");
        // opt memory
        sparkConf.set("spark.shuffle.detectCorrupt", "false");
        // For sinai_poc/query03, enable implicit cross join conversion
        sparkConf.set("spark.sql.crossJoin.enabled", "true");
        sparkConf.set(StaticSQLConf.WAREHOUSE_PATH().key(),
                TempMetadataBuilder.TEMP_TEST_METADATA + "/spark-warehouse");

        sparkConf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY");
        sparkConf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY");
        sparkConf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED");
        sparkConf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED");
        sparkConf.set("spark.sql.legacy.timeParserPolicy", "LEGACY");
        sparkConf.set("spark.sql.parquet.mergeSchema", "true");
        sparkConf.set("spark.sql.legacy.allowNegativeScaleOfDecimal", "true");
        sparkConf.set("spark.sql.broadcastTimeout", "900");
        sparkConf.set("spark.sql.globalTempDatabase", "KYLIN_LOGICAL_VIEW");

        if (!sparkConf.getOption("spark.sql.extensions").isEmpty()) {
            sparkConf.set("spark.sql.extensions",
                    sparkConf.get("spark.sql.extensions") + ", io.delta.sql.DeltaSparkSessionExtension");
        } else {
            sparkConf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension");
        }
        sparkConf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog");
        ss = SparkSession.builder().withExtensions(ext -> {
            ext.injectOptimizerRule(ss -> new ConvertInnerJoinToSemiJoin());
            return null;
        }).config(sparkConf).getOrCreate();
        SparderEnv.setSparkSession(ss);
    }

    @AfterClass
    public static void afterClass() {
        if (ss != null) {
            ss.close();
        }
        FileUtils.deleteQuietly(new File("../kap-it/metastore_db"));
    }

    protected IndexDataConstructor indexDataConstructor;

    @Before
    public void setUp() throws Exception {
        overwriteSystemProp("calcite.keep-in-clause", "true");
        overwriteSystemProp("kylin.build.resource.consecutive-idle-state-num", "1");
        overwriteSystemProp("kylin.build.resource.state-check-interval-seconds", "1s");
        overwriteSystemProp("kylin.engine.spark.build-job-progress-reporter", //
                "org.apache.kylin.engine.spark.job.MockJobProgressReport");
        this.createTestMetadata();
        SparkJobFactoryUtils.initJobFactory();
        for (int i = 0; i < 100; i++) {
            try {
                zkTestServer = new TestingServer(RandomUtil.nextInt(7100, 65530), true);
                break;
            } catch (BindException e) {
                log.warn(e.getMessage());
            }
        }
        overwriteSystemProp("kylin.env.zookeeper-connect-string", zkTestServer.getConnectString());
        overwriteSystemProp("kylin.source.provider.9", "org.apache.kylin.engine.spark.mockup.CsvSource");
        indexDataConstructor = new IndexDataConstructor(getProject());
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
        if (zkTestServer != null) {
            zkTestServer.close();
        }
    }

    public String getProject() {
        return "default";
    }

    protected void init() throws Exception {
        overwriteSystemProp("kylin.job.scheduler.poll-interval-second", "1");
        overwriteSystemProp("calcite.keep-in-clause", "true");
        this.createTestMetadata();
        NDefaultScheduler scheduler = NDefaultScheduler.getInstance(getProject());
        scheduler.init(new JobEngineConfig(KylinConfig.getInstanceFromEnv()));
        if (!scheduler.hasStarted()) {
            throw new RuntimeException("scheduler has not been started");
        }
    }

    public static void populateSSWithCSVData(KylinConfig kylinConfig, String project, SparkSession sparkSession) {

        ProjectInstance projectInstance = NProjectManager.getInstance(kylinConfig).getProject(project);
        Preconditions.checkArgument(projectInstance != null);
        for (String table : projectInstance.getTables()) {

            if ("DEFAULT.STREAMING_TABLE".equals(table) || "DEFAULT.TEST_SNAPSHOT_TABLE".equals(table)
                    || table.contains(kylinConfig.getDDLLogicalViewDB())) {
                continue;
            }

            TableDesc tableDesc = NTableMetadataManager.getInstance(kylinConfig, project).getTableDesc(table);
            ColumnDesc[] columns = tableDesc.getColumns();
            StructType schema = new StructType();
            for (ColumnDesc column : columns) {
                schema = schema.add(column.getName(), convertType(column.getType()), false);
            }
            Dataset<Row> ret = sparkSession.read().schema(schema).csv(String.format(Locale.ROOT, CSV_TABLE_DIR, table));
            ret.createOrReplaceTempView(tableDesc.getName());
        }
    }

    private static DataType convertType(org.apache.kylin.metadata.datatype.DataType type) {
        if (type.isTimeFamily())
            return DataTypes.TimestampType;

        if (type.isDateTimeFamily())
            return DataTypes.DateType;

        if (type.isIntegerFamily())
            switch (type.getName()) {
            case "tinyint":
                return DataTypes.ByteType;
            case "smallint":
                return DataTypes.ShortType;
            case "integer":
            case "int4":
                return DataTypes.IntegerType;
            default:
                return DataTypes.LongType;
            }

        if (type.isNumberFamily())
            switch (type.getName()) {
            case "float":
                return DataTypes.FloatType;
            case "double":
                return DataTypes.DoubleType;
            default:
                if (type.getPrecision() == -1 || type.getScale() == -1) {
                    return DataTypes.createDecimalType(19, 4);
                } else {
                    return DataTypes.createDecimalType(type.getPrecision(), type.getScale());
                }
            }

        if (type.isStringFamily())
            return DataTypes.StringType;

        if (type.isBoolean())
            return DataTypes.BooleanType;

        throw new IllegalArgumentException("KAP data type: " + type + " can not be converted to spark's type.");
    }

    protected void fullBuild(String dfName) throws Exception {
        indexDataConstructor.buildDataflow(dfName);
    }

    public void buildMultiSegs(String dfName, long... layoutID) throws Exception {
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow df = dsMgr.getDataflow(dfName);
        List<LayoutEntity> layouts = new ArrayList<>();
        IndexPlan indexPlan = df.getIndexPlan();
        if (layoutID.length == 0) {
            layouts = indexPlan.getAllLayouts();
        } else {
            for (long id : layoutID) {
                layouts.add(indexPlan.getLayoutEntity(id));
            }
        }
        long start = SegmentRange.dateToLong("2009-01-01 00:00:00");
        long end = SegmentRange.dateToLong("2011-01-01 00:00:00");
        indexDataConstructor.buildIndex(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.newLinkedHashSet(layouts), true);

        start = SegmentRange.dateToLong("2011-01-01 00:00:00");
        end = SegmentRange.dateToLong("2013-01-01 00:00:00");
        indexDataConstructor.buildIndex(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.newLinkedHashSet(layouts), true);

        start = SegmentRange.dateToLong("2013-01-01 00:00:00");
        end = SegmentRange.dateToLong("2015-01-01 00:00:00");
        indexDataConstructor.buildIndex(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end),
                Sets.newLinkedHashSet(layouts), true);
    }

    public void buildMultiSegAndMerge(String dfName, long... layoutID) throws Exception {
        buildMultiSegs(dfName, layoutID);
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow df = dsMgr.getDataflow(dfName);
        List<LayoutEntity> layouts = new ArrayList<>();
        IndexPlan indexPlan = df.getIndexPlan();
        if (layoutID.length == 0) {
            layouts = indexPlan.getAllLayouts();
        } else {
            for (long id : layoutID) {
                layouts.add(indexPlan.getLayoutEntity(id));
            }
        }
        mergeSegments(dfName, Sets.newLinkedHashSet(layouts));
    }

    public void mergeSegments(String dfName, Set<LayoutEntity> toBuildLayouts) throws Exception {
        NDataflowManager dsMgr = NDataflowManager.getInstance(getTestConfig(), getProject());
        NDataflow df = dsMgr.getDataflow(dfName);
        NDataSegment firstMergeSeg = dsMgr.mergeSegments(df, new SegmentRange.TimePartitionedSegmentRange(
                SegmentRange.dateToLong("2011-01-01 00:00:00"), SegmentRange.dateToLong("2015-01-01 00:00:00")), false);
        NSparkMergingJob job = NSparkMergingJob.merge(firstMergeSeg, Sets.newLinkedHashSet(toBuildLayouts), "ADMIN",
                RandomUtil.randomUUIDStr());
        NExecutableManager execMgr = NExecutableManager.getInstance(getTestConfig(), getProject());
        // launch the job
        execMgr.addJob(job);

        if (!Objects.equals(IndexDataConstructor.wait(job), ExecutableState.SUCCEED)) {
            throw new IllegalStateException(IndexDataConstructor.firstFailedJobErrorMessage(execMgr, job));
        }

        val merger = new AfterMergeOrRefreshResourceMerger(getTestConfig(), getProject());
        merger.merge(job.getSparkMergingStep());

    }

}
