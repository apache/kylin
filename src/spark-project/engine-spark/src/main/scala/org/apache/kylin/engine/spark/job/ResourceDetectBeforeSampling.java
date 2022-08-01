package org.apache.kylin.engine.spark.job;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.SourceFactory;
import org.apache.kylin.engine.spark.NSparkCubingEngine;
import org.apache.kylin.engine.spark.application.SparkApplication;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;

import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;

@Slf4j
public class ResourceDetectBeforeSampling extends SparkApplication implements ResourceDetect {
    public static void main(String[] args) {
        ResourceDetectBeforeSampling detect = new ResourceDetectBeforeSampling();
        detect.execute(args);
    }

    @Override
    protected void doExecute() {
        String tableName = getParam(NBatchConstants.P_TABLE_NAME);
        final TableDesc tableDesc = NTableMetadataManager.getInstance(config, project).getTableDesc(tableName);
        Map<String, String> params = NProjectManager.getInstance(config).getProject(project)
                .getLegalOverrideKylinProps();
        long rowCount = Long.parseLong(getParam(NBatchConstants.P_SAMPLING_ROWS));
        params.put("sampleRowCount", String.valueOf(rowCount));
        final Dataset<Row> dataset = SourceFactory
                .createEngineAdapter(tableDesc, NSparkCubingEngine.NSparkCubingSource.class)
                .getSourceData(tableDesc, ss, params);
        final List<Path> paths = JavaConversions
                .seqAsJavaList(ResourceDetectUtils.getPaths(dataset.queryExecution().sparkPlan()));

        Map<String, Long> resourceSize = Maps.newHashMap();
        resourceSize.put(String.valueOf(tableName),
                ResourceDetectUtils.getResourceSize(SparderEnv.getHadoopConfiguration(),config.isConcurrencyFetchDataSourceSize(),
                        JavaConverters.asScalaIteratorConverter(paths.iterator()).asScala().toSeq()));

        Map<String, String> tableLeafTaskNums = Maps.newHashMap();
        tableLeafTaskNums.put(tableName, ResourceDetectUtils.getPartitions(dataset.queryExecution().executedPlan()));

        ResourceDetectUtils.write(
                new Path(config.getJobTmpShareDir(project, jobId), tableName + "_" + ResourceDetectUtils.fileName()),
                resourceSize);

        ResourceDetectUtils.write(new Path(config.getJobTmpShareDir(project, jobId),
                tableName + "_" + ResourceDetectUtils.samplingDetectItemFileSuffix()), tableLeafTaskNums);
    }
}
