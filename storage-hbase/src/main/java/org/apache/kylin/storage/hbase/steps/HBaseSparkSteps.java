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

package org.apache.kylin.storage.hbase.steps;

import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.spark.SparkBatchCubingJobBuilder2;
import org.apache.kylin.engine.spark.SparkExecutable;
import org.apache.kylin.engine.spark.SparkExecutableFactory;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.AbstractExecutable;

public class HBaseSparkSteps extends HBaseJobSteps {

    public HBaseSparkSteps(CubeSegment seg) {
        super(seg);
    }

    public AbstractExecutable createConvertCuboidToHfileStep(String jobId) {
        String cuboidRootPath = getCuboidRootPath(jobId);
        String inputPath = cuboidRootPath + (cuboidRootPath.endsWith("/") ? "" : "/");

        SparkBatchCubingJobBuilder2 jobBuilder2 = new SparkBatchCubingJobBuilder2(seg, null);
        final SparkExecutable sparkExecutable = SparkExecutableFactory.instance(seg.getConfig());
        sparkExecutable.setClassName(SparkCubeHFile.class.getName());
        sparkExecutable.setParam(SparkCubeHFile.OPTION_CUBE_NAME.getOpt(), seg.getRealization().getName());
        sparkExecutable.setParam(SparkCubeHFile.OPTION_SEGMENT_ID.getOpt(), seg.getUuid());
        sparkExecutable.setParam(SparkCubeHFile.OPTION_INPUT_PATH.getOpt(), inputPath);
        sparkExecutable.setParam(SparkCubeHFile.OPTION_META_URL.getOpt(),
                jobBuilder2.getSegmentMetadataUrl(seg.getConfig(), jobId));
        sparkExecutable.setParam(SparkCubeHFile.OPTION_OUTPUT_PATH.getOpt(), getHFilePath(jobId));
        sparkExecutable.setParam(SparkCubeHFile.OPTION_PARTITION_FILE_PATH.getOpt(),
                getRowkeyDistributionOutputPath(jobId) + "/part-r-00000_hfile");
        sparkExecutable.setParam(AbstractHadoopJob.OPTION_HBASE_CONF_PATH.getOpt(), getHBaseConfFilePath(jobId));
        sparkExecutable.setJobId(jobId);

        StringBuilder jars = new StringBuilder();
        StringUtil.appendWithSeparator(jars, ClassUtil.findContainingJar(org.apache.hadoop.hbase.KeyValue.class));
        StringUtil.appendWithSeparator(jars,
                ClassUtil.findContainingJar(org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2.class));
        StringUtil.appendWithSeparator(jars,
                ClassUtil.findContainingJar(org.apache.hadoop.hbase.regionserver.BloomType.class));
        StringUtil.appendWithSeparator(jars,
                ClassUtil.findContainingJar(org.apache.hadoop.hbase.protobuf.generated.HFileProtos.class)); //hbase-protocal.jar
        StringUtil.appendWithSeparator(jars,
                ClassUtil.findContainingJar(org.apache.hadoop.hbase.CompatibilityFactory.class)); //hbase-hadoop-compact.jar
        StringUtil.appendWithSeparator(jars, ClassUtil.findContainingJar("org.htrace.HTraceConfiguration", null)); // htrace-core.jar
        StringUtil.appendWithSeparator(jars, ClassUtil.findContainingJar("org.apache.htrace.Trace", null)); // htrace-core.jar
        StringUtil.appendWithSeparator(jars,
                ClassUtil.findContainingJar("com.yammer.metrics.core.MetricsRegistry", null)); // metrics-core.jar
        //KYLIN-3607
        StringUtil.appendWithSeparator(jars,
                ClassUtil.findContainingJar("org.apache.hadoop.hbase.regionserver.MetricsRegionServerSourceFactory", null));//hbase-hadoop-compat-1.1.1.jar
        StringUtil.appendWithSeparator(jars,
                ClassUtil.findContainingJar("org.apache.hadoop.hbase.regionserver.MetricsRegionServerSourceFactoryImpl", null));//hbase-hadoop2-compat-1.1.1.jar

        StringUtil.appendWithSeparator(jars, seg.getConfig().getSparkAdditionalJars());
        sparkExecutable.setJars(jars.toString());

        sparkExecutable.setName(ExecutableConstants.STEP_NAME_CONVERT_CUBOID_TO_HFILE);
        sparkExecutable.setCounterSaveAs(",," + CubingJob.CUBE_SIZE_BYTES, getCounterOutputPath(jobId));

        return sparkExecutable;
    }

}
