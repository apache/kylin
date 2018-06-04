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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.BatchCubingJobBuilder2;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 */
public class SparkBatchCubingJobBuilder2 extends BatchCubingJobBuilder2 {

    private static final Logger logger = LoggerFactory.getLogger(SparkBatchCubingJobBuilder2.class);

    public SparkBatchCubingJobBuilder2(CubeSegment newSegment, String submitter) {
        super(newSegment, submitter);
    }

    @Override
    protected void addLayerCubingSteps(final CubingJob result, final String jobId, final String cuboidRootPath) {
        final SparkExecutable sparkExecutable = new SparkExecutable();
        sparkExecutable.setClassName(SparkCubingByLayer.class.getName());
        configureSparkJob(seg, sparkExecutable, jobId, cuboidRootPath);
        result.addTask(sparkExecutable);
    }

    @Override
    protected void addInMemCubingSteps(final CubingJob result, String jobId, String cuboidRootPath) {

    }

    private static String findJar(String className, String perferLibraryName) {
        try {
            return ClassUtil.findContainingJar(Class.forName(className), perferLibraryName);
        } catch (ClassNotFoundException e) {
            logger.warn("failed to locate jar for class " + className + ", ignore it");
        }

        return "";
    }

    public void configureSparkJob(final CubeSegment seg, final SparkExecutable sparkExecutable,
            final String jobId, final String cuboidRootPath) {
        final IJoinedFlatTableDesc flatTableDesc = EngineFactory.getJoinedFlatTableDesc(seg);
        final String tablePath = JoinedFlatTable.getTableDir(flatTableDesc, getJobWorkingDir(jobId));
        sparkExecutable.setParam(SparkCubingByLayer.OPTION_CUBE_NAME.getOpt(), seg.getRealization().getName());
        sparkExecutable.setParam(SparkCubingByLayer.OPTION_SEGMENT_ID.getOpt(), seg.getUuid());
        sparkExecutable.setParam(SparkCubingByLayer.OPTION_INPUT_TABLE.getOpt(),
                seg.getConfig().getHiveDatabaseForIntermediateTable() + "." + flatTableDesc.getTableName());
        sparkExecutable.setParam(SparkCubingByLayer.OPTION_INPUT_PATH.getOpt(),
                tablePath);
        sparkExecutable.setParam(SparkCubingByLayer.OPTION_META_URL.getOpt(),
                getSegmentMetadataUrl(seg.getConfig(), jobId));
        sparkExecutable.setParam(SparkCubingByLayer.OPTION_OUTPUT_PATH.getOpt(), cuboidRootPath);
        sparkExecutable.setJobId(jobId);

        StringBuilder jars = new StringBuilder();

        StringUtil.appendWithSeparator(jars, seg.getConfig().getSparkAdditionalJars());
        sparkExecutable.setJars(jars.toString());
        sparkExecutable.setName(ExecutableConstants.STEP_NAME_BUILD_SPARK_CUBE);
    }

    private String getSegmentMetadataUrl(KylinConfig kylinConfig, String jobId) {
        Map<String, String> param = new HashMap<>();
        param.put("path", getDumpMetadataPath(jobId));
        return new StorageURL(kylinConfig.getMetadataUrl().getIdentifier(), "hdfs", param).toString();
    }
}
