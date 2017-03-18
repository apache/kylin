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
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.BatchCubingJobBuilder2;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class SparkBatchCubingJobBuilder2 extends BatchCubingJobBuilder2 {

    private static final Logger logger = LoggerFactory.getLogger(SparkBatchCubingJobBuilder2.class);

    public SparkBatchCubingJobBuilder2(CubeSegment newSegment, String submitter) {
        super(newSegment, submitter);
    }

    @Override
    protected void addLayerCubingSteps(final CubingJob result, final String jobId, final String cuboidRootPath) {
        IJoinedFlatTableDesc flatTableDesc = EngineFactory.getJoinedFlatTableDesc(seg);
        final SparkExecutable sparkExecutable = new SparkExecutable();
        sparkExecutable.setClassName(SparkCubingByLayer.class.getName());
        sparkExecutable.setParam(SparkCubingByLayer.OPTION_CUBE_NAME.getOpt(), seg.getRealization().getName());
        sparkExecutable.setParam(SparkCubingByLayer.OPTION_SEGMENT_ID.getOpt(), seg.getUuid());
        sparkExecutable.setParam(SparkCubingByLayer.OPTION_INPUT_TABLE.getOpt(), seg.getConfig().getHiveDatabaseForIntermediateTable() + "." + flatTableDesc.getTableName());
        sparkExecutable.setParam(SparkCubingByLayer.OPTION_CONF_PATH.getOpt(), KylinConfig.getKylinConfPath());
        sparkExecutable.setParam(SparkCubingByLayer.OPTION_OUTPUT_PATH.getOpt(), cuboidRootPath);

        StringBuilder jars = new StringBuilder();

        StringUtil.appendWithSeparator(jars, findJar("org.htrace.HTraceConfiguration", null)); // htrace-core.jar
        StringUtil.appendWithSeparator(jars, findJar("org.apache.htrace.Trace", null)); // htrace-core.jar
        StringUtil.appendWithSeparator(jars, findJar("org.cloudera.htrace.HTraceConfiguration", null)); // htrace-core.jar
        StringUtil.appendWithSeparator(jars, findJar("org.apache.hadoop.hbase.client.HConnection", null)); // hbase-client.jar
        StringUtil.appendWithSeparator(jars, findJar("org.apache.hadoop.hbase.HBaseConfiguration", null)); // hbase-common.jar
        StringUtil.appendWithSeparator(jars, findJar("org.apache.hadoop.hbase.util.ByteStringer", null)); // hbase-protocol.jar
        StringUtil.appendWithSeparator(jars, findJar("com.yammer.metrics.core.Gauge", null)); // metrics-core.jar
        StringUtil.appendWithSeparator(jars, findJar("com.google.common.collect.Maps", "guava")); //guava.jar

        StringUtil.appendWithSeparator(jars, seg.getConfig().getSparkAdditionalJars());
        sparkExecutable.setJars(jars.toString());

        sparkExecutable.setName(ExecutableConstants.STEP_NAME_BUILD_SPARK_CUBE);
        result.addTask(sparkExecutable);
    }

    @Override
    protected void addInMemCubingSteps(final CubingJob result, String jobId, String cuboidRootPath) {

    }

    private String findJar(String className, String perferLibraryName) {
        try {
            return ClassUtil.findContainingJar(Class.forName(className), perferLibraryName);
        } catch (ClassNotFoundException e) {
            logger.warn("failed to locate jar for class " + className + ", ignore it");
        }

        return "";
    }

}
