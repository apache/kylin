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
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.engine.mr.CubingJob;
import org.apache.kylin.engine.mr.common.JobRelatedMetaUtil;
import org.apache.kylin.job.common.PatternedLogger;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.LoggerFactory;

/**
 */
public class SparkExecutable extends AbstractExecutable {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(SparkExecutable.class);

    private static final String CLASS_NAME = "className";
    private static final String JARS = "jars";
    private static final String JOB_ID = "jobId";

    public void setClassName(String className) {
        this.setParam(CLASS_NAME, className);
    }

    public void setJobId(String jobId) {
        this.setParam(JOB_ID, jobId);
    }

    public void setJars(String jars) {
        this.setParam(JARS, jars);
    }

    private String formatArgs() {
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<String, String> entry : getParams().entrySet()) {
            StringBuilder tmp = new StringBuilder();
            tmp.append("-").append(entry.getKey()).append(" ").append(entry.getValue()).append(" ");
            if (entry.getKey().equals(CLASS_NAME)) {
                stringBuilder.insert(0, tmp);
            } else if (entry.getKey().equals(JARS) || entry.getKey().equals(JOB_ID)) {
                // JARS is for spark-submit, not for app
                continue;
            } else {
                stringBuilder.append(tmp);
            }
        }
        if (stringBuilder.length() > 0) {
            return stringBuilder.substring(0, stringBuilder.length() - 1).toString();
        } else {
            return StringUtils.EMPTY;
        }
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        String cubeName = this.getParam(SparkCubingByLayer.OPTION_CUBE_NAME.getOpt());
        CubeInstance cube = CubeManager.getInstance(context.getConfig()).getCube(cubeName);
        final KylinConfig config = cube.getConfig();

        setAlgorithmLayer();

        if (KylinConfig.getSparkHome() == null) {
            throw new NullPointerException();
        }
        if (config.getKylinJobJarPath() == null) {
            throw new NullPointerException();
        }
        String jars = this.getParam(JARS);

        //hadoop conf dir
        String hadoopConf = null;
        hadoopConf = System.getProperty("kylin.hadoop.conf.dir");

        if (StringUtils.isEmpty(hadoopConf)) {
            throw new RuntimeException(
                    "kylin_hadoop_conf_dir is empty, check if there's error in the output of 'kylin.sh start'");
        }

        logger.info("Using " + hadoopConf + " as HADOOP_CONF_DIR");

        String jobJar = config.getKylinJobJarPath();
        if (StringUtils.isEmpty(jars)) {
            jars = jobJar;
        }

        String segmentID = this.getParam(SparkCubingByLayer.OPTION_SEGMENT_ID.getOpt());
        CubeSegment segment = cube.getSegmentById(segmentID);

        try {
            attachSegmentMetadataWithDict(segment);
        } catch (IOException e) {
            throw new ExecuteException("meta dump fialed");
        }

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(
                "export HADOOP_CONF_DIR=%s && %s/bin/spark-submit --class org.apache.kylin.common.util.SparkEntry ");

        Map<String, String> sparkConfs = config.getSparkConfigOverride();
        for (Map.Entry<String, String> entry : sparkConfs.entrySet()) {
            stringBuilder.append(" --conf ").append(entry.getKey()).append("=").append(entry.getValue()).append(" ");
        }

        stringBuilder.append("--jars %s %s %s");
        try {
            String cmd = String.format(stringBuilder.toString(), hadoopConf, KylinConfig.getSparkHome(), jars, jobJar,
                    formatArgs());
            logger.info("cmd: " + cmd);
            CliCommandExecutor exec = new CliCommandExecutor();
            PatternedLogger patternedLogger = new PatternedLogger(logger);
            exec.execute(cmd, patternedLogger);
            getManager().addJobInfo(getId(), patternedLogger.getInfo());
            return new ExecuteResult(ExecuteResult.State.SUCCEED, patternedLogger.getBufferedLog());
        } catch (Exception e) {
            logger.error("error run spark job:", e);
            return ExecuteResult.createError(e);
        }
    }

    // Spark Cubing can only work in layer algorithm
    private void setAlgorithmLayer() {
        ExecutableManager execMgr = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv());
        CubingJob cubingJob = (CubingJob) execMgr.getJob(this.getParam(JOB_ID));
        cubingJob.setAlgorithm(CubingJob.AlgorithmEnum.LAYER);
    }

    private void attachSegmentMetadataWithDict(CubeSegment segment) throws IOException {
        Set<String> dumpList = new LinkedHashSet<>();
        dumpList.addAll(JobRelatedMetaUtil.collectCubeMetadata(segment.getCubeInstance()));
        dumpList.addAll(segment.getDictionaryPaths());
        dumpList.add(segment.getStatisticsResourcePath());
        dumpAndUploadKylinPropsAndMetadata(dumpList, (KylinConfigExt) segment.getConfig());
    }

    private void dumpAndUploadKylinPropsAndMetadata(Set<String> dumpList, KylinConfigExt kylinConfig)
            throws IOException {
        File tmp = File.createTempFile("kylin_job_meta", "");
        FileUtils.forceDelete(tmp); // we need a directory, so delete the file first

        File metaDir = new File(tmp, "meta");
        metaDir.mkdirs();

        // dump metadata
        JobRelatedMetaUtil.dumpResources(kylinConfig, metaDir, dumpList);

        // write kylin.properties
        Properties props = kylinConfig.exportToProperties();
        String metadataUrl = this.getParam(SparkCubingByLayer.OPTION_META_URL.getOpt());
        props.setProperty("kylin.metadata.url", metadataUrl);
        File kylinPropsFile = new File(metaDir, "kylin.properties");
        try (FileOutputStream os = new FileOutputStream(kylinPropsFile)) {
            props.store(os, kylinPropsFile.getAbsolutePath());
        }

        KylinConfig dstConfig = KylinConfig.createKylinConfig(props);
        //upload metadata
        ResourceTool.copy(KylinConfig.createInstanceFromUri(metaDir.getAbsolutePath()), dstConfig);
    }
}
