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

package org.apache.kylin.engine.spark.job;

import static org.apache.kylin.engine.spark.job.NSparkExecutable.SPARK_MASTER;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.util.BufferedLogger;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.cluster.ClusterManagerFactory;
import org.apache.kylin.cluster.IClusterManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.engine.spark.job.ISparkJobHandler;
import io.kyligence.kap.engine.spark.job.SparkAppDescription;
import io.kyligence.kap.guava20.shaded.common.util.concurrent.UncheckedTimeoutException;
import lombok.val;

public class DefaultSparkBuildJobHandler implements ISparkJobHandler {
    private static final Logger logger = LoggerFactory.getLogger(DefaultSparkBuildJobHandler.class);
    private static final String SPACE = " ";
    private static final String SUBMIT_LINE_FORMAT = " \\\n";

    private static final String SPARK_JARS_1 = "spark.jars";
    private static final String SPARK_JARS_2 = "spark.yarn.dist.jars";
    private static final String SPARK_FILES_1 = "spark.files";
    private static final String SPARK_FILES_2 = "spark.yarn.dist.files";
    private static final String EQUALS = "=";

    @Override
    public void killOrphanApplicationIfExists(String project, String jobStepId, KylinConfig config,
            Map<String, String> sparkConf) {
        try {
            val sparkMaster = sparkConf.getOrDefault(SPARK_MASTER, "local");
            if (sparkMaster.startsWith("local")) {
                logger.info("Skip kill orphan app for spark.master={}", sparkMaster);
                return;
            }
            final IClusterManager cm = ClusterManagerFactory.create(config);
            cm.killApplication(jobStepId);
        } catch (UncheckedTimeoutException e) {
            logger.warn("Kill orphan app timeout {}", e.getMessage());
        }
    }

    @Override
    public void checkApplicationJar(KylinConfig config) throws ExecuteException {
        // Application-jar:
        // Path to a bundled jar including your application and all dependencies.
        // The URL must be globally visible inside of your cluster,
        // for instance, an hdfs:// path or a file:// path that is present on all nodes.
        try {
            String path = config.getKylinJobJarPath();
            final String failedMsg = "Application jar should be only one bundled jar.";
            URI uri = new URI(path);
            if (Objects.isNull(uri.getScheme()) || uri.getScheme().startsWith("file:/")) {
                Preconditions.checkState(new File(path).exists(), failedMsg);
                return;
            }

            Path path0 = new Path(path);
            FileSystem fs = HadoopUtil.getFileSystem(path0);
            Preconditions.checkState(fs.exists(path0), failedMsg);
        } catch (URISyntaxException | IOException e) {
            throw new ExecuteException("Failed to check application jar.", e);
        }
    }

    @Override
    public String createArgsFileOnRemoteFileSystem(KylinConfig config, String project, String jobId,
            Map<String, String> params) throws ExecuteException {
        val fs = HadoopUtil.getWorkingFileSystem();
        Path path = fs.makeQualified(new Path(config.getJobTmpArgsDir(project, jobId)));
        try (FSDataOutputStream out = fs.create(path)) {
            out.write(JsonUtil.writeValueAsBytes(params));
        } catch (IOException e) {
            try {
                fs.delete(path, true);
            } catch (IOException e1) {
                throw new ExecuteException("Write spark args failed! Error for delete file: " + path.toString(), e1);
            }
            throw new ExecuteException("Write spark args failed: ", e);
        }
        return path.toString();
    }

    @Override
    public Object generateSparkCmd(KylinConfig config, SparkAppDescription desc) {
        // Hadoop conf dir.
        StringBuilder cmdBuilder = new StringBuilder("export HADOOP_CONF_DIR=");
        cmdBuilder.append(desc.getHadoopConfDir());
        cmdBuilder.append(SPACE).append("&&");

        // Spark submit.
        cmdBuilder.append(SPACE).append(KylinConfigBase.getSparkHome()).append(File.separator);
        cmdBuilder.append("bin/spark-submit");
        cmdBuilder.append(SUBMIT_LINE_FORMAT);

        // Application main class.
        cmdBuilder.append(SPACE).append("--class");
        cmdBuilder.append(SPACE).append("org.apache.kylin.engine.spark.application.SparkEntry");
        cmdBuilder.append(SUBMIT_LINE_FORMAT);

        // Application name.
        cmdBuilder.append(SPACE).append("--name");
        cmdBuilder.append(SPACE).append(desc.getJobNamePrefix()).append(desc.getJobId());
        cmdBuilder.append(SUBMIT_LINE_FORMAT);

        // Spark jars.
        cmdBuilder.append(SPACE).append("--jars");
        cmdBuilder.append(SPACE).append(String.join(desc.getComma(), desc.getSparkJars()));
        cmdBuilder.append(SUBMIT_LINE_FORMAT);

        cmdBuilder.append(SPACE).append("--files");
        cmdBuilder.append(SPACE).append(String.join(desc.getComma(), desc.getSparkFiles()));
        cmdBuilder.append(SUBMIT_LINE_FORMAT);

        // Spark conf.
        // Maybe we would rewrite some confs, like 'extraJavaOptions', 'extraClassPath',
        // and the confs rewrited should be removed from props thru #modifyDump.
        wrapSparkConf(cmdBuilder, desc.getSparkConf());

        // Application jar. KylinJobJar is the application-jar (of spark-submit),
        // path to a bundled jar including your application and all dependencies,
        // The URL must be globally visible inside of your cluster,
        // for instance, an hdfs:// path or a file:// path that is present on all nodes.
        cmdBuilder.append(SPACE).append(desc.getKylinJobJar());
        cmdBuilder.append(SUBMIT_LINE_FORMAT);

        // Application parameter file.
        cmdBuilder.append(SPACE).append(desc.getAppArgs());

        final String command = cmdBuilder.toString();
        logger.info("spark submit cmd: {}", command);

        // Safe check.
        checkCommandInjection(command);
        return command;
    }

    private void checkCommandInjection(String command) {
        if (Objects.isNull(command)) {
            return;
        }
        List<String> illegals = Lists.newArrayList();
        Matcher matcher = Pattern.compile("(`[^`]*+`)|(\\$\\([^)]*+)").matcher(command);
        while (matcher.find()) {
            illegals.add(matcher.group());
        }

        if (illegals.isEmpty()) {
            return;
        }

        String msg = String.format("Not allowed to specify injected command through "
                + "java options (like: %s). Vulnerabilities would allow attackers to trigger "
                + "such a crash or crippling of the service.", String.join(", ", illegals));
        throw new IllegalArgumentException(msg);
    }

    private void wrapSparkConf(StringBuilder cmdBuilder, Map<String, String> sparkConf) {
        for (Map.Entry<String, String> entry : sparkConf.entrySet()) {
            switch (entry.getKey()) {
            // Avoid duplicated from '--jars'
            // Avoid duplicated from '--files'
            case SPARK_JARS_1:
            case SPARK_JARS_2:
            case SPARK_FILES_1:
            case SPARK_FILES_2:
                // Do nothing.
                break;
            default:
                appendSparkConf(cmdBuilder, entry.getKey(), entry.getValue());
                break;
            }
        }
    }

    protected void appendSparkConf(StringBuilder sb, String confKey, String confValue) {
        // Multiple parameters in "--conf" need to be enclosed in single quotes
        sb.append(" --conf '").append(confKey).append(EQUALS).append(confValue).append("' ");
        sb.append(SUBMIT_LINE_FORMAT);
    }

    @Override
    public Map<String, String> runSparkSubmit(Object cmd, String parentId) throws ExecuteException {
        Map<String, String> updateInfo = Maps.newHashMap();
        try {
            val patternedLogger = new BufferedLogger(logger);
            CliCommandExecutor exec = new CliCommandExecutor();
            CliCommandExecutor.CliCmdExecResult r = exec.execute((String) cmd, patternedLogger, parentId);
            if (StringUtils.isNotEmpty(r.getProcessId())) {
                updateInfo.put("process_id", r.getProcessId());
            }
            updateInfo.put("output", r.getCmd());
            return updateInfo;
        } catch (Exception e) {
            logger.warn("failed to execute spark submit command.");
            throw new ExecuteException(e);
        }
    }
}
