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

package org.apache.kylin.streaming.jobs;

import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.metadata.cube.utils.StreamingUtils;
import org.apache.kylin.streaming.constants.StreamingConstants;
import org.apache.kylin.streaming.manager.StreamingJobManager;
import org.apache.kylin.streaming.metadata.StreamingJobMeta;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractSparkJobLauncher implements SparkJobLauncher {

    protected KylinConfig config;

    protected String project;
    protected String modelId;
    protected String jobId;

    protected SparkAppHandle handler;
    protected SparkAppHandle.Listener listener;
    protected Map<String, String> env;

    protected StreamingJobManager streamingJobManager;
    protected StreamingJobMeta strmJob;
    protected JobTypeEnum jobType;

    protected String kylinJobJar;
    protected SparkLauncher launcher;

    protected static String javaPropertyFormatter(@Nonnull String key, @Nullable String value) {
        Preconditions.checkNotNull(key, "the key of java property cannot be empty");
        return String.format(Locale.ROOT, " -D%s=%s ", key, value);
    }

    protected static Map<String, String> getStreamingSparkConfig(KylinConfig config) {
        return config.getStreamingSparkConfigOverride().entrySet().stream()
                .filter(entry -> entry.getKey().startsWith("spark."))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    protected static Map<String, String> getStreamingKafkaConfig(KylinConfig config) {
        return config.getStreamingKafkaConfigOverride();
    }

    public void init(String project, String modelId, JobTypeEnum jobType) {
        this.project = project;
        this.modelId = modelId;
        this.jobId = StreamingUtils.getJobId(modelId, jobType.name());
        this.jobType = jobType;
        this.env = Maps.newHashMap();
        this.config = KylinConfig.getInstanceFromEnv();
        this.kylinJobJar = config.getStreamingJobJarPath();
        this.listener = new StreamingJobListener(project, jobId);
        this.streamingJobManager = StreamingJobManager.getInstance(config, project);
        this.strmJob = streamingJobManager.getStreamingJobByUuid(StreamingUtils.getJobId(modelId, jobType.name()));
        env.put(StreamingConstants.HADOOP_CONF_DIR, HadoopUtil.getHadoopConfDir());
        if (StringUtils.isEmpty(kylinJobJar) && !config.isUTEnv()) {
            throw new IllegalArgumentException("Missing kylin job jar");
        }
        this.launcher = new SparkLauncher(env);
        log.info("The {} - {} initialized successfully...", jobType, jobId);
    }

    public abstract void launch();

    public abstract void stop();

}
