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

package org.apache.kylin.job.engine;

import java.io.File;
import java.io.IOException;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.OptionsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ysong1
 */
public class JobEngineConfig {
    private static final Logger logger = LoggerFactory.getLogger(JobEngineConfig.class);
    public static final String HADOOP_JOB_CONF_FILENAME = "kylin_job_conf";
    public static final String DEFAUL_JOB_CONF_SUFFIX = "";
    public static final String IN_MEM_JOB_CONF_SUFFIX = "inmem";

    private static File getJobConfig(String fileName) {
        String path = System.getProperty(KylinConfig.KYLIN_CONF);
        if (StringUtils.isNotEmpty(path)) {
            return new File(path, fileName);
        }

        path = KylinConfig.getKylinHome();
        if (StringUtils.isNotEmpty(path)) {
            return new File(path + File.separator + "conf", fileName);
        }
        return null;
    }

    private String getHadoopJobConfFilePath(String suffix, boolean appendSuffix) {
        String hadoopJobConfFile;
        if (suffix != null && appendSuffix) {
            hadoopJobConfFile = (HADOOP_JOB_CONF_FILENAME + "_" + suffix.toLowerCase(Locale.ROOT) + ".xml");
        } else {
            hadoopJobConfFile = (HADOOP_JOB_CONF_FILENAME + ".xml");
        }

        File jobConfig = getJobConfig(hadoopJobConfFile);
        if (jobConfig == null || !jobConfig.exists()) {
            logger.warn("fail to locate {}, trying to locate {}.xml", hadoopJobConfFile, HADOOP_JOB_CONF_FILENAME);
            jobConfig = getJobConfig(HADOOP_JOB_CONF_FILENAME + ".xml");
            if (jobConfig == null || !jobConfig.exists()) {
                logger.error("fail to locate " + HADOOP_JOB_CONF_FILENAME + ".xml");
                throw new RuntimeException("fail to locate " + HADOOP_JOB_CONF_FILENAME + ".xml");
            }
        }
        return OptionsHelper.convertToFileURL(jobConfig.getAbsolutePath());
    }

    /**
     *
     * @param jobType job config file suffix name; if be null, will use the default job conf
     * @return the job config file path
     * @throws IOException
     */
    public String getHadoopJobConfFilePath(String jobType) throws IOException {
        String suffix = null;
        if (!StringUtils.isEmpty(jobType)) {
            suffix = jobType;
        }

        String path = getHadoopJobConfFilePath(suffix, true);
        if (StringUtils.isEmpty(path)) {
            path = getHadoopJobConfFilePath(jobType, true);
            if (StringUtils.isEmpty(path)) {
                path = getHadoopJobConfFilePath(jobType, false);
                if (StringUtils.isEmpty(path)) {
                    path = "";
                }
            }
        }
        return path;
    }

    // there should be no setters
    private final KylinConfig config;

    public JobEngineConfig(KylinConfig config) {
        this.config = config;
    }

    public KylinConfig getConfig() {
        return config;
    }

    public String getHdfsWorkingDirectory() {
        return config.getHdfsWorkingDirectory();
    }

    /**
     * @return the maxConcurrentJobLimit
     */
    public int getMaxConcurrentJobLimit() {
        return config.getMaxConcurrentJobLimit();
    }

    /**
     * @return the timeZone
     */
    public String getTimeZone() {
        return config.getTimeZone();
    }

    /**
     * @return the adminDls
     */
    public String[] getAdminDls() {
        return config.getAdminDls();
    }

    public int getPollIntervalSecond() {
        return config.getSchedulerPollIntervalSecond();
    }

    public String getServerMode() {
        return config.getServerMode();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((config == null) ? 0 : config.hashCode());
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        JobEngineConfig other = (JobEngineConfig) obj;
        if (config == null) {
            if (other.config != null)
                return false;
        } else if (!config.equals(other.config))
            return false;
        return true;
    }
}
