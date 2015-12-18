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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.tools.OptionsHelper;
import org.apache.kylin.metadata.model.DataModelDesc.RealizationCapacity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author ysong1
 */
public class JobEngineConfig {
    private static final Logger logger = LoggerFactory.getLogger(JobEngineConfig.class);
    public static String HADOOP_JOB_CONF_FILENAME = "kylin_job_conf";

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

    public String getHadoopJobConfFilePath(RealizationCapacity capaticy, String projectName) throws IOException {
        String hadoopJobConfFile = (HADOOP_JOB_CONF_FILENAME + "_" + capaticy.toString().toLowerCase() + "_" + projectName + ".xml");
        File jobConfig = getJobConfig(hadoopJobConfFile);
        if (jobConfig == null || !jobConfig.exists()) {
            logger.warn("fail to locate " + hadoopJobConfFile);
            
            hadoopJobConfFile = (HADOOP_JOB_CONF_FILENAME + "_" + projectName + ".xml");
            logger.warn("trying to locate " + hadoopJobConfFile);
            jobConfig = getJobConfig(hadoopJobConfFile);
            if (jobConfig == null || !jobConfig.exists()) {
                logger.warn("fail to locate " + hadoopJobConfFile);
                
                hadoopJobConfFile = (HADOOP_JOB_CONF_FILENAME + "_" + capaticy.toString().toLowerCase() + ".xml");
                logger.warn("trying to locate " + hadoopJobConfFile);
                jobConfig = getJobConfig(hadoopJobConfFile);
                if (jobConfig == null || !jobConfig.exists()) {
                    logger.warn("fail to locate " + hadoopJobConfFile);
                    
                    hadoopJobConfFile = (HADOOP_JOB_CONF_FILENAME + ".xml");
                    logger.warn("trying to locate " + hadoopJobConfFile);
                    jobConfig = getJobConfig(hadoopJobConfFile);
                    if (jobConfig == null || !jobConfig.exists()) {
                        logger.warn("fail to locate " + hadoopJobConfFile);
                        throw new RuntimeException("fail to locate " + hadoopJobConfFile);
                    }
                }
            }
        }
        
        return OptionsHelper.convertToFileURL(jobConfig.getAbsolutePath());
    }

    private void inputStreamToFile(InputStream ins, File file) throws IOException {
        OutputStream os = new FileOutputStream(file);
        int bytesRead = 0;
        byte[] buffer = new byte[8192];
        while ((bytesRead = ins.read(buffer, 0, 8192)) != -1) {
            os.write(buffer, 0, bytesRead);
        }
        os.close();
        ins.close();
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
    public String getAdminDls() {
        return config.getAdminDls();
    }

    /**
     * @return the jobStepTimeout
     */
    public long getJobStepTimeout() {
        return config.getJobStepTimeout();
    }

    /**
     * @return the asyncJobCheckInterval
     */
    public int getAsyncJobCheckInterval() {
        return config.getYarnStatusCheckIntervalSeconds();
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
