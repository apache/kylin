/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.job.engine;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.job.tools.OptionsHelper;
import com.kylinolap.metadata.model.cube.CubeDesc.CubeCapacity;

/**
 * @author ysong1
 *
 */
public class JobEngineConfig {
    private static final Logger logger = LoggerFactory.getLogger(JobEngineConfig.class);
    public static String HADOOP_JOB_CONF_FILENAME = "hadoop_job_conf_";

    public String getHadoopJobConfFilePath(CubeCapacity capaticy) throws IOException {
        String hadoopJobConfFile = (HADOOP_JOB_CONF_FILENAME + capaticy.toString().toLowerCase() + ".xml");
        String path = System.getProperty(KylinConfig.KYLIN_CONF);

        if (path == null) {
            path = System.getenv(KylinConfig.KYLIN_CONF);
        }

        if (path != null) {
            path = path + File.separator + hadoopJobConfFile;
        }

        if (null == path || !new File(path).exists()) {
            File defaultFilePath = new File("/etc/kylin/" + hadoopJobConfFile);

            if (defaultFilePath.exists()) {
                path = defaultFilePath.getAbsolutePath();
            } else {
                logger.debug("Search conf file " + hadoopJobConfFile + "  from classpath ...");
                InputStream is =
                        JobEngineConfig.class.getClassLoader().getResourceAsStream(hadoopJobConfFile);
                if (is == null) {
                    logger.debug("Can't get " + hadoopJobConfFile + " from classpath");
                    logger.debug("No " + hadoopJobConfFile + " file were found");
                } else {
                    File tmp = File.createTempFile("hadoop_job_conf", ".xml");
                    inputStreamToFile(is, tmp);
                    path = tmp.getAbsolutePath();
                }
            }
        }

        if (null == path || !new File(path).exists()) {
            return "";
        }

        return OptionsHelper.convertToFileURL(path);
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
    private final String hdfsWorkingDirectory;
    private final String kylinJobJarPath;
    private final boolean runAsRemoteCommand;
    private final String zookeeperString;
    private final String remoteHadoopCliHostname;
    private final String remoteHadoopCliUsername;
    private final String remoteHadoopCliPassword;
    private final String yarnStatusServiceUrl;
    private final int maxConcurrentJobLimit;
    private final String timeZone;
    private final String adminDls;
    private final long jobStepTimeout;
    private final int asyncJobCheckInterval;
    private final boolean flatTableByHive;

    public JobEngineConfig(KylinConfig kylinConfig) {
        this.config = kylinConfig;
        this.hdfsWorkingDirectory = kylinConfig.getHdfsWorkingDirectory();
        this.kylinJobJarPath = kylinConfig.getKylinJobJarPath();
        this.runAsRemoteCommand = kylinConfig.getRunAsRemoteCommand();
        this.zookeeperString = kylinConfig.getZookeeperString();
        this.remoteHadoopCliHostname = kylinConfig.getRemoteHadoopCliHostname();
        this.remoteHadoopCliUsername = kylinConfig.getRemoteHadoopCliUsername();
        this.remoteHadoopCliPassword = kylinConfig.getRemoteHadoopCliPassword();
        this.yarnStatusServiceUrl = kylinConfig.getYarnStatusServiceUrl();
        this.maxConcurrentJobLimit = kylinConfig.getMaxConcurrentJobLimit();
        this.timeZone = kylinConfig.getTimeZone();
        this.adminDls = kylinConfig.getAdminDls();
        this.jobStepTimeout = kylinConfig.getJobStepTimeout();
        this.asyncJobCheckInterval = kylinConfig.getYarnStatusCheckIntervalSeconds();
        this.flatTableByHive = kylinConfig.getFlatTableByHive();
    }

    public KylinConfig getConfig() {
        return config;
    }

    /**
     * @return the hdfsWorkingDirectory
     */
    public String getHdfsWorkingDirectory() {
        return hdfsWorkingDirectory;
    }

    /**
     * @return the kylinJobJarPath
     */
    public String getKylinJobJarPath() {
        return kylinJobJarPath;
    }

    /**
     * @return the runAsRemoteCommand
     */
    public boolean isRunAsRemoteCommand() {
        return runAsRemoteCommand;
    }

    /**
     * @return the zookeeperString
     */
    public String getZookeeperString() {
        return zookeeperString;
    }

    /**
     * @return the remoteHadoopCliHostname
     */
    public String getRemoteHadoopCliHostname() {
        return remoteHadoopCliHostname;
    }

    /**
     * @return the remoteHadoopCliUsername
     */
    public String getRemoteHadoopCliUsername() {
        return remoteHadoopCliUsername;
    }

    /**
     * @return the remoteHadoopCliPassword
     */
    public String getRemoteHadoopCliPassword() {
        return remoteHadoopCliPassword;
    }

    /**
     * @return the yarnStatusServiceUrl
     */
    public String getYarnStatusServiceUrl() {
        return yarnStatusServiceUrl;
    }

    /**
     * @return the maxConcurrentJobLimit
     */
    public int getMaxConcurrentJobLimit() {
        return maxConcurrentJobLimit;
    }

    /**
     * @return the timeZone
     */
    public String getTimeZone() {
        return timeZone;
    }

    /**
     * @return the adminDls
     */
    public String getAdminDls() {
        return adminDls;
    }

    /**
     * @return the jobStepTimeout
     */
    public long getJobStepTimeout() {
        return jobStepTimeout;
    }

    /**
     * @return the flatTableByHive
     */
    public boolean isFlatTableByHive() {
        return flatTableByHive;
    }

    /**
     * @return the asyncJobCheckInterval
     */
    public int getAsyncJobCheckInterval() {
        return asyncJobCheckInterval;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((config == null) ? 0 : config.hashCode());
        return result;
    }

    /* (non-Javadoc)
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