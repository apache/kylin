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

package com.kylinolap.rest.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.job.hadoop.cube.StorageCleanupJob;
import com.kylinolap.rest.constant.Constant;
import com.kylinolap.rest.exception.InternalErrorException;

/**
 * @author jianliu
 *
 */
@Component("adminService")
public class AdminService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(AdminService.class);

    /**
     * Get Java Env info as string
     * 
     * @return
     */
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public String getEnv() {
        logger.debug("Get Kylin Runtime environment");
        PropertiesConfiguration tempConfig = new PropertiesConfiguration();

        //Add Java Env

        try {
            String content = "";
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            //env
            Map<String, String> env = System.getenv();
            for (String envName : env.keySet()) {
                tempConfig.addProperty(envName, env.get(envName));
            }
            //properties
            Properties properteis = System.getProperties();
            for (Object propName : properteis.keySet()) {
                tempConfig.setProperty((String) propName, properteis.get(propName));
            }

            //do save
            tempConfig.save(baos);
            content = baos.toString();
            return content;
        } catch (ConfigurationException e) {
            logger.debug("Failed to get Kylin Runtime env", e);
            throw new InternalErrorException("Failed to get Kylin env Config", e);
        }
    }

    /**
     * Get Java config info as String
     * 
     * @return
     */
    //@PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public String getConfigAsString() {
        logger.debug("Get Kylin Runtime Config");

        try {
            return KylinConfig.getInstanceFromEnv().getConfigAsString();
        } catch (IOException e) {
            logger.debug("Failed to get Kylin Runtime Config", e);
            throw new InternalErrorException("Failed to get Kylin Runtime Config", e);
        }
    }

    public void cleanupStorage() {
        StorageCleanupJob job = new StorageCleanupJob();
        String[] args = new String[] { "-delete", "true" };
        try {
            ToolRunner.run(job, args);
        } catch (Exception e) {
            logger.error(e.getLocalizedMessage(), e);
            throw new InternalErrorException(e);
        }
    }
}
