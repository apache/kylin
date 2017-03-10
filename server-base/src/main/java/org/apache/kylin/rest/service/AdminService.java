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

package org.apache.kylin.rest.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.util.OrderedProperties;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.tool.StorageCleanupJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

/**
 * @author jianliu
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
        PropertiesConfiguration tempConfig = new PropertiesConfiguration();
        OrderedProperties orderedProperties = new OrderedProperties(new TreeMap<String, String>());
        // Add Java Env

        try {
            String content = "";
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            // env
            Map<String, String> env = System.getenv();

            for (Map.Entry<String, String> entry : env.entrySet()) {
                orderedProperties.setProperty(entry.getKey(), entry.getValue());
            }

            // properties
            Properties properties = System.getProperties();

            for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                orderedProperties.setProperty((String) entry.getKey(), (String) entry.getValue());
            }

            for (Map.Entry<String, String> entry : orderedProperties.entrySet()) {
                tempConfig.addProperty(entry.getKey(), entry.getValue());
            }

            // do save
            tempConfig.save(baos);
            content = baos.toString();
            return content;
        } catch (ConfigurationException e) {
            throw new InternalErrorException("Failed to get Kylin env Config", e);
        }
    }

    /**
     * Get Java config info as String
     *
     * @return
     */
    // @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public String getConfigAsString() {
        logger.debug("Get Kylin Runtime Config");

        try {
            return getAllConfigAsString();
        } catch (IOException e) {
            throw new InternalErrorException("Failed to get Kylin Runtime Config", e);
        }
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public void cleanupStorage() {
        StorageCleanupJob job = new StorageCleanupJob();
        String[] args = new String[] { "-delete", "true" };
        try {
            job.execute(args);
        } catch (Exception e) {
            throw new InternalErrorException(e.getMessage(), e);
        }
    }

    private String getAllConfigAsString() throws IOException {
        Properties allProps;
        try {
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            Method getAllMethod = KylinConfigBase.class.getDeclaredMethod("getAllProperties");
            getAllMethod.setAccessible(true);
            allProps = (Properties) getAllMethod.invoke(kylinConfig);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        OrderedProperties orderedProperties = KylinConfig.getKylinOrderedProperties();

        final StringBuilder sb = new StringBuilder();

        for (Map.Entry<Object, Object> entry : allProps.entrySet()) {
            String key = entry.getKey().toString();
            String value = entry.getValue().toString();
            if (!orderedProperties.containsProperty(key)) {
                orderedProperties.setProperty(key, value);
            } else if (!orderedProperties.getProperty(key).equalsIgnoreCase(value))
                orderedProperties.setProperty(key, value);
        }
        for (Map.Entry<String, String> entry : orderedProperties.entrySet()) {
            sb.append(entry.getKey() + "=" + entry.getValue()).append('\n');
        }
        return sb.toString();
    }
}
