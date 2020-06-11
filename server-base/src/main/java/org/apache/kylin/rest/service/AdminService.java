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
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.OrderedProperties;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.job.StorageCleanupJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import org.apache.kylin.shaded.com.google.common.collect.Lists;

/**
 */
@Component("adminService")
public class AdminService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(AdminService.class);

    /**
     * Get Java Env info as string
     */
    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public String getEnv() throws ConfigurationException, UnsupportedEncodingException {
        PropertiesConfiguration tempConfig = new PropertiesConfiguration();
        OrderedProperties orderedProperties = new OrderedProperties(new TreeMap<String, String>());
        // Add Java Env

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
        content = baos.toString("UTF-8");
        return content;
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public void updateConfig(String key, String value) {
        logger.debug("Update Kylin Runtime Config, key=" + key + ", value=" + value);

        KylinConfig.getInstanceFromEnv().setProperty(key, value);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public boolean configWritableStatus() {
        return KylinConfig.getInstanceFromEnv().isWebConfigEnabled();
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public void cleanupStorage() {
        StorageCleanupJob job = null;
        try {
            job = new StorageCleanupJob();
        } catch (IOException e) {
            throw new RuntimeException("Can not init StorageCleanupJob", e);
        }
        String[] args = new String[] { "-delete", "true" };
        job.execute(args);
    }

    public String getPublicConfig() throws IOException {
        final String whiteListProperties = KylinConfig.getInstanceFromEnv().getPropertiesWhiteList();

        Collection<String> propertyKeys = Lists.newArrayList();
        if (StringUtils.isNotEmpty(whiteListProperties)) {
            propertyKeys.addAll(Arrays.asList(StringUtil.splitByComma(whiteListProperties)));
        }

        return KylinConfig.getInstanceFromEnv().exportToString(propertyKeys);
    }
}
