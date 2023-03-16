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

package org.apache.kylin.common;

import static org.apache.kylin.common.KylinConfig.getSitePropertiesFile;
import static org.apache.kylin.common.KylinConfigBase.BCC;
import static org.apache.kylin.common.exception.CommonErrorCode.UNKNOWN_ERROR_CODE;
import static org.apache.kylin.common.exception.ServerErrorCode.FILE_NOT_EXIST;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import javax.annotation.Nonnull;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.OrderedProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;
import org.apache.kylin.guava30.shaded.common.collect.Maps;

public class KylinExternalConfigLoader implements ICachedExternalConfigLoader {

    private static final long serialVersionUID = 1694879531312203159L;

    private static final Logger logger = LoggerFactory.getLogger(KylinExternalConfigLoader.class);

    public static final String KYLIN_CONF_PROPERTIES_FILE = "kylin.properties";

    private final File propFile;

    private final Map<String, String> properties;

    private final ImmutableMap<Object, Object> propertyEntries;

    public KylinExternalConfigLoader(Map<String, String> map) {
        this(map.get("config-dir") == null ? getSitePropertiesFile() : new File(map.get("config-dir")));
    }

    public KylinExternalConfigLoader(File file) {
        this.propFile = file;
        this.properties = loadProperties();
        this.propertyEntries = ImmutableMap.copyOf(properties);
    }

    private Map<String, String> loadProperties() {
        Map<String, String> siteProperties = Maps.newConcurrentMap();
        OrderedProperties orderedProperties = buildSiteOrderedProps();
        for (Map.Entry<String, String> each : orderedProperties.entrySet()) {
            siteProperties.put(String.valueOf(each.getKey()), String.valueOf(each.getValue()));
        }
        return siteProperties;
    }

    // build kylin properties from site deployment, a.k.a. KYLIN_HOME/conf/kylin.properties
    private OrderedProperties buildSiteOrderedProps() {
        try {
            // 1. load default configurations from classpath.
            // we have a kylin-defaults.properties in kylin/core-common/src/main/resources
            URL resource = Thread.currentThread().getContextClassLoader().getResource("kylin-defaults.properties");
            Preconditions.checkNotNull(resource);
            logger.info("Loading kylin-defaults.properties from {}", resource.getPath());
            OrderedProperties orderedProperties = new OrderedProperties();
            loadAndTrimProperties(resource.openStream(), orderedProperties);

            for (int i = 0; i < 10; i++) {
                String fileName = "kylin-defaults" + (i) + ".properties";
                URL additionalResource = Thread.currentThread().getContextClassLoader().getResource(fileName);
                if (additionalResource != null) {
                    logger.info("Loading {} from {} ", fileName, additionalResource.getPath());
                    loadAndTrimProperties(additionalResource.openStream(), orderedProperties);
                }
            }

            // 2. load site conf, to keep backward compatibility it's still named kylin.properties
            // actually it's better to be named kylin-site.properties
            if (propFile == null || !propFile.exists()) {
                logger.error("fail to locate " + KYLIN_CONF_PROPERTIES_FILE);
                throw new KylinConfigCannotInitException("fail to locate " + KYLIN_CONF_PROPERTIES_FILE);
            }
            loadAndTrimProperties(Files.newInputStream(propFile.toPath()), orderedProperties);

            // 3. still support kylin.properties.override as secondary override
            // not suggest to use it anymore
            File propOverrideFile = new File(propFile.getParentFile(), propFile.getName() + ".override");
            if (propOverrideFile.exists()) {
                loadAndTrimProperties(Files.newInputStream(propOverrideFile.toPath()), orderedProperties);
            }

            return orderedProperties;
        } catch (IOException e) {
            throw new KylinException(FILE_NOT_EXIST, e);
        }
    }

    private static OrderedProperties loadPropertiesFromInputStream(InputStream inputStream) {
        try (BufferedReader confReader = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            OrderedProperties temp = new OrderedProperties();
            temp.load(confReader);
            return temp;
        } catch (Exception e) {
            throw new KylinException(FILE_NOT_EXIST, e);
        }
    }

    /**
     * 1.load props from InputStream
     * 2.trim all key-value
     * 3.backward compatibility check
     * 4.close the passed in inputStream
     * @param inputStream
     * @param properties
     */
    private static void loadAndTrimProperties(@Nonnull InputStream inputStream, @Nonnull OrderedProperties properties) {
        Preconditions.checkNotNull(inputStream);
        Preconditions.checkNotNull(properties);
        try {
            OrderedProperties trimProps = OrderedProperties.copyAndTrim(loadPropertiesFromInputStream(inputStream));
            properties.putAll(BCC.check(trimProps));
        } catch (Exception e) {
            throw new KylinException(UNKNOWN_ERROR_CODE, " loadAndTrimProperties error ", e);
        }
    }

    @Override
    public String getConfig() {
        StringWriter writer = new StringWriter();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            writer.append(entry.getKey() + "=" + entry.getValue()).append("\n");
        }
        return writer.toString();
    }

    @Override
    public String getProperty(String key) {
        return Objects.toString(properties.get(key), null);
    }

    /**
     * @see #getPropertyEntries
     * @deprecated
     */
    @Override
    @Deprecated
    public Properties getProperties() {
        Properties newProperties = new Properties();
        newProperties.putAll(properties);
        return newProperties;
    }

    @Override
    public ImmutableMap<Object, Object> getPropertyEntries() {
        return propertyEntries;
    }
}
