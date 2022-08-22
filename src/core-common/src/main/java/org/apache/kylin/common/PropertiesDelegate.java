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

import com.google.common.collect.Maps;
import io.kyligence.config.core.loader.IExternalConfigLoader;
import io.kyligence.config.external.loader.NacosExternalConfigLoader;
import lombok.EqualsAndHashCode;

import java.util.Collections;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

@EqualsAndHashCode
public class PropertiesDelegate extends Properties {

    @EqualsAndHashCode.Include
    private final ConcurrentMap<Object, Object> properties = Maps.newConcurrentMap();

    @EqualsAndHashCode.Include
    private final transient IExternalConfigLoader configLoader;

    public PropertiesDelegate(Properties properties, IExternalConfigLoader configLoader) {
        if(configLoader != null) this.properties.putAll(configLoader.getProperties());
        this.properties.putAll(properties);
        this.configLoader = configLoader;
    }

    public void reloadProperties(Properties properties) {
        this.properties.clear();
        if(configLoader != null) this.properties.putAll(configLoader.getProperties());
        this.properties.putAll(properties);
    }

    @Override
    public String getProperty(String key) {
        String property = (String) this.properties.get(key);
        if (property == null && this.configLoader != null) {
            return configLoader.getProperty(key);
        }
        return property;
    }

    @Override
    public String getProperty(String key, String defaultValue) {
        String property = this.getProperty(key);
        if (property == null) {
            return defaultValue;
        }
        return property;
    }

    @Override
    public Object put(Object key, Object value) {
        return this.properties.put(key, value);
    }

    @Override
    public Object setProperty(String key, String value) {
        return this.put(key, value);
    }

    @Override
    public Set<Map.Entry<Object, Object>> entrySet() {
        return getAllProperties().entrySet();
    }

    @Override
    public int size() {
        return getAllProperties().size();
    }

    @Override
    public Enumeration<Object> keys() {
        return Collections.enumeration(getAllProperties().keySet());
    }

    private ConcurrentMap<Object, Object> getAllProperties() {
        // When KylinExternalConfigLoader is enabled, properties is static
        if (configLoader == null || configLoader.getClass().equals(KylinExternalConfigLoader.class)) {
            return properties;
        } else if (configLoader.getClass().equals(NacosExternalConfigLoader.class)) {
            // When NacosExternalConfigLoader enabled, fetch config entries from remote for each call
            // TODO: Kylin should call remote server in periodically, otherwise query concurrency
            // maybe impacted badly
            ConcurrentMap<Object, Object> propertiesView = Maps.newConcurrentMap();
            propertiesView.putAll(this.configLoader.getProperties());
            propertiesView.putAll(this.properties);
            return propertiesView;
        } else {
            throw new IllegalArgumentException(configLoader.getClass() + " is not supported ");
        }
    }
}
