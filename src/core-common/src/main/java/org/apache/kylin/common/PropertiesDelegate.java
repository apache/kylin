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

import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import io.kyligence.config.core.loader.IExternalConfigLoader;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class PropertiesDelegate extends Properties {

    @EqualsAndHashCode.Include
    private final Properties properties;

    @EqualsAndHashCode.Include
    private final transient IExternalConfigLoader configLoader;

    public PropertiesDelegate(Properties properties, IExternalConfigLoader configLoader) {
        this.properties = properties;
        this.configLoader = configLoader;
    }

    public synchronized void reloadProperties(Properties properties) {
        this.properties.clear();
        this.properties.putAll(properties);
    }

    @Override
    public String getProperty(String key) {
        String property = this.properties.getProperty(key);
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
    public synchronized Object put(Object key, Object value) {
        return this.properties.put(key, value);
    }

    @Override
    public synchronized Object setProperty(String key, String value) {
        return this.put(key, value);
    }

    @Override
    public Set<Map.Entry<Object, Object>> entrySet() {
        return getAllProperties().entrySet();
    }

    @Override
    public synchronized int size() {
        return getAllProperties().size();
    }

    @Override
    public synchronized Enumeration<Object> keys() {
        return getAllProperties().keys();
    }

    private synchronized Properties getAllProperties() {
        Properties propertiesView = new Properties();
        if (this.configLoader != null) {
            propertiesView.putAll(this.configLoader.getProperties());
        }
        propertiesView.putAll(this.properties);
        return propertiesView;
    }
}
