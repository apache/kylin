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

import java.util.Map;

import org.springframework.core.env.Environment;

import com.alibaba.cloud.nacos.NacosConfigProperties;
import com.google.common.collect.ImmutableMap;

import io.kyligence.config.external.loader.NacosExternalConfigLoader;

public class CachedNacosExternalConfigLoader extends NacosExternalConfigLoader implements ICachedExternalConfigLoader {

    private final ImmutableMap<Object, Object> propertyEntries;

    public CachedNacosExternalConfigLoader(Map<String, String> properties, NacosConfigProperties nacosConfigProperties,
                                           Environment environment) {
        super(properties, nacosConfigProperties, environment);
        this.propertyEntries = ImmutableMap.copyOf(super.getProperties());
    }

    @Override
    public ImmutableMap getPropertyEntries() {
        return propertyEntries;
    }
}