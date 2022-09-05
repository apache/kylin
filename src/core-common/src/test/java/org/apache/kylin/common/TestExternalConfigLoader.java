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

import static org.apache.kylin.common.exception.CommonErrorCode.UNKNOWN_ERROR_CODE;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Properties;

import org.apache.kylin.common.exception.KylinException;

import com.google.common.collect.ImmutableMap;

public class TestExternalConfigLoader implements ICachedExternalConfigLoader {
    private final Properties properties;
    private final ImmutableMap<Object, Object> propertyEntries;

    public TestExternalConfigLoader(Properties properties) {
        this.properties = properties;
        this.propertyEntries = ImmutableMap.copyOf(properties);
    }

    @Override
    public String getConfig() {
        StringWriter writer = new StringWriter();
        try {
            properties.store(writer, "");
        } catch (IOException e) {
            throw new KylinException(UNKNOWN_ERROR_CODE, e);
        }
        return writer.getBuffer().toString();
    }

    @Override
    public String getProperty(String s) {
        return properties.getProperty(s);
    }

    @Override
    @Deprecated
    public Properties getProperties() {
        return properties;
    }

    @Override
    public ImmutableMap getPropertyEntries() {
        return propertyEntries;
    }
}
