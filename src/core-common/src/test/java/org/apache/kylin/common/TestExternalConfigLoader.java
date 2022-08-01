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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;

import io.kyligence.config.core.loader.IExternalConfigLoader;

public class TestExternalConfigLoader implements IExternalConfigLoader {
    private Properties properties;

    public TestExternalConfigLoader(Properties properties) {
        this.properties = properties;
    }

    @Override
    public String getConfig() {
        StringWriter writer = new StringWriter();
        properties.list(new PrintWriter(writer));
        return writer.toString();
    }

    @Override
    public String getProperty(String s) {
        return properties.getProperty(s);
    }

    @Override
    public Properties getProperties() {
        return properties;
    }
}
