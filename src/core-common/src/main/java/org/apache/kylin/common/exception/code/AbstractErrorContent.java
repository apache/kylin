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

package org.apache.kylin.common.exception.code;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.common.util.FileUtils;
import org.apache.kylin.common.util.ResourceUtils;

import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractErrorContent implements Serializable {

    private static final long serialVersionUID = 1L;

    static final String CN_LANG = "cn";

    protected static Map<String, String> loadProperties(String name) throws IOException {
        URL resource = ResourceUtils.getServerConfUrl(name);
        log.info("loading error code msg/suggestion map: {}", resource.getPath());
        Map<String, String> map = ImmutableMap
                .copyOf(new ConcurrentHashMap<>(FileUtils.readFromPropertiesFile(resource.openStream())));
        log.info("loading error code msg/suggestion map successful: {}", resource.getPath());
        return map;
    }

    protected final String keCode;

    protected AbstractErrorContent(String keCode) {
        this.keCode = keCode;
    }

    public abstract Map<String, String> getMap();

    public abstract Map<String, String> getDefaultMap();

    public String getLocalizedString() {
        return getMap().getOrDefault(keCode, "unknown msg");
    }

    public String getString() {
        return getDefaultMap().getOrDefault(keCode, "unknown msg");
    }

    public String getCodeString() {
        return keCode;
    }
}
