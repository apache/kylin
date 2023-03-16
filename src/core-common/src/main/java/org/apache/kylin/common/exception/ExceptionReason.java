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

package org.apache.kylin.common.exception;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.common.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;

public class ExceptionReason implements Serializable {
    public static final Logger logger = LoggerFactory.getLogger(ExceptionReason.class);
    private static final String EN_EXCEPTION_REASON_FILE = "job_exception_reason_en.properties";
    private static final String ZH_EXCEPTION_REASON_FILE = "job_exception_reason_zh.properties";
    private static final ImmutableMap<String, String> enMap;
    private static final ImmutableMap<String, String> zhMap;
    private static final ThreadLocal<ImmutableMap<String, String>> frontMap = new ThreadLocal<>();

    static {
        try {
            URL resource = Thread.currentThread().getContextClassLoader().getResource(EN_EXCEPTION_REASON_FILE);
            Preconditions.checkNotNull(resource);
            logger.info("loading reason enMap {}", resource.getPath());
            enMap = ImmutableMap
                    .copyOf(new ConcurrentHashMap<>(FileUtils.readFromPropertiesFile(resource.openStream())));
            logger.info("loading reason enMap successful");
            resource = Thread.currentThread().getContextClassLoader().getResource(ZH_EXCEPTION_REASON_FILE);
            Preconditions.checkNotNull(resource);
            logger.info("loading reason zhMap {}", resource.getPath());
            zhMap = ImmutableMap
                    .copyOf(new ConcurrentHashMap<>(FileUtils.readFromPropertiesFile(resource.openStream())));
            logger.info("loading reason zhMap successful");
            frontMap.set(enMap);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private final String keCode;

    public ExceptionReason(String keCode) {
        this.keCode = keCode;
    }

    public static void setLang(String lang) {
        if ("cn".equals(lang)) {
            frontMap.set(zhMap);
        } else {
            frontMap.set(enMap);
        }
    }

    private static ImmutableMap<String, String> getMap() {
        ImmutableMap<String, String> res = frontMap.get();
        return res == null ? enMap : res;
    }

    public static String getReason(String keCode) {
        ImmutableMap<String, String> res = getMap();
        return res.getOrDefault(keCode, "unknown");
    }

    public String getReason() {
        ImmutableMap<String, String> res = getMap();
        return res.getOrDefault(keCode, "unknown");
    }

    public String getCodeString() {
        return keCode;
    }
}
