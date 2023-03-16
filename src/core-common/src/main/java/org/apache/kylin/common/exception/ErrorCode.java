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
import java.util.Locale;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.FileUtils;
import org.apache.kylin.common.util.ResourceUtils;

import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ErrorCode implements Serializable {

    private static final String CN_LANG = "cn";
    private static final String EN_ERROR_CODE_FILE = "kylin_errorcode_conf_en.properties";
    private static final String ZH_ERROR_CODE_FILE = "kylin_errorcode_conf_zh.properties";
    private static final String ZH_SEPARATOR = "：";
    private static final String EN_SEPARATOR = ":";
    private static final ImmutableMap<String, String> EN_MAP;
    private static final ImmutableMap<String, String> ZH_MAP;
    private static final ThreadLocal<ImmutableMap<String, String>> FRONT_MAP = new ThreadLocal<>();

    static {
        try {
            URL resource = ResourceUtils.getServerConfUrl(EN_ERROR_CODE_FILE);
            log.info("loading enMap {}", resource.getPath());
            EN_MAP = ImmutableMap
                    .copyOf(new ConcurrentHashMap<>(FileUtils.readFromPropertiesFile(resource.openStream())));
            log.info("loading enMap successful");
            resource = ResourceUtils.getServerConfUrl(ZH_ERROR_CODE_FILE);
            log.info("loading zhMap {}", resource.getPath());
            ZH_MAP = ImmutableMap
                    .copyOf(new ConcurrentHashMap<>(FileUtils.readFromPropertiesFile(resource.openStream())));
            log.info("loading zhMap successful");
            FRONT_MAP.set(EN_MAP);
        } catch (IOException e) {
            throw new ErrorCodeException("loading old error code failed.", e);
        }
    }

    private final String keCode;

    public ErrorCode(String keCode) {
        this.keCode = keCode;
    }

    public static void setMsg(String lang) {
        if (StringUtils.equalsIgnoreCase(CN_LANG, lang)) {
            FRONT_MAP.set(ZH_MAP);
        } else {
            FRONT_MAP.set(EN_MAP);
        }
    }

    private static ImmutableMap<String, String> getMap() {
        ImmutableMap<String, String> res = FRONT_MAP.get();
        return res == null ? EN_MAP : res;
    }

    public String getLocalizedString() {
        ImmutableMap<String, String> res = getMap();
        String description = res.getOrDefault(keCode, "unknown");
        String separator = ZH_MAP.equals(res) ? ZH_SEPARATOR : EN_SEPARATOR;
        return String.format(Locale.ROOT, "%s(%s)%s", keCode, description, separator);
    }

    public static String getLocalizedString(String keCode) {
        ImmutableMap<String, String> res = getMap();
        String description = res.getOrDefault(keCode, "unknown");
        String separator = ZH_MAP.equals(res) ? ZH_SEPARATOR : EN_SEPARATOR;
        return String.format(Locale.ROOT, "%s(%s)%s", keCode, description, separator);
    }

    public String getString() {
        String description = EN_MAP.getOrDefault(keCode, "unknown");
        return String.format(Locale.ROOT, "%s(%s)", keCode, description);
    }

    public String getCodeString() {
        return keCode;
    }

}
