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
import java.util.Map;

import org.apache.kylin.common.exception.ErrorCodeException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ErrorSuggestion extends AbstractErrorContent {

    private static final Map<String, String> EN_MAP;
    // private static final Map<String, String> CN_MAP;
    private static final ThreadLocal<Map<String, String>> FRONT_MAP = new ThreadLocal<>();

    static {
        try {
            EN_MAP = loadProperties("kylin_error_suggestion_conf_en.properties");
            // CN_MAP = loadProperties("kylin_error_suggestion_conf_cn.properties");
            FRONT_MAP.set(EN_MAP);
        } catch (IOException e) {
            throw new ErrorCodeException("loading suggestion map failed.", e);
        }
    }

    public ErrorSuggestion(String keCode) {
        super(keCode);
    }

    public static void setMsg(String lang) {
        //        if (StringUtils.equalsIgnoreCase(CN_LANG, lang)) {
        //            FRONT_MAP.set(CN_MAP);
        //        } else {
        FRONT_MAP.set(EN_MAP);
        //        }
    }

    @Override
    public Map<String, String> getMap() {
        Map<String, String> res = FRONT_MAP.get();
        return res == null ? EN_MAP : res;
    }

    @Override
    public Map<String, String> getDefaultMap() {
        return EN_MAP;
    }
}
