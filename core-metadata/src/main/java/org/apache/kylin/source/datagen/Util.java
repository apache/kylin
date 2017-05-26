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

package org.apache.kylin.source.datagen;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

public class Util {

    static Map<String, String> parseEqualCommaPairs(String equalCommaPairs, String defaultKey) {
        Map<String, String> r = new LinkedHashMap<>();
        
        if (StringUtils.isBlank(equalCommaPairs))
            return r;

        for (String s : equalCommaPairs.split(",")) {
            int equal = s.indexOf("=");
            if (equal < 0) {
                if (r.containsKey(defaultKey))
                    r.put(s.trim(), "true");
                else
                    r.put(defaultKey, s.trim());
            } else {
                r.put(s.substring(0, equal).trim(), s.substring(equal + 1).trim());
            }
        }
        return r;
    }

    static double parseDouble(Map<String, String> config, String key, double dft) {
        if (config.containsKey(key))
            return Double.parseDouble(config.get(key));
        else
            return dft;
    }

    static boolean parseBoolean(Map<String, String> config, String key, boolean dft) {
        if (config.containsKey(key))
            return Boolean.parseBoolean(config.get(key));
        else
            return dft;
    }

    public static int parseInt(Map<String, String> config, String key, int dft) {
        if (config.containsKey(key))
            return Integer.parseInt(config.get(key));
        else
            return dft;
    }

    public static String parseString(Map<String, String> config, String key, String dft) {
        if (config.containsKey(key))
            return config.get(key);
        else
            return dft;
    }
}
