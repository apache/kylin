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

package org.apache.kylin.tool;

import java.util.Map;
import java.util.Properties;

import org.apache.kylin.common.BackwardCompatibilityConfig;
import org.apache.kylin.common.KylinConfig;

import org.apache.kylin.shaded.com.google.common.collect.Maps;

public class KylinConfigCLI {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: KylinConfigCLI conf_name");
            System.err.println("Example: KylinConfigCLI kylin.server.mode");
            System.exit(1);
        }

        Properties config = KylinConfig.getInstanceFromEnv().exportToProperties();
        
        BackwardCompatibilityConfig bcc = new BackwardCompatibilityConfig();
        String key = bcc.check(args[0].trim());
        if (!key.endsWith(".")) {
            String value = config.getProperty(key);
            if (value == null) {
                value = "";
            }
            System.out.println(value.trim());
        } else {
            Map<String, String> props = getPropertiesByPrefix(config, key);
            for (Map.Entry<String, String> prop : props.entrySet()) {
                System.out.println(prop.getKey() + "=" + prop.getValue().trim());
            }
        }
    }

    private static Map<String, String> getPropertiesByPrefix(Properties props, String prefix) {
        Map<String, String> result = Maps.newLinkedHashMap();
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            String entryKey = (String) entry.getKey();
            if (entryKey.startsWith(prefix)) {
                result.put(entryKey.substring(prefix.length()), (String) entry.getValue());
            }
        }
        return result;
    }
}
