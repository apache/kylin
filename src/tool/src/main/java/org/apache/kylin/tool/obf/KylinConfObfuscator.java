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
package org.apache.kylin.tool.obf;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.tool.constant.SensitiveConfigKeysConstant;

import org.apache.kylin.guava30.shaded.common.collect.Maps;

public class KylinConfObfuscator extends PropertiesFileObfuscator {

    private final String[] sensitiveKeywords = { "password", "user", "zookeeper.zk-auth", "source.jdbc.pass" };

    public KylinConfObfuscator(ObfLevel level, MappingRecorder recorder, ResultRecorder resultRecorder) {
        super(level, recorder, resultRecorder);
    }

    @Override
    void obfuscateProperties(Properties input) {
        Map<Object, Object> item = Maps.newHashMap();
        for (Map.Entry<Object, Object> entry : input.entrySet()) {
            ACTION action = checkConfEntry(entry.getKey().toString(), entry.getValue().toString());
            switch (action) {
            case NONE:
                break;
            case META_JDBC:
                item.put(entry.getKey(), action.masked);
                break;
            case SENSITIVE_KEY:
                item.put(entry.getKey(), action.masked);
                break;
            default:
                break;
            }
        }
        input.putAll(item);
    }

    private ACTION checkConfEntry(String name, String value) {
        if (name.equals("kylin.metadata.url") && value.contains("@jdbc")) { // for example: jdbc password may be set in kylin.metadata.url
            ACTION act = ACTION.META_JDBC;
            try {
                act.masked = simpleToString(StorageURL.valueOf(value));
            } catch (Exception e) {
                act.masked = SensitiveConfigKeysConstant.HIDDEN;
                logger.error("Failed to parse metadata url", e);
            }
            return act;
        } else {
            for (String conf : SensitiveConfigKeysConstant.DEFAULT_SENSITIVE_CONF_BLACKLIST) {
                if (StringUtils.equals(conf, name)) {
                    return ACTION.NONE;
                }
            }
            for (String keyWord : sensitiveKeywords) {
                if ((name.contains(keyWord))) {
                    ACTION act = ACTION.SENSITIVE_KEY;
                    act.masked = SensitiveConfigKeysConstant.HIDDEN;
                    return act;
                }
            }

        }
        return ACTION.NONE;
    }

    private String simpleToString(StorageURL storageURL) {
        StringBuilder sb = new StringBuilder();
        sb.append(storageURL.getIdentifier());
        if (storageURL.getScheme() != null)
            sb.append("@").append(storageURL.getScheme());
        for (Map.Entry<String, String> kv : storageURL.getAllParameters().entrySet()) {
            String key = kv.getKey();
            String val = kv.getValue();
            if (key.equalsIgnoreCase("username") || key.equalsIgnoreCase("password")) {
                val = SensitiveConfigKeysConstant.HIDDEN;
            }
            sb.append(",").append(key).append("=");
            if (!val.isEmpty()) {
                sb.append(val);
            }
        }
        return sb.toString();
    }

    enum ACTION {
        NONE, META_JDBC, SENSITIVE_KEY;

        String masked = SensitiveConfigKeysConstant.HIDDEN;
    }
}
