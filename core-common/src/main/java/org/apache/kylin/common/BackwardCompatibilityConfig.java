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

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class BackwardCompatibilityConfig {

    private static final Logger logger = LoggerFactory.getLogger(BackwardCompatibilityConfig.class);
    
    private final Map<String, String> old2new = Maps.newConcurrentMap();
    
    public BackwardCompatibilityConfig() {
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("kylin-backward-compatibility.properties");
        init(is);
    }
    
    // for test
    BackwardCompatibilityConfig(InputStream is) {
        init(is);
    }
    
    private void init(InputStream is) {
        if (is == null)
            return;
        
        Properties props = new Properties();
        try {
            props.load(is);
        } catch (IOException e) {
            logger.error("", e);
        } finally {
            IOUtils.closeQuietly(is);
        }
        
        for (Entry<Object, Object> kv : props.entrySet()) {
            old2new.put((String) kv.getKey(), (String) kv.getValue());
        }
    }
    
    public String check(String key) {
        String newKey = old2new.get(key);
        if (newKey != null) {
            logger.warn("Config '{}' is deprecated, use '{}' instead", key, newKey);
            return newKey;
        } else {
            return key;
        }
    }
    
    public Map<String, String> check(Map<String, String> props) {
        if (containsOldKey(props.keySet()) == false)
            return props;
        
        LinkedHashMap<String, String> result = new LinkedHashMap<>();
        for (Entry<String, String> kv : props.entrySet()) {
            result.put(check(kv.getKey()), kv.getValue());
        }
        return result;
    }
    
    public Properties check(Properties props) {
        if (containsOldKey(props.keySet()) == false)
            return props;
        
        Properties result = new Properties();
        for (Entry<Object, Object> kv : props.entrySet()) {
            result.setProperty(check((String) kv.getKey()), (String) kv.getValue());
        }
        return result;
    }
    
    private boolean containsOldKey(Set<? extends Object> keySet) {
        for (Object k : keySet) {
            if (old2new.containsKey(k))
                return true;
        }
        return false;
    }

}
