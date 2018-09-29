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

package org.apache.kylin.junit;

import com.google.common.collect.Maps;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;

public final class EnvUtils {

    public static boolean checkEnv(String env) {
        return System.getenv(env) != null;
    }

    public static void setNormalEnv() throws Exception {

        Map<String, String> newenv = Maps.newHashMap();
        setDefaultEnv("SPARK_HOME", "../../build/spark", newenv);
        //setDefaultEnv("hdp.version", "2.4.0.0-169", newenv);
        setDefaultEnv("ZIPKIN_HOSTNAME", "sandbox", newenv);
        setDefaultEnv("ZIPKIN_SCRIBE_PORT", "9410", newenv);
        setDefaultEnv("KAP_HDFS_WORKING_DIR", "/kylin", newenv);
        changeEnv(newenv);

    }

    protected static void changeEnv(Map<String, String> newenv) throws Exception {
        Class[] classes = Collections.class.getDeclaredClasses();
        Map<String, String> env = System.getenv();
        for (Class cl : classes) {
            if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
                Field field = cl.getDeclaredField("m");
                field.setAccessible(true);
                Object obj = field.get(env);
                Map<String, String> map = (Map<String, String>) obj;
                map.putAll(newenv);
            }
        }
    }

    private static void setDefaultEnv(String env, String defaultValue, Map<String, String> newenv) {
        if (System.getenv(env) == null) {
            newenv.put(env, defaultValue);
        }
    }
}
