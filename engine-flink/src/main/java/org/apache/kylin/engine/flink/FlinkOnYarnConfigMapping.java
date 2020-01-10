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
package org.apache.kylin.engine.flink;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.FallbackKey;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A util class which build mapping between Flink config option keys from flink-conf.yaml
 * and Flink on YARN cmd option.
 */
public class FlinkOnYarnConfigMapping {

    public static final Map<String, String> flinkOnYarnConfigMap;

    static {
        flinkOnYarnConfigMap = new HashMap<>();

        //mapping job manager heap size -> -yjm
        ConfigOption<String> jmHeapSizeOption = JobManagerOptions.JOB_MANAGER_HEAP_MEMORY;
        flinkOnYarnConfigMap.put(jmHeapSizeOption.key(), "-yjm");
        if (jmHeapSizeOption.hasFallbackKeys()) {
            Iterator<FallbackKey> deprecatedKeyIterator = jmHeapSizeOption.fallbackKeys().iterator();
            while (deprecatedKeyIterator.hasNext()) {
                flinkOnYarnConfigMap.put(deprecatedKeyIterator.next().getKey(), "-yjm");
            }
        }

        //mapping task manager heap size -> -ytm
        ConfigOption<String> tmHeapSizeOption = TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY;
        flinkOnYarnConfigMap.put(tmHeapSizeOption.key(), "-ytm");
        if (tmHeapSizeOption.hasFallbackKeys()) {
            Iterator<FallbackKey> deprecatedKeyIterator = tmHeapSizeOption.fallbackKeys().iterator();
            while (deprecatedKeyIterator.hasNext()) {
                flinkOnYarnConfigMap.put(deprecatedKeyIterator.next().getKey(), "-ytm");
            }
        }

        ConfigOption<Integer> taskSlotNumOption = TaskManagerOptions.NUM_TASK_SLOTS;
        flinkOnYarnConfigMap.put(taskSlotNumOption.key(), "-ys");
        if (taskSlotNumOption.hasFallbackKeys()) {
            Iterator<FallbackKey> deprecatedKeyIterator = taskSlotNumOption.fallbackKeys().iterator();
            while (deprecatedKeyIterator.hasNext()) {
                flinkOnYarnConfigMap.put(deprecatedKeyIterator.next().getKey(), "-ys");
            }
        }

        ConfigOption<Boolean> tmMemoryPreallocate = TaskManagerOptions.MANAGED_MEMORY_PRE_ALLOCATE;
        flinkOnYarnConfigMap.put(tmMemoryPreallocate.key(), "-yD taskmanager.memory.preallocate");
        if (taskSlotNumOption.hasFallbackKeys()) {
            Iterator<FallbackKey> deprecatedKeyIterator = tmMemoryPreallocate.fallbackKeys().iterator();
            while (deprecatedKeyIterator.hasNext()) {
                flinkOnYarnConfigMap.put(deprecatedKeyIterator.next().getKey(), "-yD taskmanager.memory.preallocate");
            }
        }

        //config options do not have mapping with config file key
        flinkOnYarnConfigMap.put("yarn.queue", "-yqu");
        flinkOnYarnConfigMap.put("yarn.nodelabel", "-ynl");
    }

}
