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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.lang.reflect.Field;
import java.util.Map;

public class JobProcessContext {

    static Map<String, Process> runningProcess = Maps.newConcurrentMap();

    public static void registerProcess(String jobId, Process process){
        runningProcess.put(jobId, process);
    }

    public static Process getProcess(String jobId){
        return runningProcess.get(jobId);
    }

    public static void removeProcess(String jobId){
        runningProcess.remove(jobId);
    }

    public static int getPid(Process process) throws IllegalAccessException, NoSuchFieldException {
        String className = process.getClass().getName();
        Preconditions.checkState(className.equals("java.lang.UNIXProcess"));
        Field f = process.getClass().getDeclaredField("pid");
        f.setAccessible(true);
        return f.getInt(process);
    }
}