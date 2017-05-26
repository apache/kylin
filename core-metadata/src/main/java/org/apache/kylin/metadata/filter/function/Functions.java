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

package org.apache.kylin.metadata.filter.function;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.kylin.metadata.filter.BuiltInFunctionTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.UDF.MassInTupleFilter;

import com.google.common.collect.Maps;

public class Functions {

    public enum FilterTableType {
        HDFS, HBASE_TABLE
    }

    private static Map<String, Class> SUPPORTED_UDF = Maps.newHashMap();

    static {
        SUPPORTED_UDF.put("MASSIN", MassInTupleFilter.class);
    }

    public static TupleFilter getFunctionTupleFilter(String name) {
        if (name == null) {
            throw new IllegalStateException("Function name cannot be null");
        }

        name = name.toUpperCase();

        if (SUPPORTED_UDF.containsKey(name)) {
            try {
                return (TupleFilter) SUPPORTED_UDF.get(name).getConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                throw new RuntimeException("Failed to on constructing FunctionTupleFilter for " + name);
            }
        }

        return new BuiltInFunctionTupleFilter(name);

    }
}
