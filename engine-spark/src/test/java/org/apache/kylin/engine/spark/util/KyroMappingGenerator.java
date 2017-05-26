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

package org.apache.kylin.engine.spark.util;

import org.apache.kylin.common.util.BytesSerializer;
import org.apache.kylin.measure.MeasureIngester;
import org.reflections.Reflections;

import java.io.Serializable;
import java.util.Set;
import java.util.TreeSet;

/**
 * Generate Kyro Registrator class, the output will be added into KylinKyroRegistrator manually. No runtime dependency with Reflections.
 */
public class KyroMappingGenerator {
    public static void main(String[] args) {
        Set<Class<? extends Serializable>> subTypesOfSerializable = new Reflections("org.apache.kylin").getSubTypesOf(Serializable.class);
        String begin = "kyroClasses.add(";
        String end = ".class);";
        TreeSet<String> sortedSet = new TreeSet();
        for (Class clazz : subTypesOfSerializable) {
            if (clazz.getCanonicalName() != null)
                sortedSet.add(clazz.getCanonicalName());
        }
        Set<Class<? extends BytesSerializer>> subTypesOfBytes = new Reflections("org.apache.kylin.metadata.datatype").getSubTypesOf(BytesSerializer.class);
        for (Class clazz : subTypesOfBytes) {
            if (clazz.getCanonicalName() != null)
                sortedSet.add(clazz.getCanonicalName());
        }
        Set<Class<? extends MeasureIngester>> subTypesOfMeasure = new Reflections("org.apache.kylin.measure").getSubTypesOf(MeasureIngester.class);
        for (Class clazz : subTypesOfMeasure) {
            if (clazz.getCanonicalName() != null)
                sortedSet.add(clazz.getCanonicalName());
        }
        for (String className : sortedSet) {
            System.out.println(begin + className + end);
        }
    }
}
