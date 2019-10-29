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

package org.apache.kylin.source.spark;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.Predef;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import com.google.common.collect.Lists;


public class ScalaUtils {
    public static Seq<String> javaListToScalaSeq(List<String> inputList) {
        if (null == inputList) {
            List<String> emptyList = Lists.newArrayList();
            return JavaConverters.asScalaIteratorConverter(emptyList.iterator()).asScala().toSeq();
        } else {
            return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
        }
    }

    public static Seq<String> emptySeq() {
        List<String> emptyList = Lists.newArrayList();
        return JavaConverters.asScalaIteratorConverter(emptyList.iterator()).asScala().toSeq();
    }

    public static scala.collection.immutable.Map<String, String> javaMapToScalaMap(
            Map<String, String> inputMap) {
        return JavaConverters.mapAsScalaMapConverter(inputMap).asScala().toMap(Predef.conforms());
    }

    public static Map<String, String> scalaMapToJava(scala.collection.immutable.Map<String, String> scalaMap) {
        Map<String, String> hashMap = new HashMap<>();
        scala.collection.Iterator iterator = scalaMap.keysIterator();
        while (iterator.hasNext()) {
            String key = iterator.next().toString();
            hashMap.put(key, scalaMap.get(key).get());
        }
        return hashMap;
    }
}
