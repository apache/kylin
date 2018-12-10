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

package org.apache.kylin.stream.core.source;

import static org.apache.kylin.metadata.model.ISourceAware.ID_KAFKA;
import static org.apache.kylin.metadata.model.ISourceAware.ID_KAFKA_HIVE;

import java.util.HashMap;
import java.util.Map;

import org.apache.kylin.common.util.ImplementationSwitch;
import org.apache.kylin.metadata.model.ISourceAware;

public class StreamingSourceFactory {
    private static String KAFKA_SOURCE_CLAZZ = "org.apache.kylin.stream.source.kafka.KafkaSource";

    private static ImplementationSwitch<IStreamingSource> sources;
    static {
        Map<Integer, String> impls = new HashMap<>();
        impls.put(ID_KAFKA, KAFKA_SOURCE_CLAZZ);
        impls.put(ID_KAFKA_HIVE, KAFKA_SOURCE_CLAZZ);
        sources = new ImplementationSwitch<>(impls, IStreamingSource.class);
    }

    public static IStreamingSource getStreamingSource(ISourceAware aware) {
        return sources.get(aware.getSourceType());
    }
}
