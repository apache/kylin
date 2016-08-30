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

package org.apache.kylin.source.kafka;

import com.google.common.collect.Lists;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.streaming.StreamingConfig;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.ReadableTable;
import org.apache.kylin.source.kafka.config.KafkaConfig;

import java.util.List;

//used by reflection
public class KafkaSource implements ISource {

    @SuppressWarnings("unchecked")
    @Override
    public <I> I adaptToBuildEngine(Class<I> engineInterface) {
        if (engineInterface == IMRInput.class) {
            return (I) new KafkaMRInput();
        } else {
            throw new RuntimeException("Cannot adapt to " + engineInterface);
        }
    }

    @Override
    public ReadableTable createReadableTable(TableDesc tableDesc) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> getMRDependentResources(TableDesc table) {
        List<String> dependentResources = Lists.newArrayList();
        dependentResources.add(KafkaConfig.concatResourcePath(table.getIdentity()));
        dependentResources.add(StreamingConfig.concatResourcePath(table.getIdentity()));
        return dependentResources;
    }

}
