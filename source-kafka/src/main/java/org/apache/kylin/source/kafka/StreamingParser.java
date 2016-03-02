/*
 *
 *
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *
 *  contributor license agreements. See the NOTICE file distributed with
 *
 *  this work for additional information regarding copyright ownership.
 *
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *
 *  (the "License"); you may not use this file except in compliance with
 *
 *  the License. You may obtain a copy of the License at
 *
 *
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *
 *
 *  Unless required by applicable law or agreed to in writing, software
 *
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 *  See the License for the specific language governing permissions and
 *
 *  limitations under the License.
 *
 * /
 */

package org.apache.kylin.source.kafka;

import java.lang.reflect.Constructor;
import java.util.List;

import javax.annotation.Nullable;

import kafka.message.MessageAndOffset;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.engine.streaming.StreamingManager;
import org.apache.kylin.common.util.StreamingMessage;
import org.apache.kylin.metadata.model.IntermediateColumnDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.source.kafka.config.KafkaConfig;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * By convention stream parsers should have a constructor with (List<TblColRef> allColumns, String propertiesStr) as params
 */
public abstract class StreamingParser {

    /**
     * @param kafkaMessage
     * @return StreamingMessage must not be NULL
     */
    abstract public StreamingMessage parse(MessageAndOffset kafkaMessage);

    abstract public boolean filter(StreamingMessage streamingMessage);

    public static StreamingParser getStreamingParser(KafkaConfig kafkaConfig, RealizationType realizationType, String realizationName) throws ReflectiveOperationException {
        final CubeInstance cubeInstance = CubeManager.getInstance(KylinConfig.getInstanceFromEnv()).getCube(realizationName);
        List<TblColRef> columns = Lists.transform(new CubeJoinedFlatTableDesc(cubeInstance.getDescriptor(), null).getColumnList(), new Function<IntermediateColumnDesc, TblColRef>() {
            @Nullable
            @Override
            public TblColRef apply(IntermediateColumnDesc input) {
                return input.getColRef();
            }
        });
        if (!StringUtils.isEmpty(kafkaConfig.getParserName())) {
            Class clazz = Class.forName(kafkaConfig.getParserName());
            Constructor constructor = clazz.getConstructor(List.class, String.class);
            return (StreamingParser) constructor.newInstance(columns, kafkaConfig.getParserProperties());
        } else {
            throw new IllegalStateException("invalid StreamingConfig:" + kafkaConfig.getName() + " missing property StreamingParser");
        }
    }
}
