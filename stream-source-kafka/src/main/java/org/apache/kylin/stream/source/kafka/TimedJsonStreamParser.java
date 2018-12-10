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

package org.apache.kylin.stream.source.kafka;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.stream.core.exception.StreamingException;
import org.apache.kylin.stream.core.model.StreamingMessage;
import org.apache.kylin.stream.core.source.IStreamingMessageParser;
import org.apache.kylin.stream.core.source.MessageParserInfo;
import org.apache.kylin.stream.core.util.TimeDerivedColumnType;
import org.apache.kylin.stream.source.kafka.KafkaPosition.KafkaPartitionPosition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.SimpleType;
import com.google.common.collect.Lists;

/**
 * each json message with a "timestamp" field
 */
public final class TimedJsonStreamParser implements IStreamingMessageParser<ConsumerRecord<byte[], byte[]>> {

    private static final Logger logger = LoggerFactory.getLogger(TimedJsonStreamParser.class);
    private final ObjectMapper mapper = new ObjectMapper();
    private final JavaType mapType = MapType.construct(HashMap.class, SimpleType.construct(String.class),
            SimpleType.construct(String.class));
    private List<TblColRef> allColumns;
    private boolean formatTs = false;//not used
    private String tsColName = "timestamp";
    private Map<String, String> columnToSourceFieldMapping;

    public TimedJsonStreamParser(CubeDesc cubeDesc, MessageParserInfo parserInfo) {
        this.allColumns = new CubeJoinedFlatTableDesc(cubeDesc).getAllColumns();
        if (parserInfo != null) {
            this.formatTs = parserInfo.isFormatTs();
            this.tsColName = parserInfo.getTsColName();
            this.columnToSourceFieldMapping = parserInfo.getColumnToSourceFieldMapping();
        }

        logger.info("TimedJsonStreamParser with formatTs {} tsColName {}", formatTs, tsColName);
    }

    @Override
    public StreamingMessage parse(ConsumerRecord<byte[], byte[]> record) {
        try {
            Map<String, String> message = mapper.readValue(parseToString(record.value()), mapType);
            Map<String, String> root = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            root.putAll(message);
            String tsStr = root.get(tsColName);
            //Preconditions.checkArgument(!StringUtils.isEmpty(tsStr), "Timestamp field " + tsColName + //
            //" cannot be null, the message offset is " + messageAndOffset.getOffset() + " content is " + new String(messageAndOffset.getRawData()));
            long t;
            if (StringUtils.isEmpty(tsStr)) {
                t = 0;
            } else {
                t = Long.valueOf(tsStr);
            }
            ArrayList<String> result = Lists.newArrayList();

            for (TblColRef column : allColumns) {
                String columnName = column.getName();
                TimeDerivedColumnType columnType = TimeDerivedColumnType.getTimeDerivedColumnType(columnName);
                if (columnType != null) {
                    result.add(String.valueOf(columnType.normalize(t)));
                } else {
                    String x = root.get(columnName.toLowerCase(Locale.ROOT));
                    result.add(x);
                }
            }

            return new StreamingMessage(result, new KafkaPartitionPosition(record.partition(), record.offset()), t,
                    Collections.<String, Object> emptyMap());
        } catch (IOException e) {
            logger.error("error", e);
            throw new RuntimeException(e);
        }
    }

    private String parseToString(byte[] bytes) {
        String value;
        try {
            value = new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new StreamingException(e);
        }
        return value;
    }
}
