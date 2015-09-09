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

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.SimpleType;
import com.google.common.collect.Lists;
import kafka.message.MessageAndOffset;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.engine.streaming.StreamingMessage;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * each json message with a "timestamp" field
 */
public final class TimedJsonStreamParser implements StreamingParser {

    private static final Logger logger = LoggerFactory.getLogger(TimedJsonStreamParser.class);

    private List<TblColRef> allColumns;
    private boolean formatTs = false;
    private final ObjectMapper mapper = new ObjectMapper();
    private String tsColName = "timestamp";
    private final JavaType mapType = MapType.construct(HashMap.class, SimpleType.construct(String.class), SimpleType.construct(String.class));

    public TimedJsonStreamParser(List<TblColRef> allColumns, String propertiesStr) {
        this.allColumns = allColumns;
        if (!StringUtils.isEmpty(propertiesStr)) {
            String[] properties = propertiesStr.split(";");
            for (String prop : properties) {
                try {
                    String[] parts = prop.split("=");
                    if (parts.length == 2) {
                        switch (parts[0]) {
                        case "formatTs":
                            this.formatTs = Boolean.valueOf(parts[1]);
                            break;
                        case "tsColName":
                            this.tsColName = parts[1];
                            break;
                        default:
                            break;
                        }
                    }
                } catch (Exception e) {
                    logger.error("Failed to parse property " + prop);
                    //ignore
                }
            }
        }

        logger.info("TimedJsonStreamParser with formatTs {} tsColName {}", formatTs, tsColName);
    }

    @Override
    public StreamingMessage parse(MessageAndOffset messageAndOffset) {
        try {
            Map<String, String> root = mapper.readValue(new ByteBufferBackedInputStream(messageAndOffset.message().payload()), mapType);
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
                if (columnName.equalsIgnoreCase("minute_start")) {
                    long minuteStart = TimeUtil.getMinuteStart(t);
                    result.add(formatTs ? DateFormat.formatToTimeStr(minuteStart) : String.valueOf(minuteStart));
                } else if (columnName.equalsIgnoreCase("hour_start")) {
                    long hourStart = TimeUtil.getHourStart(t);
                    result.add(formatTs ? DateFormat.formatToTimeStr(hourStart) : String.valueOf(hourStart));
                } else if (columnName.equalsIgnoreCase("day_start")) {
                    //of day start we'll add yyyy-mm-dd
                    long ts = TimeUtil.getDayStart(t);
                    result.add(DateFormat.formatToDateStr(ts));
                } else {
                    String x = root.get(columnName.toLowerCase());
                    result.add(x);
                }
            }

            return new StreamingMessage(result, messageAndOffset.offset(), t, Collections.<String, Object>emptyMap());

        } catch (IOException e) {
            logger.error("error", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean filter(StreamingMessage streamingMessage) {
        return true;
    }

}
