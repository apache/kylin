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

package org.apache.kylin.streaming;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.SimpleType;
import com.google.common.collect.Lists;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
public final class SEOJsonStreamParser implements StreamParser {

    private static final Logger logger = LoggerFactory.getLogger(SEOJsonStreamParser.class);

    private final List<TblColRef> allColumns;
    private final ObjectMapper mapper = new ObjectMapper();
    private final JavaType mapType = MapType.construct(HashMap.class, SimpleType.construct(String.class), SimpleType.construct(String.class));

    public SEOJsonStreamParser(List<TblColRef> allColumns) {
        this.allColumns = allColumns;
    }

    @Override
    public List<String> parse(StreamMessage stream) {
        try {
            Map<String, String> root = mapper.readValue(stream.getRawData(), mapType);
            String trafficSource = root.get("trafficsourceid");
            if ("20".equals(trafficSource) || "21".equals(trafficSource) || "22".equals(trafficSource) || "23".equals(trafficSource)) {
                ArrayList<String> result = Lists.newArrayList();
                for (TblColRef column : allColumns) {
                    String columnName = column.getName();
                    if (columnName.equalsIgnoreCase("minute_start")) {
                        result.add(String.valueOf(TimeUtil.getMinuteStart(Long.valueOf(root.get("timestamp")))));
                    } else if (columnName.equalsIgnoreCase("hour_start")) {
                        result.add(String.valueOf(TimeUtil.getHourStart(Long.valueOf(root.get("timestamp")))));
                    } else if (columnName.equalsIgnoreCase("day")) {
                        //of day start we'll add yyyy-mm-dd
                        long ts = TimeUtil.getDayStart(Long.valueOf(root.get("timestamp")));
                        result.add(DateFormat.formatToDateStr(ts));
                    } else {
                        String x = root.get(columnName.toLowerCase());
                        result.add(x);
                    }
                }

                return result;
            } else {
                return null;
            }
        } catch (IOException e) {
            logger.error("error parsing:" + new String(stream.getRawData()), e);
            return null;
        }
    }
}
