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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.SimpleType;
import com.google.common.collect.Lists;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by qianzhou on 3/25/15.
 */
public final class JsonStreamParser implements StreamParser {

    private static final Logger logger = LoggerFactory.getLogger(JsonStreamParser.class);

    private final List<TblColRef> allColumns;

    public JsonStreamParser(List<TblColRef> allColumns){
        this.allColumns = allColumns;
    }

    @Override
    public List<String> parse(Stream stream) {
        try {
            Map<String, String> json = new ObjectMapper().readValue(
                    stream.getRawData(), MapType.construct(HashMap.class, SimpleType.construct(String.class), SimpleType.construct(String.class)));
            ArrayList<String> result = Lists.newArrayList();
            for (TblColRef column : allColumns) {
                for (Map.Entry<String, String> entry : json.entrySet()) {
                    if (entry.getKey().equalsIgnoreCase(column.getName())) {
                        result.add(entry.getValue());
                    }
                }
            }
            return result;
        } catch (IOException e) {
            logger.error("error parsing stream", e);
            throw new RuntimeException(e);
        }
    }

}
