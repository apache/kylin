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

import com.google.common.collect.Lists;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kylin.metadata.model.TblColRef;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by qianzhou on 3/25/15.
 */
public final class JsonStreamParser implements StreamParser {

    public static final JsonStreamParser instance = new JsonStreamParser();

    private final JsonParser jsonParser = new JsonParser();

    private JsonStreamParser(){}

    @Override
    public List<String> parse(Stream stream, List<TblColRef> allColumns) {
        final JsonObject root = jsonParser.parse(new String(stream.getRawData())).getAsJsonObject();
        ArrayList<String> result = Lists.newArrayList();

        for (TblColRef column : allColumns) {
            for (Map.Entry<String, JsonElement> entry : root.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(column.getName())) {
                    result.add(entry.getValue().getAsString());
                }
            }
        }
        return result;
    }
}
