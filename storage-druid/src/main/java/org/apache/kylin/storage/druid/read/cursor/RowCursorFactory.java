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

package org.apache.kylin.storage.druid.read.cursor;

import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.druid.DruidSchema;
import org.apache.kylin.storage.druid.http.HttpClient;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.druid.data.input.Row;
import io.druid.query.Query;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.scan.ScanQuery;
import io.druid.query.scan.ScanResultValue;

public class RowCursorFactory {
    public static final TypeReference<Row> ROW_TYPE_REFERENCE = new TypeReference<Row>() {
    };
    public static final TypeReference<ScanResultValue> SCAN_TYPE_REFERENCE = new TypeReference<ScanResultValue>() {
    };

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public RowCursorFactory(HttpClient httpClient, ObjectMapper objectMapper) {
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
    }

    public RowCursor createRowCursor(DruidSchema schema, Query<?> query, StorageContext context) {
        String brokerHost = context.getCuboid().getCubeDesc().getConfig().getDruidBrokerHost();

        if (query instanceof GroupByQuery) {
            DruidClient<Row> client = new HttpDruidClient<>(httpClient, objectMapper, ROW_TYPE_REFERENCE, brokerHost);
            return new GroupByRowCursor(schema, client, (GroupByQuery) query, context);
        }

        if (query instanceof ScanQuery) {
            DruidClient<ScanResultValue> client = new HttpDruidClient<>(httpClient, objectMapper, SCAN_TYPE_REFERENCE, brokerHost);
            return new ScanRowCursor(schema, client, (ScanQuery) query, context);
        }

        throw new IllegalArgumentException("Unknown query type " + query.getType());
    }
}
