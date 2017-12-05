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

import java.util.List;

import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.druid.DruidSchema;

import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.scan.ScanQuery;
import io.druid.query.scan.ScanResultValue;

public class ScanRowCursor extends AbstractRowCursor<ScanResultValue> {
    private final AggregatorFactory[] factories;

    public ScanRowCursor(DruidSchema schema, DruidClient<ScanResultValue> client, ScanQuery query, StorageContext context) {
        super(schema, client, query, context);
        this.factories = schema.getAggregators();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected boolean convert(ScanResultValue result, int index, Object[] row) {
        List<List<Object>> rows = (List<List<Object>>) result.getEvents();
        if (index >= rows.size()) {
            return false;
        }

        List<Object> values = rows.get(index);
        final int numDims = schema.getDimensions().size();
        for (int i = 0; i < values.size(); i++) {
            Object value = values.get(i);
            if (i >= numDims) { // measures
                value = factories[i - numDims].deserialize(value);
            }
            row[i] = convertComplexValue(value);
        }
        return true;
    }
}
