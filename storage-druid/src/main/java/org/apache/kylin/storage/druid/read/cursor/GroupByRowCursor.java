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

import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.storage.StorageContext;
import org.apache.kylin.storage.druid.DruidSchema;

import io.druid.data.input.Row;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.groupby.GroupByQuery;

public class GroupByRowCursor extends AbstractRowCursor<Row> {
    private final List<AggregatorFactory> factories;

    public GroupByRowCursor(DruidSchema schema, DruidClient<Row> client, GroupByQuery query, StorageContext context) {
        super(schema, client, query, context);
        this.factories = query.getAggregatorSpecs();
    }

    @Override
    protected boolean convert(Row result, int index, Object[] row) {
        if (index > 0) {
            return false;
        }

        for (TblColRef dim : schema.getDimensions()) {
            String name = schema.getDimFieldName(dim);
            row[schema.getFieldIndex(name)] = result.getRaw(name);
        }

        for (AggregatorFactory factory : factories) {
            String name = factory.getName();
            Object object = factory.deserialize(result.getRaw(name));
            row[schema.getFieldIndex(name)] = convertComplexValue(object);
        }
        return true;
    }
}
