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

package org.apache.kylin.metadata.model.schema.strategy;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.metadata.model.schema.SchemaChangeCheckResult;
import org.apache.kylin.metadata.model.schema.SchemaNode;
import org.apache.kylin.metadata.model.schema.SchemaNodeType;
import org.apache.kylin.metadata.model.schema.SchemaUtil;
import org.apache.kylin.metadata.model.util.MultiPartitionUtil;

import io.kyligence.kap.guava20.shaded.common.collect.MapDifference;

public class MultiplePartitionStrategy extends UnOverWritableStrategy {
    @Override
    public List<SchemaNodeType> supportedSchemaNodeTypes() {
        return Collections.singletonList(SchemaNodeType.MODEL_MULTIPLE_PARTITION);
    }

    @Override
    public List<SchemaChangeCheckResult.UpdatedItem> updateItemFunction(SchemaUtil.SchemaDifference difference,
            MapDifference.ValueDifference<SchemaNode> diff, Set<String> importModels, Set<String> originalModels) {
        String modelAlias = diff.rightValue().getSubject();

        boolean overwritable = overwritable(diff);

        // columns equals
        if (overwritable) {
            List<List<String>> rightPartitions = (List<List<String>>) diff.rightValue().getAttributes()
                    .get("partitions");
            if (rightPartitions.isEmpty()) {
                return Collections.emptyList();
            }
            List<List<String>> leftPartitions = (List<List<String>>) diff.leftValue().getAttributes().get("partitions");

            if (leftPartitions.size() == rightPartitions.size()) {
                // ignore orders
                List<String[]> duplicateValues = MultiPartitionUtil.findDuplicateValues(
                        leftPartitions.stream().map(item -> item.toArray(new String[0])).collect(Collectors.toList()),
                        rightPartitions.stream().map(item -> item.toArray(new String[0])).collect(Collectors.toList()));

                if (duplicateValues.size() == rightPartitions.size()) {
                    return Collections.emptyList();
                }
            }
        }

        return Collections.singletonList(SchemaChangeCheckResult.UpdatedItem.getSchemaUpdate(diff.leftValue(),
                diff.rightValue(), modelAlias, hasSameName(modelAlias, originalModels), true, true, overwritable));
    }

    /**
     *
     * @param diff
     * @return
     */
    public boolean overwritable(MapDifference.ValueDifference<SchemaNode> diff) {
        return Objects.equals(diff.leftValue().getAttributes().get("columns"),
                diff.rightValue().getAttributes().get("columns"));
    }
}
