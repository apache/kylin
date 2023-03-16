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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.metadata.model.schema.SchemaChangeCheckResult;
import org.apache.kylin.metadata.model.schema.SchemaNode;
import org.apache.kylin.metadata.model.schema.SchemaNodeType;
import org.apache.kylin.metadata.model.schema.SchemaUtil;

import org.apache.kylin.guava30.shaded.common.collect.MapDifference;
import lombok.val;

public class UnOverWritableStrategy implements SchemaChangeStrategy {

    @Override
    public List<SchemaNodeType> supportedSchemaNodeTypes() {
        return Arrays.asList(SchemaNodeType.MODEL_FACT, SchemaNodeType.MODEL_DIM, SchemaNodeType.MODEL_FILTER,
                SchemaNodeType.MODEL_JOIN, SchemaNodeType.MODEL_PARTITION);
    }

    @Override
    public List<SchemaChangeCheckResult.ChangedItem> newItemFunction(SchemaUtil.SchemaDifference difference,
            Map.Entry<SchemaNode.SchemaNodeIdentifier, SchemaNode> entry, Set<String> importModels,
            Set<String> originalModels, Set<String> originalBrokenModels) {
        String modelAlias = entry.getValue().getSubject();
        return Collections.singletonList(SchemaChangeCheckResult.ChangedItem.createCreatableSchemaNode(
                entry.getKey().getType(), entry.getValue(), hasSameName(modelAlias, originalModels),
                hasSameWithBroken(modelAlias, originalBrokenModels)));
    }

    @Override
    public List<SchemaChangeCheckResult.UpdatedItem> updateItemFunction(SchemaUtil.SchemaDifference difference,
            MapDifference.ValueDifference<SchemaNode> diff, Set<String> importModels, Set<String> originalModels,
            Set<String> originalBrokenModels) {
        String modelAlias = diff.rightValue().getSubject();
        val parameter = new SchemaChangeCheckResult.BaseItemParameter(hasSameName(modelAlias, originalModels),
                hasSameWithBroken(modelAlias, originalBrokenModels), true, true, false);
        return Collections.singletonList(SchemaChangeCheckResult.UpdatedItem.getSchemaUpdate(diff.leftValue(),
                diff.rightValue(), modelAlias, parameter));
    }

    @Override
    public List<SchemaChangeCheckResult.ChangedItem> reduceItemFunction(SchemaUtil.SchemaDifference difference,
            Map.Entry<SchemaNode.SchemaNodeIdentifier, SchemaNode> entry, Set<String> importModels,
            Set<String> originalModels, Set<String> originalBrokenModels) {
        String modelAlias = entry.getValue().getSubject();
        return Collections.singletonList(SchemaChangeCheckResult.ChangedItem.createCreatableSchemaNode(
                entry.getKey().getType(), entry.getValue(), hasSameName(modelAlias, originalModels),
                hasSameWithBroken(modelAlias, originalBrokenModels)));
    }
}
