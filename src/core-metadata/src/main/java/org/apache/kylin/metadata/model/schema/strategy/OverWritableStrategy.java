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

public class OverWritableStrategy implements SchemaChangeStrategy {

    @Override
    public List<SchemaNodeType> supportedSchemaNodeTypes() {
        return Arrays.asList(SchemaNodeType.MODEL_DIMENSION, SchemaNodeType.MODEL_MEASURE,
                SchemaNodeType.RULE_BASED_INDEX, SchemaNodeType.WHITE_LIST_INDEX, SchemaNodeType.TO_BE_DELETED_INDEX);
    }

    @Override
    public List<SchemaChangeCheckResult.ChangedItem> newItemFunction(SchemaUtil.SchemaDifference difference,
            Map.Entry<SchemaNode.SchemaNodeIdentifier, SchemaNode> entry, Set<String> importModels,
            Set<String> originalModels, Set<String> originalBrokenModels) {
        return createSchemaChange(difference, entry, importModels, originalModels, originalBrokenModels);
    }

    @Override
    public List<SchemaChangeCheckResult.ChangedItem> reduceItemFunction(SchemaUtil.SchemaDifference difference,
            Map.Entry<SchemaNode.SchemaNodeIdentifier, SchemaNode> entry, Set<String> importModels,
            Set<String> originalModels, Set<String> originalBrokenModels) {
        return createSchemaChange(difference, entry, importModels, originalModels, originalBrokenModels);
    }

    private List<SchemaChangeCheckResult.ChangedItem> createSchemaChange(SchemaUtil.SchemaDifference difference,
            Map.Entry<SchemaNode.SchemaNodeIdentifier, SchemaNode> entry, Set<String> importModels,
            Set<String> originalModels, Set<String> originalBrokenModels) {
        String modelAlias = entry.getValue().getSubject();
        if (overwritable(importModels, originalModels, modelAlias)) {
            return Collections.singletonList(SchemaChangeCheckResult.ChangedItem.createOverwritableSchemaNode(
                    entry.getKey().getType(), entry.getValue(), hasSameName(modelAlias, originalModels),
                    hasSameWithBroken(modelAlias, originalBrokenModels)));
        } else {
            return Collections.singletonList(SchemaChangeCheckResult.ChangedItem.createCreatableSchemaNode(
                    entry.getKey().getType(), entry.getValue(), hasSameName(modelAlias, originalModels),
                    hasSameWithBroken(modelAlias, originalBrokenModels)));
        }
    }
}
