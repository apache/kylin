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

import static org.apache.kylin.metadata.model.schema.SchemaChangeCheckResult.UN_IMPORT_REASON.TABLE_COLUMN_DATATYPE_CHANGED;
import static org.apache.kylin.metadata.model.schema.SchemaChangeCheckResult.UN_IMPORT_REASON.USED_UNLOADED_TABLE;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.metadata.model.schema.SchemaChangeCheckResult;
import org.apache.kylin.metadata.model.schema.SchemaNode;
import org.apache.kylin.metadata.model.schema.SchemaNodeType;
import org.apache.kylin.metadata.model.schema.SchemaUtil;

import org.apache.kylin.guava30.shaded.common.collect.MapDifference;
import org.apache.kylin.guava30.shaded.common.graph.Graphs;
import lombok.val;

public class TableColumnStrategy implements SchemaChangeStrategy {
    @Override
    public List<SchemaNodeType> supportedSchemaNodeTypes() {
        return Collections.singletonList(SchemaNodeType.TABLE_COLUMN);
    }

    @Override
    public List<SchemaChangeCheckResult.ChangedItem> missingItems(SchemaUtil.SchemaDifference difference,
            Set<String> importModels, Set<String> originalModels, Set<String> originalBrokenModels) {
        return difference.getNodeDiff().entriesOnlyOnRight().entrySet().stream()
                .filter(pair -> supportedSchemaNodeTypes().contains(pair.getKey().getType()))
                .map(pair -> missingItemFunction(difference, pair, importModels, originalModels, originalBrokenModels))
                .flatMap(Collection::stream).filter(schemaChange -> importModels.contains(schemaChange.getModelAlias()))
                .collect(Collectors.toList());
    }

    @Override
    public List<SchemaChangeCheckResult.ChangedItem> missingItemFunction(SchemaUtil.SchemaDifference difference,
            Map.Entry<SchemaNode.SchemaNodeIdentifier, SchemaNode> entry, Set<String> importModels,
            Set<String> originalModels, Set<String> originalBrokenModels) {
        return reachableModel(difference.getTargetGraph(), entry.getValue()).stream()
                .map(modelAlias -> SchemaChangeCheckResult.ChangedItem.createUnImportableSchemaNode(
                        entry.getKey().getType(), entry.getValue(), modelAlias, USED_UNLOADED_TABLE,
                        entry.getValue().getDetail(), hasSameName(modelAlias, originalModels),
                        hasSameWithBroken(modelAlias, originalBrokenModels)))
                .collect(Collectors.toList());
    }

    @Override
    public List<SchemaChangeCheckResult.UpdatedItem> updateItemFunction(SchemaUtil.SchemaDifference difference,
            MapDifference.ValueDifference<SchemaNode> diff, Set<String> importModels, Set<String> originalModels,
            Set<String> originalBrokenModels) {
        return Graphs.reachableNodes(difference.getTargetGraph(), diff.rightValue()).stream()
                .filter(SchemaNode::isModelNode).map(SchemaNode::getSubject).distinct()
                .map(modelAlias -> {
                    val parameter = new SchemaChangeCheckResult.BaseItemParameter(
                            hasSameName(modelAlias, originalModels),
                            hasSameWithBroken(modelAlias, originalBrokenModels), false, false, false);
                    return SchemaChangeCheckResult.UpdatedItem.getSchemaUpdate(diff.leftValue(), diff.rightValue(),
                            modelAlias, TABLE_COLUMN_DATATYPE_CHANGED, diff.rightValue().getDetail(), parameter);
                })
                .collect(Collectors.toList());
    }

    @Override
    public List<SchemaChangeCheckResult.ChangedItem> reduceItemFunction(SchemaUtil.SchemaDifference difference,
            Map.Entry<SchemaNode.SchemaNodeIdentifier, SchemaNode> entry, Set<String> importModels,
            Set<String> originalModels, Set<String> originalBrokenModels) {
        return Graphs.reachableNodes(difference.getSourceGraph(), entry.getValue()).stream()
                .filter(SchemaNode::isModelNode).map(SchemaNode::getSubject).distinct()
                .map(modelAlias -> SchemaChangeCheckResult.ChangedItem.createOverwritableSchemaNode(
                        entry.getKey().getType(), entry.getValue(), modelAlias,
                        hasSameName(modelAlias, originalModels), hasSameWithBroken(modelAlias, originalBrokenModels)))
                .collect(Collectors.toList());
    }
}
