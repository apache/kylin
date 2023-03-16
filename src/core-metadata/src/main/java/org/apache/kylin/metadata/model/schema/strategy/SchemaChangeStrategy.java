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
import org.apache.kylin.guava30.shaded.common.graph.Graph;
import org.apache.kylin.guava30.shaded.common.graph.Graphs;
import lombok.val;

public interface SchemaChangeStrategy {
    List<SchemaNodeType> supportedSchemaNodeTypes();

    default List<SchemaChangeCheckResult.ChangedItem> missingItemFunction(SchemaUtil.SchemaDifference difference,
            Map.Entry<SchemaNode.SchemaNodeIdentifier, SchemaNode> entry, Set<String> importModels,
            Set<String> originalModels, Set<String> originalBrokenModels) {
        return Collections.emptyList();
    }

    default List<SchemaChangeCheckResult.ChangedItem> missingItems(SchemaUtil.SchemaDifference difference,
            Set<String> importModels, Set<String> originalModels, Set<String> originalBrokenModels) {
        return Collections.emptyList();
    }

    default List<SchemaChangeCheckResult.ChangedItem> newItemFunction(SchemaUtil.SchemaDifference difference,
            Map.Entry<SchemaNode.SchemaNodeIdentifier, SchemaNode> entry, Set<String> importModels,
            Set<String> originalModels, Set<String> originalBrokenModels) {
        return Collections.emptyList();
    }

    default List<SchemaChangeCheckResult.ChangedItem> newItems(SchemaUtil.SchemaDifference difference,
            Set<String> importModels, Set<String> originalModels, Set<String> originalBrokenModels) {
        return difference.getNodeDiff().entriesOnlyOnRight().entrySet().stream()
                .filter(entry -> supportedSchemaNodeTypes().contains(entry.getKey().getType()))
                .map(entry -> newItemFunction(difference, entry, importModels, originalModels, originalBrokenModels))
                .flatMap(Collection::stream).filter(schemaChange -> importModels.contains(schemaChange.getModelAlias()))
                .collect(Collectors.toList());
    }

    default List<SchemaChangeCheckResult.UpdatedItem> updateItemFunction(SchemaUtil.SchemaDifference difference,
            MapDifference.ValueDifference<SchemaNode> diff, Set<String> importModels, Set<String> originalModels,
            Set<String> originalBrokenModels) {
        String modelAlias = diff.rightValue().getSubject();
        boolean overwritable = overwritable(importModels, originalModels, modelAlias);
        val parameter = new SchemaChangeCheckResult.BaseItemParameter(hasSameName(modelAlias, originalModels),
                hasSameWithBroken(modelAlias, originalBrokenModels), true, true, overwritable);
        return Collections.singletonList(SchemaChangeCheckResult.UpdatedItem.getSchemaUpdate(diff.leftValue(),
                diff.rightValue(), modelAlias, parameter));
    }

    default List<SchemaChangeCheckResult.UpdatedItem> updateItems(SchemaUtil.SchemaDifference difference,
            Set<String> importModels, Set<String> originalModels, Set<String> originalBrokenModels) {
        return difference.getNodeDiff().entriesDiffering().values().stream()
                .filter(entry -> supportedSchemaNodeTypes().contains(entry.leftValue().getType()))
                .map(diff -> updateItemFunction(difference, diff, importModels, originalModels, originalBrokenModels))
                .flatMap(Collection::stream).filter(schemaChange -> importModels.contains(schemaChange.getModelAlias()))
                .collect(Collectors.toList());
    }

    default List<SchemaChangeCheckResult.ChangedItem> reduceItemFunction(SchemaUtil.SchemaDifference difference,
            Map.Entry<SchemaNode.SchemaNodeIdentifier, SchemaNode> entry, Set<String> importModels,
            Set<String> originalModels, Set<String> originalBrokenModels) {
        return Collections.emptyList();
    }

    default List<SchemaChangeCheckResult.ChangedItem> reduceItems(SchemaUtil.SchemaDifference difference,
            Set<String> importModels, Set<String> originalModels, Set<String> originalBrokenModels) {
        return difference.getNodeDiff().entriesOnlyOnLeft().entrySet().stream()
                .filter(entry -> supportedSchemaNodeTypes().contains(entry.getKey().getType()))
                .map(entry -> reduceItemFunction(difference, entry, importModels, originalModels, originalBrokenModels))
                .flatMap(Collection::stream).filter(schemaChange -> importModels.contains(schemaChange.getModelAlias()))
                .collect(Collectors.toList());
    }

    default List<String> areEqual(SchemaUtil.SchemaDifference difference, Set<String> importModels) {
        return difference.getNodeDiff().entriesInCommon().values().stream()
                .filter(schemaNode -> schemaNode.getType() == SchemaNodeType.MODEL_FACT)
                .filter(schemaChange -> importModels.contains(schemaChange.getSubject())).map(SchemaNode::getSubject)
                .collect(Collectors.toList());
    }

    default boolean overwritable(Set<String> importModels, Set<String> originalModels, String modelAlias) {
        return importModels.contains(modelAlias) && originalModels.contains(modelAlias);
    }

    default boolean hasSameName(String modelAlias, Set<String> originalModels) {
        return originalModels.contains(modelAlias);
    }

    default Set<String> reachableModel(Graph<SchemaNode> graph, SchemaNode schemaNode) {
        return Graphs.reachableNodes(graph, schemaNode).stream().filter(SchemaNode::isModelNode)
                .map(SchemaNode::getSubject).collect(Collectors.toSet());
    }

    default boolean hasSameWithBroken(String modelAlias, Set<String> originalBrokenModels) {
        return originalBrokenModels.contains(modelAlias);
    }

}
