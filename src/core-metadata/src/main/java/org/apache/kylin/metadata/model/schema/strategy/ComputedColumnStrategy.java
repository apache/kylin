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

import static org.apache.kylin.metadata.model.schema.SchemaChangeCheckResult.UN_IMPORT_REASON.DIFFERENT_CC_NAME_HAS_SAME_EXPR;
import static org.apache.kylin.metadata.model.schema.SchemaChangeCheckResult.UN_IMPORT_REASON.SAME_CC_NAME_HAS_DIFFERENT_EXPR;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.metadata.model.schema.SchemaChangeCheckResult;
import org.apache.kylin.metadata.model.schema.SchemaNode;
import org.apache.kylin.metadata.model.schema.SchemaNodeType;
import org.apache.kylin.metadata.model.schema.SchemaUtil;

import com.google.common.base.Objects;

import io.kyligence.kap.guava20.shaded.common.collect.MapDifference;
import lombok.val;

public class ComputedColumnStrategy implements SchemaChangeStrategy {

    @Override
    public List<SchemaNodeType> supportedSchemaNodeTypes() {
        return Collections.singletonList(SchemaNodeType.MODEL_CC);
    }

    @Override
    public List<SchemaChangeCheckResult.ChangedItem> newItemFunction(SchemaUtil.SchemaDifference difference,
            Map.Entry<SchemaNode.SchemaNodeIdentifier, SchemaNode> entry, Set<String> importModels,
            Set<String> originalModels, Set<String> originalBrokenModels) {
        List<SchemaNode> allComputedColumns = difference.getSourceGraph().nodes().stream()
                .filter(schemaNode -> supportedSchemaNodeTypes().contains(schemaNode.getType()))
                .collect(Collectors.toList());

        String modelAlias = entry.getValue().getSubject();

        // same cc name with different expression
        if (hasComputedColumnNameWithDifferentExpression(entry.getValue(), allComputedColumns)) {
            return Collections.singletonList(SchemaChangeCheckResult.ChangedItem.createUnImportableSchemaNode(
                    entry.getKey().getType(), entry.getValue(), SAME_CC_NAME_HAS_DIFFERENT_EXPR, null,
                    hasSameName(modelAlias, originalModels), hasSameWithBroken(modelAlias, originalBrokenModels)));
        }

        // different cc name with same expression
        val optional = hasExpressionWithDifferentComputedColumn(entry.getValue(), allComputedColumns);
        if (optional.isPresent()) {
            return Collections.singletonList(SchemaChangeCheckResult.ChangedItem.createUnImportableSchemaNode(
                    entry.getKey().getType(), entry.getValue(), DIFFERENT_CC_NAME_HAS_SAME_EXPR,
                    optional.get().getDetail(), hasSameName(modelAlias, originalModels),
                    hasSameWithBroken(modelAlias, originalBrokenModels)));
        }

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

    @Override
    public List<SchemaChangeCheckResult.UpdatedItem> updateItemFunction(SchemaUtil.SchemaDifference difference,
            MapDifference.ValueDifference<SchemaNode> diff, Set<String> importModels, Set<String> originalModels,
            Set<String> originalBrokenModels) {
        List<SchemaNode> allComputedColumns = difference.getSourceGraph().nodes().stream()
                .filter(schemaNode -> supportedSchemaNodeTypes().contains(schemaNode.getType()))
                .collect(Collectors.toList());

        SchemaNode schemaNode = diff.rightValue();
        String modelAlias = diff.rightValue().getSubject();
        // same cc name with different expression
        if (hasComputedColumnNameWithDifferentExpression(schemaNode, allComputedColumns)) {
            val parameter = new SchemaChangeCheckResult.BaseItemParameter(hasSameName(modelAlias, originalModels),
                    hasSameWithBroken(modelAlias, originalBrokenModels), false, false, false);
            return Collections.singletonList(SchemaChangeCheckResult.UpdatedItem.getSchemaUpdate(diff.leftValue(),
                    diff.rightValue(), modelAlias, SAME_CC_NAME_HAS_DIFFERENT_EXPR, null, parameter));
        }

        // different cc name with same expression
        val optional = hasExpressionWithDifferentComputedColumn(schemaNode, allComputedColumns);
        if (optional.isPresent()) {
            val parameter = new SchemaChangeCheckResult.BaseItemParameter(hasSameName(modelAlias, originalModels),
                    hasSameWithBroken(modelAlias, originalBrokenModels), false, false, false);
            return Collections.singletonList(SchemaChangeCheckResult.UpdatedItem.getSchemaUpdate(diff.leftValue(),
                    diff.rightValue(), modelAlias, DIFFERENT_CC_NAME_HAS_SAME_EXPR, optional.get().getDetail(),
                    parameter));
        }

        boolean overwritable = overwritable(importModels, originalModels, modelAlias);
        val parameter = new SchemaChangeCheckResult.BaseItemParameter(hasSameName(modelAlias, originalModels),
                hasSameWithBroken(modelAlias, originalBrokenModels), true, true, overwritable);
        return Collections.singletonList(SchemaChangeCheckResult.UpdatedItem.getSchemaUpdate(diff.leftValue(),
                diff.rightValue(), modelAlias, parameter));
    }

    @Override
    public List<SchemaChangeCheckResult.ChangedItem> reduceItemFunction(SchemaUtil.SchemaDifference difference,
            Map.Entry<SchemaNode.SchemaNodeIdentifier, SchemaNode> entry, Set<String> importModels,
            Set<String> originalModels, Set<String> originalBrokenModels) {
        String modelAlias = entry.getValue().getSubject();
        boolean overwritable = overwritable(importModels, originalModels, modelAlias);
        if (overwritable) {
            return Collections.singletonList(SchemaChangeCheckResult.ChangedItem.createOverwritableSchemaNode(
                    entry.getKey().getType(), entry.getValue(), hasSameName(modelAlias, originalModels),
                    hasSameWithBroken(modelAlias, originalBrokenModels)));
        } else {
            return Collections.singletonList(SchemaChangeCheckResult.ChangedItem.createCreatableSchemaNode(
                    entry.getKey().getType(), entry.getValue(), hasSameName(modelAlias, originalModels),
                    hasSameWithBroken(modelAlias, originalBrokenModels)));
        }
    }

    /**
     * check same cc name with different expression
     * @param node
     * @param allComputedColumns
     * @return
     */
    private boolean hasComputedColumnNameWithDifferentExpression(SchemaNode node, List<SchemaNode> allComputedColumns) {
        String ccName = node.getDetail();
        String expression = (String) node.getAttributes().get("expression");

        return allComputedColumns.stream()
                .anyMatch(schemaNode -> Objects.equal(node.getAttributes().get("fact_table"),
                        schemaNode.getAttributes().get("fact_table")) && schemaNode.getDetail().equals(ccName)
                        && !schemaNode.getAttributes().get("expression").equals(expression));
    }

    /**
     * different same cc name with same expression
     * @param node
     * @param allComputedColumns
     * @return
     */
    private Optional<SchemaNode> hasExpressionWithDifferentComputedColumn(SchemaNode node,
            List<SchemaNode> allComputedColumns) {
        String ccName = node.getDetail();
        String expression = (String) node.getAttributes().get("expression");

        return allComputedColumns.stream()
                .filter(schemaNode -> Objects.equal(node.getAttributes().get("fact_table"),
                        schemaNode.getAttributes().get("fact_table")) && !schemaNode.getDetail().equals(ccName)
                        && schemaNode.getAttributes().get("expression").equals(expression))
                .findAny();
    }

}
