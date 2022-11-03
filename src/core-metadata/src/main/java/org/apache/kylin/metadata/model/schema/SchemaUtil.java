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
package org.apache.kylin.metadata.model.schema;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;

import com.google.common.collect.Lists;

import io.kyligence.kap.guava20.shaded.common.base.Preconditions;
import io.kyligence.kap.guava20.shaded.common.collect.MapDifference;
import io.kyligence.kap.guava20.shaded.common.collect.Maps;
import io.kyligence.kap.guava20.shaded.common.graph.Graph;
import io.kyligence.kap.guava20.shaded.common.graph.GraphBuilder;
import io.kyligence.kap.guava20.shaded.common.graph.MutableGraph;
import lombok.Data;
import lombok.val;

public class SchemaUtil {

    public static SchemaDifference diff(String project, KylinConfig sourceConfig, KylinConfig targetConfig,
            List<TableDesc> incrTableDescList) {
        val sourceGraph = dependencyGraph(project, sourceConfig, incrTableDescList);
        val targetGraph = dependencyGraph(project, targetConfig);
        return new SchemaDifference(sourceGraph, targetGraph);
    }

    public static Graph<SchemaNode> dependencyGraph(String project, KylinConfig config,
            List<TableDesc> incrTableDescList) {
        val tableManager = NTableMetadataManager.getInstance(config, project);
        val planManager = NIndexPlanManager.getInstance(config, project);
        List<TableDesc> tableDescs = Lists.newArrayList(tableManager.listAllTables());
        tableDescs.addAll(incrTableDescList);
        return dependencyGraph(tableDescs, planManager.listAllIndexPlans());
    }

    public static SchemaDifference diff(String project, KylinConfig sourceConfig, KylinConfig targetConfig) {
        val sourceGraph = dependencyGraph(project, sourceConfig);
        val targetGraph = dependencyGraph(project, targetConfig);
        return new SchemaDifference(sourceGraph, targetGraph);
    }

    public static Graph<SchemaNode> dependencyGraph(String project, KylinConfig config) {
        val tableManager = NTableMetadataManager.getInstance(config, project);
        val planManager = NIndexPlanManager.getInstance(config, project);
        return dependencyGraph(tableManager.listAllTables(), planManager.listAllIndexPlans());
    }

    /**
     * Build dependency graph of specific model, just all related tables of this model.
     */
    public static Graph<SchemaNode> dependencyGraph(String project, NDataModel model) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(config, project);
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(config, project);
        String factTableName = model.getRootFactTableName();
        TableDesc table = tableManager.getTableDesc(factTableName);
        List<TableDesc> tables = Lists.newArrayList();
        List<IndexPlan> indexPlans = Lists.newArrayList();
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(model.getUuid());
        if (!indexPlan.isBroken()) {
            indexPlans.add(indexPlan);
        }
        tables.add(table);
        List<JoinTableDesc> joinTables = model.getJoinTables();
        for (JoinTableDesc joinTable : joinTables) {
            tables.add(tableManager.getTableDesc(joinTable.getTable()));
        }
        return dependencyGraph(tables, indexPlans);
    }

    /**
     * Build dependency graph on a table, it will deduce all related models and tables.
     */
    public static Graph<SchemaNode> dependencyGraph(String project, String tableIdentity) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(config, project);
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(config, project);
        TableDesc table = tableManager.getTableDesc(tableIdentity);
        List<TableDesc> tables = Lists.newArrayList();
        List<IndexPlan> indexPlans = Lists.newArrayList();

        Preconditions.checkNotNull(table,
                String.format(Locale.ROOT, "Table(%s) not exist in project(%s)", tableIdentity, project));
        tables.add(table);

        NDataModelManager modelManager = NDataModelManager.getInstance(KylinConfig.readSystemKylinConfig(), project);
        List<NDataModel> models = modelManager.listAllModels().stream() //
                .filter(model -> model.isBroken() || isTableRelatedModel(tableIdentity, model)) //
                .collect(Collectors.toList());
        models.forEach(model -> {
            if (!model.isBroken()) {
                indexPlans.add(indexPlanManager.getIndexPlan(model.getUuid()));
            }
            tableManager.getTableDesc(model.getRootFactTableName());
            List<JoinTableDesc> joinTables = model.getJoinTables();
            joinTables.stream().map(JoinTableDesc::getTable).map(tableManager::getTableDesc).filter(Objects::nonNull)
                    .forEach(tables::add);
        });

        return dependencyGraph(tables, indexPlans);
    }

    private static boolean isTableRelatedModel(String tableIdentity, NDataModel model) {
        List<JoinTableDesc> joinTables = model.getJoinTables();
        for (JoinTableDesc joinTable : joinTables) {
            final TableRef tableRef = joinTable.getTableRef();
            if (tableRef.getTableIdentity().equalsIgnoreCase(tableIdentity)) {
                return true;
            }
        }
        return model.getRootFactTableName().equalsIgnoreCase(tableIdentity);
    }

    static Graph<SchemaNode> dependencyGraph(List<TableDesc> tables, List<IndexPlan> plans) {
        MutableGraph<SchemaNode> graph = GraphBuilder.directed().allowsSelfLoops(false).build();
        tables.forEach(table -> {
            for (ColumnDesc column : table.getColumns()) {
                graph.putEdge(SchemaNode.ofTableColumn(column), SchemaNode.ofTable(table));
            }
        });

        for (IndexPlan plan : plans) {
            new ModelEdgeCollector(plan, graph).collect();
        }
        return graph;
    }

    @Data
    public static class SchemaDifference {

        private final Graph<SchemaNode> sourceGraph;

        private final Graph<SchemaNode> targetGraph;

        private final MapDifference<SchemaNode.SchemaNodeIdentifier, SchemaNode> nodeDiff;

        public SchemaDifference(Graph<SchemaNode> sourceGraph, Graph<SchemaNode> targetGraph) {
            this.sourceGraph = sourceGraph;
            this.targetGraph = targetGraph;
            this.nodeDiff = Maps.difference(toSchemaNodeMap(sourceGraph), toSchemaNodeMap(targetGraph));

        }

        private Map<SchemaNode.SchemaNodeIdentifier, SchemaNode> toSchemaNodeMap(Graph<SchemaNode> sourceGraph) {
            return sourceGraph.nodes().stream()
                    .collect(Collectors.toMap(SchemaNode::getIdentifier, Function.identity()));
        }
    }
}
