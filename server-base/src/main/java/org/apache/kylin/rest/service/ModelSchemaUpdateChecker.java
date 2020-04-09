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

package org.apache.kylin.rest.service;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static org.apache.kylin.measure.topn.TopNMeasureType.FUNC_TOP_N;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.util.CheckUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.ModelDimensionDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class ModelSchemaUpdateChecker {

    private final TableMetadataManager metadataManager;
    private final CubeManager cubeManager;
    private final DataModelManager dataModelManager;

    static class CheckResult {
        private final boolean valid;
        private final String reason;

        private CheckResult(boolean valid, String reason) {
            this.valid = valid;
            this.reason = reason;
        }

        void raiseExceptionWhenInvalid() {
            if (!valid) {
                throw new RuntimeException(reason);
            }
        }

        static CheckResult validOnFirstCreate(String modelName) {
            return new CheckResult(true, format(Locale.ROOT, "Model '%s' hasn't been created before", modelName));
        }

        static CheckResult validOnCompatibleSchema(String modelName) {
            return new CheckResult(true,
                    format(Locale.ROOT, "Table '%s' is compatible with all existing cubes", modelName));
        }

        static CheckResult invalidOnIncompatibleSchema(String modelName, List<String> reasons) {
            StringBuilder buf = new StringBuilder();
            for (String reason : reasons) {
                buf.append("- ").append(reason).append("\n");
            }

            return new CheckResult(false,
                    format(Locale.ROOT,
                            "Found %d issue(s) with '%s':%n%s Please disable and purge related cube(s) first",
                            reasons.size(), modelName, buf.toString()));
        }
    }

    ModelSchemaUpdateChecker(TableMetadataManager metadataManager, CubeManager cubeManager,
            DataModelManager dataModelManager) {
        this.metadataManager = checkNotNull(metadataManager, "metadataManager is null");
        this.cubeManager = checkNotNull(cubeManager, "cubeManager is null");
        this.dataModelManager = checkNotNull(dataModelManager, "dataModelManager is null");
    }

    private List<CubeInstance> findCubeByModel(final String modelName) {
        Iterable<CubeInstance> relatedCubes = Iterables.filter(cubeManager.listAllCubes(), cube -> {
            if (cube == null || cube.allowBrokenDescriptor()) {
                return false;
            }
            DataModelDesc model = cube.getModel();
            if (model == null)
                return false;
            return model.getName().equals(modelName);
        });

        return ImmutableList.copyOf(relatedCubes);
    }

    /**
     * Model compatible rule includes:
     * 1. the same fact table
     * 2. the same lookup table
     * 3. the same joins
     * 4. the same partition
     * 5. the same filter
     */
    private static void checkDataModelCompatible(DataModelDesc existing, DataModelDesc newModel, List<String> issues) {
        // Check fact table
        if (!existing.getRootFactTableName().equalsIgnoreCase(newModel.getRootFactTableName())) {
            issues.add(format(Locale.ROOT,
                    "The fact table %s used in existing model is not the same as the updated one %s",
                    existing.getRootFactTableName(), newModel.getRootFactTableName()));
        }
        // Check join table
        Map<String, JoinTableDesc> existingLookupMap = Maps.newHashMap();
        for (JoinTableDesc joinTableDesc : existing.getJoinTables()) {
            existingLookupMap.put(joinTableDesc.getAlias(), joinTableDesc);
        }
        for (JoinTableDesc joinTableDesc : newModel.getJoinTables()) {
            if (existingLookupMap.get(joinTableDesc.getAlias()) == null) {
                issues.add(format(Locale.ROOT, "The join table %s does not existing in existing model",
                        joinTableDesc.getTable()));
                continue;
            }
            JoinTableDesc existingLookup = existingLookupMap.remove(joinTableDesc.getAlias());
            if (!existingLookup.getTable().equals(joinTableDesc.getTable())) {
                issues.add(format(Locale.ROOT,
                        "The join table %s used in existing model is not the same as the updated one %s",
                        existingLookup.getTable(), joinTableDesc.getTable()));
                continue;
            }
            if (!existingLookup.getKind().equals(joinTableDesc.getKind())) {
                issues.add(format(Locale.ROOT,
                        "The TableKind %s in existing model is not the same as the updated one %s for table %s",
                        existingLookup.getKind(), joinTableDesc.getKind(), existingLookup.getTable()));
                continue;
            }
            if (!existingLookup.getJoin().equals(joinTableDesc.getJoin())) {
                issues.add(format(Locale.ROOT, "The join %s is not the same as the existing one %s",
                        joinTableDesc.getJoin(), existingLookup.getJoin()));
            }
        }
        if (existingLookupMap.size() > 0) {
            issues.add(format(Locale.ROOT, "Missing lookup tables %s", existingLookupMap.keySet()));
        }
        // Check partition column
        if (!CheckUtil.equals(existing.getPartitionDesc(), newModel.getPartitionDesc())) {
            issues.add(format(Locale.ROOT, "The partition desc %s is not the same as the existing one %s",
                    newModel.getPartitionDesc(), existing.getPartitionDesc()));
        }
        // Check filter
        if (!CheckUtil.equals(existing.getFilterCondition(), newModel.getFilterCondition())) {
            issues.add(format(Locale.ROOT, "The filter %s is not the same as the existing one %s",
                    newModel.getFilterCondition(), existing.getFilterCondition()));
        }
    }

    public CheckResult allowEdit(DataModelDesc modelDesc, String prj) {

        final String modelName = modelDesc.getName();
        // No model
        DataModelDesc existing = dataModelManager.getDataModelDesc(modelName);
        if (existing == null) {
            return CheckResult.validOnFirstCreate(modelName);
        }
        modelDesc.init(metadataManager.getConfig(), metadataManager.getAllTablesMap(prj));

        // No cube
        List<CubeInstance> cubes = findCubeByModel(modelName);
        if (cubes.size() <= 0) {
            return CheckResult.validOnCompatibleSchema(modelName);
        }

        existing = cubes.get(0).getModel();
        List<String> issues = Lists.newArrayList();
        // Check model related
        checkDataModelCompatible(existing, modelDesc, issues);
        if (!issues.isEmpty()) {
            return CheckResult.invalidOnIncompatibleSchema(modelName, issues);
        }

        // Check cube related
        Set<String> dimensionColumns = Sets.newHashSet();
        for (ModelDimensionDesc modelDimensionDesc : modelDesc.getDimensions()) {
            for (String columnName : modelDimensionDesc.getColumns()) {
                dimensionColumns.add(modelDimensionDesc.getTable() + "." + columnName);
            }
        }
        // Add key related columns
        for (JoinTableDesc joinTableDesc : modelDesc.getJoinTables()) {
            List<TblColRef> keyCols = Lists.newArrayList(joinTableDesc.getJoin().getForeignKeyColumns());
            keyCols.addAll(Lists.newArrayList(joinTableDesc.getJoin().getPrimaryKeyColumns()));
            dimensionColumns.addAll(Lists.transform(keyCols, entry -> entry.getIdentity()));
        }
        Set<String> measureColumns = Sets.newHashSet(modelDesc.getMetrics());
        for (CubeInstance cube : cubes) {
            // Check dimensions
            List<String> cubeDimensionColumns = Lists.newLinkedList();
            for (TblColRef entry : cube.getAllDimensions()) {
                cubeDimensionColumns.add(entry.getIdentity());
            }
            for (MeasureDesc input : cube.getMeasures()) {
                FunctionDesc funcDesc = input.getFunction();
                if (FUNC_TOP_N.equalsIgnoreCase(funcDesc.getExpression())) {
                    List<TblColRef> ret = funcDesc.getParameter().getColRefs();
                    cubeDimensionColumns
                            .addAll(Lists.transform(ret.subList(1, ret.size()), entry -> entry.getIdentity()));
                }
            }
            if (!dimensionColumns.containsAll(cubeDimensionColumns)) {
                cubeDimensionColumns.removeAll(dimensionColumns);
                issues.add(format(Locale.ROOT, "Missing some dimension columns %s for cube %s", cubeDimensionColumns,
                        cube.getName()));
            }
            // Check measures
            List<List<TblColRef>> cubeMeasureTblColRefLists = Lists.transform(cube.getMeasures(), entry -> {
                FunctionDesc funcDesc = entry.getFunction();
                List<TblColRef> ret = funcDesc.getParameter().getColRefs();
                if (FUNC_TOP_N.equalsIgnoreCase(funcDesc.getExpression())) {
                    return Lists.newArrayList(ret.get(0));
                } else {
                    return funcDesc.getParameter().getColRefs();
                }
            });
            List<String> cubeMeasureColumns = Lists.transform(
                    Lists.newArrayList(Iterables.concat(cubeMeasureTblColRefLists)), entry -> entry.getIdentity());
            if (!measureColumns.containsAll(cubeMeasureColumns)) {
                cubeMeasureColumns.removeAll(measureColumns);
                issues.add(format(Locale.ROOT, "Missing some measure columns %s for cube %s", cubeMeasureColumns,
                        cube.getName()));
            }
        }

        if (issues.isEmpty()) {
            return CheckResult.validOnCompatibleSchema(modelName);
        }
        return CheckResult.invalidOnIncompatibleSchema(modelName, issues);
    }
}