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

package org.apache.kylin.metadata.model.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.BiConsumer;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.cube.cuboid.NAggregationGroup;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.RuleBasedIndex;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import lombok.val;

/**
 * helper class for expanding measures in models or indexplan
 */
public class ExpandableMeasureUtil {

    // TODO refactor this
    // Since we cannot access ComputedColumnEvalUtil.evaluateExprAndType from core-metadata,
    // we let the user to provide the function to evaluate cc type
    // this class is used by server-base, smart
    private final BiConsumer<NDataModel, ComputedColumnDesc> evaluateExprAndType;

    public ExpandableMeasureUtil(BiConsumer<NDataModel, ComputedColumnDesc> evaluateExprAndType) {
        this.evaluateExprAndType = evaluateExprAndType;
    }

    // ---------------------------------------
    // index plan
    // ---------------------------------------

    /**
     * expand expand EXPANDABLE measures in index plan request's indexes
     * @param plan
     * @param model
     */
    public static void expandIndexPlanIndexes(IndexPlan plan, NDataModel model) {
        if (plan.getIndexes() == null) {
            return;
        }

        for (IndexEntity index : plan.getIndexes()) {
            if (index.getMeasures() == null) {
                continue;
            }

            Set<Integer> measureIds = Sets.newHashSet(index.getMeasures());
            List<Integer> expandedIds = Lists.newLinkedList();
            for (Integer measureId : index.getMeasures()) {
                if (!model.getEffectiveExpandedMeasures().containsKey(measureId)) {
                    continue;
                }
                for (Integer internalId : model.getEffectiveExpandedMeasures().get(measureId)) {
                    if (!measureIds.contains(internalId)) {
                        measureIds.add(internalId);
                        expandedIds.add(internalId);
                    }
                }
            }
            Collections.sort(expandedIds);

            index.getMeasures().addAll(expandedIds);

            if (index.getLayouts() == null) {
                continue;
            }
            for (LayoutEntity layout : index.getLayouts()) {
                List<Integer> colOrders = Lists.newArrayList(layout.getColOrder());
                colOrders.addAll(expandedIds);
                layout.setColOrder(colOrders);
            }
        }
    }

    /**
     * expand expand EXPANDABLE measures in index plan request's rule based index
     * @param ruleBasedIndex
     * @param model
     */
    public static void expandRuleBasedIndex(RuleBasedIndex ruleBasedIndex, NDataModel model) {
        if (ruleBasedIndex == null || ruleBasedIndex.getAggregationGroups() == null) {
            return;
        }
        for (NAggregationGroup aggGrp : ruleBasedIndex.getAggregationGroups()) {
            if (aggGrp.getMeasures() == null) {
                continue;
            }
            val measureIds = Sets.newHashSet(aggGrp.getMeasures());
            for (Integer measureId : aggGrp.getMeasures()) {
                if (model.getEffectiveExpandedMeasures().containsKey(measureId)) {
                    measureIds.addAll(model.getEffectiveExpandedMeasures().get(measureId));
                }
            }
            val newMeasureIds = measureIds.toArray(new Integer[0]);
            Arrays.sort(newMeasureIds);
            aggGrp.setMeasures(newMeasureIds);
        }
        ruleBasedIndex.adjustMeasures();
        ruleBasedIndex.adjustDimensions();
    }

    // ---------------------------------------
    // model desc
    // ---------------------------------------

    public void deleteExpandableMeasureInternalMeasures(NDataModel model) {
        Set<Integer> toBeTombInternalIds = new HashSet<>();
        Set<Integer> aliveInternalIds = new HashSet<>();
        for (NDataModel.Measure measure : model.getAllMeasures()) {
            if (measure.getType() == NDataModel.MeasureType.EXPANDABLE) {
                if (measure.isTomb()) {
                    toBeTombInternalIds.addAll(measure.getInternalIds());
                } else {
                    aliveInternalIds.addAll(measure.getInternalIds());
                }
            }
        }

        toBeTombInternalIds.removeAll(aliveInternalIds);
        for (NDataModel.Measure measure : model.getAllMeasures()) {
            if (measure.getType() == NDataModel.MeasureType.INTERNAL && toBeTombInternalIds.contains(measure.getId())) {
                measure.setTomb(true);
            }
        }
    }

    /**
     * expand measures (e.g. CORR measure) in current model, may create new CC or new measures
     * @param model
     * @return
     */
    public void expandExpandableMeasure(NDataModel model) {
        List<NDataModel.Measure> measuresBefore = new ArrayList<>(model.getAllMeasures());
        int nextCCIdx = 0;
        int nextMeasureIdx = 0;
        long ccCreationTs = System.currentTimeMillis();
        for (NDataModel.Measure measure : measuresBefore) {
            if (measure.isTomb()) {
                continue;
            }

            MeasureType measureType = measure.getFunction().getMeasureType();
            if (measureType.expandable()) {
                List<FunctionDesc> internalFuncs = measureType.convertToInternalFunctionDesc(measure.getFunction());

                // add missing cc and update func desc params with cc name
                for (FunctionDesc internalFunc : internalFuncs) {
                    for (int i = 0; i < internalFunc.getParameters().size(); i++) {
                        ParameterDesc param = internalFunc.getParameters().get(i);
                        if (!param.getType().equalsIgnoreCase(FunctionDesc.PARAMETER_TYPE_MATH_EXPRESSION)) {
                            continue;
                        }

                        ComputedColumnDesc cc = findOrCreateCC(model, param.getValue(),
                                ComputedColumnUtil.newAutoCCName(ccCreationTs, nextCCIdx++));
                        ParameterDesc ccParam = new ParameterDesc();
                        ccParam.setType(FunctionDesc.PARAMETER_TYPE_COLUMN);
                        ccParam.setValue(cc.getFullName());
                        internalFunc.getParameters().set(i, ccParam);
                    }
                }

                // check and add missing internal measures
                int nextMid = model.getMaxMeasureId() + 1;
                List<NDataModel.Measure> missingInternalMeasures = new LinkedList<>();
                List<Integer> internalMeasureIds = new LinkedList<>();
                for (FunctionDesc internalFunc : internalFuncs) {
                    try {
                        // init col refs
                        // ignore exceptions as internal funcs may refer to newly created CC
                        internalFunc.init(model);
                    } catch (Exception ignored) {
                    }
                    boolean missing = true;
                    for (NDataModel.Measure existingMeasure : model.getAllMeasures()) {
                        if (!existingMeasure.isTomb() && existingMeasure.getFunction().equals(internalFunc)) {
                            internalMeasureIds.add(existingMeasure.getId());
                            missing = false;
                            break;
                        }
                    }
                    if (!missing) {
                        continue;
                    }

                    NDataModel.Measure missingInternalMeasure = new NDataModel.Measure();
                    missingInternalMeasure.setId(nextMid++);
                    missingInternalMeasure.setName(String.format(Locale.ROOT, "%s_%s_%s_%d", measure.getName(), // outter measure
                            internalFunc.getExpression(), // func
                            internalFunc.getParameters().get(0).getValue().replaceAll("[^A-Za-z0-9]", "_"), // col name
                            nextMeasureIdx++ // avoid duplicate names
                    ));
                    missingInternalMeasure.setFunction(internalFunc);
                    missingInternalMeasure.setType(NDataModel.MeasureType.INTERNAL);

                    missingInternalMeasures.add(missingInternalMeasure);
                    internalMeasureIds.add(missingInternalMeasure.getId());
                }

                measure.setInternalIds(internalMeasureIds);
                measure.setType(NDataModel.MeasureType.EXPANDABLE);

                model.getAllMeasures().addAll(missingInternalMeasures);
            }
        }
    }

    /**
     * search for cc by expr, if missing, adding one in the current model
     * @param model
     * @param expr
     * @return
     */
    private ComputedColumnDesc findOrCreateCC(NDataModel model, String expr, String newCCName) {
        ComputedColumnDesc ccDesc = new ComputedColumnDesc();
        ccDesc.setExpression(expr);

        ComputedColumnDesc found = ComputedColumnUtil.findCCByExpr(Lists.newArrayList(model), ccDesc);
        if (found != null) {
            return found;
        }

        List<NDataModel> otherModels = NDataflowManager
                .getInstance(KylinConfig.getInstanceFromEnv(), model.getProject()).listUnderliningDataModels();
        found = ComputedColumnUtil.findCCByExpr(otherModels, ccDesc);
        if (found != null) {
            ccDesc = found;
        } else {
            ccDesc.setTableIdentity(model.getRootFactTableName());
            ccDesc.setTableAlias(model.getRootFactTable().getAlias());
            ccDesc.setComment("Auto generated for CORR measure");
            ccDesc.setDatatype("ANY"); // resolve data type later
            ccDesc.setExpression(expr);
            ccDesc.setColumnName(newCCName);
        }

        model.getComputedColumnDescs().add(ccDesc);
        evaluateExprAndType.accept(model, ccDesc);

        NDataModel.NamedColumn ccColumn = new NDataModel.NamedColumn();
        ccColumn.setName(ccDesc.getColumnName());
        ccColumn.setAliasDotColumn(ccDesc.getFullName());
        ccColumn.setId(model.getMaxColumnId() + 1);
        model.getAllNamedColumns().add(ccColumn);

        return ccDesc;
    }
}
