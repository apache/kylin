/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.metadata.validation.rule;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.metadata.model.cube.CubeDesc;
import com.kylinolap.metadata.model.cube.DimensionDesc;
import com.kylinolap.metadata.model.cube.HierarchyDesc;
import com.kylinolap.metadata.validation.IValidatorRule;
import com.kylinolap.metadata.validation.ResultLevel;
import com.kylinolap.metadata.validation.ValidateContext;

/**
 * Rule to validate:
 * 1. The aggregationGroup size must be less than 20
 * 2. hierarchy column must NOT across aggregation group
 * 
 * @author jianliu
 *
 */
public class AggregationGroupSizeRule implements IValidatorRule<CubeDesc> {

    /* (non-Javadoc)
     * @see com.kylinolap.metadata.validation.IValidatorRule#validate(java.lang.Object, com.kylinolap.metadata.validation.ValidateContext)
     */
    @Override
    public void validate(CubeDesc cube, ValidateContext context) {
        innerValidateMaxSize(cube, context);
        //        innerValidateHierarchy(cube, context);
    }

    /**
     * @param cube
     * @param context
     */
    private void innerValidateHierarchy(CubeDesc cube, ValidateContext context) {
        String[][] groups = cube.getRowkey().getAggregationGroups();
        Map<String, Set<String>> hierMap = getHierMap(cube, context);
        Iterator<Entry<String, Set<String>>> it = hierMap.entrySet().iterator();
        while (it.hasNext()) {
            Set<String> set = it.next().getValue();
            validateGroups(set, context, groups);
        }

    }

    /**
     * @param set
     * @param context
     * @param groups
     */
    private void validateGroups(Set<String> set, ValidateContext context, String[][] groups) {
        int sum = 0;
        for (int i = 0; i < groups.length; i++) {
            String[] group = groups[i];
            int result = validateGroup1(set, context, group);
            sum = sum + result;
        }
        if (sum > 1) {
            context.addResult(ResultLevel.ERROR, "Hierachy column [" + set + "] was referenced in " + sum
                    + " groups ");
        }
    }

    /**
     * Only validate "hierarchy dimensions not across aggregation groups".
     * 
     * @param set
     * @param existingHier
     * @param context
     * @param group
     * @return
     */
    private int validateGroup1(Set<String> set, ValidateContext context, String[] group) {
        int sum = 0;
        for (int i = 0; i < group.length; i++) {
            if (set.contains(group[i])) {
                sum++;
            }
        }
        if (sum == 0) {
            return 0;
        } else if (sum > set.size()) {
            context.addResult(ResultLevel.ERROR, "Aggregation group [" + ArrayUtils.toString(group)
                    + "] contains duplicated hierachy columns [ " + set + "]");
            return 1;
        } else {
            return 1;
        }
    }

    /**
     * @param cube
     * @param context
     */
    private Map<String, Set<String>> getHierMap(CubeDesc cube, ValidateContext context) {
        Map<String, Set<String>> hierMap = new HashMap<String, Set<String>>();
        List<DimensionDesc> dims = cube.getDimensions();
        Iterator<DimensionDesc> it = dims.iterator();
        while (it.hasNext()) {
            Set<String> cset = new HashSet<String>();
            Set<String> lset = new HashSet<String>();
            DimensionDesc dim = it.next();
            HierarchyDesc[] hiers = dim.getHierarchy();
            if (hiers == null) {
                continue;
            }
            for (int i = 0; i < hiers.length; i++) {
                if (cset.contains(hiers[i].getColumn())) {
                    context.addResult(ResultLevel.ERROR, "Duplicate column " + hiers[i].getColumn()
                            + " in dimension " + dim.getName());
                } else {
                    cset.add(hiers[i].getColumn());
                }
                if (lset.contains(hiers[i].getLevel())) {
                    context.addResult(ResultLevel.ERROR, "Duplicate level " + hiers[i].getLevel()
                            + " in dimension " + dim.getName());
                } else {
                    lset.add(hiers[i].getLevel());
                }
            }
            hierMap.put(dim.getName(), cset);
        }
        return hierMap;

    }

    /**
     * @param cube
     * @param context
     */
    private void innerValidateMaxSize(CubeDesc cube, ValidateContext context) {
        int maxSize = getMaxAgrGroupSize();
        String[][] groups = cube.getRowkey().getAggregationGroups();
        for (int i = 0; i < groups.length; i++) {
            String[] group = groups[i];
            if (group.length >= maxSize) {
                context.addResult(ResultLevel.ERROR, "Length of the number " + i
                        + " aggregation group's length should be less that " + maxSize);
            }
        }
    }

    protected int getMaxAgrGroupSize() {
        String size =
                KylinConfig.getInstanceFromEnv().getProperty(KEY_MAX_AGR_GROUP_SIZE,
                        String.valueOf(DEFAULT_MAX_AGR_GROUP_SIZE));
        return Integer.parseInt(size);
    }
}
