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

package org.apache.kylin.metadata.filter;

import java.util.IdentityHashMap;

import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.collect.Maps;

/**
 */
public class TsConditionEraser implements TupleFilterSerializer.Decorator {

    private final TblColRef tsColumn;
    private final TupleFilter root;

    private IdentityHashMap<TupleFilter, Boolean> isInTopLevelANDs;

    public TsConditionEraser(TblColRef tsColumn, TupleFilter root) {
        this.tsColumn = tsColumn;
        this.root = root;
        this.isInTopLevelANDs = Maps.newIdentityHashMap();
    }

    /**
     * replace filter on timestamp column to null, so that two tuple filter trees can
     * be compared regardless of the filter condition on timestamp column (In top level where conditions concatenated by ANDs)
     * @param filter
     * @return
     */
    @Override
    public TupleFilter onSerialize(TupleFilter filter) {

        if (filter == null)
            return null;

        //we just need reference equal
        if (root == filter) {
            isInTopLevelANDs.put(filter, true);
        }

        if (isInTopLevelANDs.containsKey(filter)) {
            classifyChildrenByMarking(filter);

            if (filter instanceof CompareTupleFilter) {
                TblColRef c = ((CompareTupleFilter) filter).getColumn();
                if (c != null && c.equals(tsColumn)) {
                    return null;
                }
            }
        }

        return filter;
    }

    private void classifyChildrenByMarking(TupleFilter filter) {
        if (filter instanceof LogicalTupleFilter) {
            if (filter.getOperator() == TupleFilter.FilterOperatorEnum.AND) {
                for (TupleFilter child : filter.getChildren()) {
                    isInTopLevelANDs.put(child, true);
                }
            }
        }
    }
}
