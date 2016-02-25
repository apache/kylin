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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.kylin.metadata.tuple.IEvaluatableTuple;

/**
 * @author xjiang
 * 
 */
public class CaseTupleFilter extends TupleFilter {

    private List<TupleFilter> whenFilters;
    private List<TupleFilter> thenFilters;
    private TupleFilter elseFilter;
    private Collection<?> values;
    private int filterIndex;

    public CaseTupleFilter() {
        super(new ArrayList<TupleFilter>(), FilterOperatorEnum.CASE);
        this.filterIndex = 0;
        this.values = Collections.emptyList();
        this.whenFilters = new ArrayList<TupleFilter>();
        this.thenFilters = new ArrayList<TupleFilter>();
        this.elseFilter = null;
    }

    @Override
    public void addChild(TupleFilter child) {
        super.addChild(child);
        if (this.filterIndex % 2 == 0) {
            this.whenFilters.add(child);
        } else {
            this.thenFilters.add(child);
        }
        this.filterIndex++;
    }

    @Override
    public String toString() {
        return "CaseTupleFilter [when=" + whenFilters + ", then=" + thenFilters + ", else=" + elseFilter + ", children=" + children + "]";
    }

    @Override
    public boolean evaluate(IEvaluatableTuple tuple, IFilterCodeSystem<?> cs) {
        if (whenFilters.size() != thenFilters.size()) {
            elseFilter = whenFilters.remove(whenFilters.size() - 1);
        }
        boolean matched = false;
        for (int i = 0; i < whenFilters.size(); i++) {
            TupleFilter whenFilter = whenFilters.get(i);
            if (whenFilter.evaluate(tuple, cs)) {
                TupleFilter thenFilter = thenFilters.get(i);
                thenFilter.evaluate(tuple, cs);
                values = thenFilter.getValues();
                matched = true;
                break;
            }
        }
        if (!matched) {
            if (elseFilter != null) {
                elseFilter.evaluate(tuple, cs);
                values = elseFilter.getValues();
            } else {
                values = Collections.emptyList();
            }
        }

        return true;
    }

    @Override
    public boolean isEvaluable() {
        return false;
    }

    @Override
    public Collection<?> getValues() {
        return this.values;
    }

    @Override
    public void serialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
        //serialize nothing
    }

    @Override
    public void deserialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
    }

}
