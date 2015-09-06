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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.kylin.metadata.tuple.ITuple;

/**
 * @author xjiang
 * 
 */
public class CaseTupleFilter extends TupleFilter {

    private List<TupleFilter> whenFilters;
    private List<TupleFilter> thenFilters;
    private TupleFilter elseFilter;
    private Collection<String> values;
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
    public boolean evaluate(ITuple tuple) {
        if (whenFilters.size() != thenFilters.size()) {
            elseFilter = whenFilters.remove(whenFilters.size() - 1);
        }
        boolean matched = false;
        for (int i = 0; i < whenFilters.size(); i++) {
            TupleFilter whenFilter = whenFilters.get(i);
            if (whenFilter.evaluate(tuple)) {
                TupleFilter thenFilter = thenFilters.get(i);
                thenFilter.evaluate(tuple);
                values = thenFilter.getValues();
                matched = true;
                break;
            }
        }
        if (!matched) {
            if (elseFilter != null) {
                elseFilter.evaluate(tuple);
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
    public Collection<String> getValues() {
        return this.values;
    }

    @Override
    public byte[] serialize() {
        return new byte[0];
    }

    @Override
    public void deserialize(byte[] bytes) {
    }

}
