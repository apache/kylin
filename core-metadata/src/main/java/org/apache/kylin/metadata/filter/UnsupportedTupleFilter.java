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

import org.apache.kylin.metadata.tuple.IEvaluatableTuple;

public class UnsupportedTupleFilter extends TupleFilter {

    //    protected UnsupportedTupleFilter(List<TupleFilter> filters, FilterOperatorEnum op) {
    //        super(filters, op);
    //    }

    public UnsupportedTupleFilter(FilterOperatorEnum op) {
        super(new ArrayList<TupleFilter>(), op);
    }

    @Override
    public boolean isEvaluable() {
        return false;
    }

    @Override
    public boolean evaluate(IEvaluatableTuple tuple, IFilterCodeSystem<?> cs) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<?> getValues() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void serialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
    }

    @Override
    public void deserialize(IFilterCodeSystem<?> cs, ByteBuffer buffer) {
    }
}
