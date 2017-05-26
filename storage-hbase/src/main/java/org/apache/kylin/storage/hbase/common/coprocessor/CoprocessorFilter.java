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

package org.apache.kylin.storage.hbase.common.coprocessor;

import java.util.Set;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.dict.DictCodeSystem;
import org.apache.kylin.dimension.IDimensionEncodingMap;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.filter.TupleFilterSerializer;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.IEvaluatableTuple;

/**
 * @author yangli9
 */
public class CoprocessorFilter {

    public static CoprocessorFilter fromFilter(final IDimensionEncodingMap dimEncMap, TupleFilter rootFilter, FilterDecorator.FilterConstantsTreatment filterConstantsTreatment) {
        // translate constants into dictionary IDs via a serialize copy
        FilterDecorator filterDecorator = new FilterDecorator(dimEncMap, filterConstantsTreatment);
        byte[] bytes = TupleFilterSerializer.serialize(rootFilter, filterDecorator, DictCodeSystem.INSTANCE);
        TupleFilter copy = TupleFilterSerializer.deserialize(bytes, DictCodeSystem.INSTANCE);

        return new CoprocessorFilter(copy, filterDecorator.getInevaluableColumns());
    }

    public static byte[] serialize(CoprocessorFilter o) {
        return (o.filter == null) ? BytesUtil.EMPTY_BYTE_ARRAY : TupleFilterSerializer.serialize(o.filter, DictCodeSystem.INSTANCE);
    }

    public static CoprocessorFilter deserialize(byte[] filterBytes) {
        TupleFilter filter = (filterBytes == null || filterBytes.length == 0) ? null //
                : TupleFilterSerializer.deserialize(filterBytes, DictCodeSystem.INSTANCE);
        return new CoprocessorFilter(filter, null);
    }

    // ============================================================================

    private final TupleFilter filter;
    private final Set<TblColRef> inevaluableColumns;

    public CoprocessorFilter(TupleFilter filter, Set<TblColRef> inevaluableColumns) {
        this.filter = filter;
        this.inevaluableColumns = inevaluableColumns;
    }

    public TupleFilter getFilter() {
        return filter;
    }

    public Set<TblColRef> getInevaluableColumns() {
        return inevaluableColumns;
    }

    public boolean evaluate(IEvaluatableTuple tuple) {
        if (filter == null)
            return true;
        else
            return filter.evaluate(tuple, DictCodeSystem.INSTANCE);
    }

}
