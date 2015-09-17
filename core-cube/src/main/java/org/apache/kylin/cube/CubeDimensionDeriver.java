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

package org.apache.kylin.cube;

import java.util.Collection;
import java.util.HashSet;

import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.SQLDigest;

/**
 *
 * the unified logic for defining a sql's dimension
 */
public class CubeDimensionDeriver {

    public static Collection<TblColRef> getDimensionColumns(SQLDigest sqlDigest) {
        Collection<TblColRef> groupByColumns = sqlDigest.groupbyColumns;
        Collection<TblColRef> filterColumns = sqlDigest.filterColumns;

        Collection<MeasureDesc> sortMeasures = sqlDigest.sortMeasures;
        Collection<SQLDigest.OrderEnum> sortOrders = sqlDigest.sortOrders;
                
        Collection<TblColRef> dimensionColumns = new HashSet<TblColRef>();
        dimensionColumns.addAll(groupByColumns);
        dimensionColumns.addAll(filterColumns);
        return dimensionColumns;
    }
}
