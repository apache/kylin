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

package org.apache.kylin.query.routing.RoutingRules;

import java.util.Comparator;
import java.util.List;

import org.apache.kylin.common.util.PartialSorter;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.metadata.realization.RealizationType;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RoutingRule;

/**
 * Created by Hongbin Ma(Binmahone) on 1/5/15.
 */
public class SimpleQueryMoreColumnsCubeFirstRule extends RoutingRule {
    @Override
    public void apply(List<IRealization> realizations, OLAPContext olapContext) {
        List<Integer> itemIndexes = super.findRealizationsOf(realizations, RealizationType.CUBE);

        if (olapContext.isSimpleQuery()) {
            PartialSorter.partialSort(realizations, itemIndexes, new Comparator<IRealization>() {
                @Override
                public int compare(IRealization o1, IRealization o2) {
                    CubeInstance c1 = (CubeInstance) o1;
                    CubeInstance c2 = (CubeInstance) o2;
                    return c1.getDescriptor().listDimensionColumnsIncludingDerived().size() - c2.getDescriptor().listDimensionColumnsIncludingDerived().size();
                }
            });
        }
    }
}
