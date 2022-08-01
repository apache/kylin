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
package org.apache.kylin.storage;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.TupleInfo;
import org.apache.kylin.metadata.model.NDataModel;

import com.google.common.collect.Lists;

/**
 */
public class StorageMockUtils {

    final NDataModel model;

    public StorageMockUtils(NDataModel model) {
        this.model = model;
    }

    public TupleInfo newTupleInfo(List<TblColRef> groups, List<FunctionDesc> aggregations) {
        TupleInfo info = new TupleInfo();
        int idx = 0;

        for (TblColRef col : groups) {
            info.setField(col.getName(), col, idx++);
        }

        TableRef sourceTable = groups.get(0).getTableRef();
        for (FunctionDesc func : aggregations) {
            ColumnDesc colDesc = func.newFakeRewriteColumn(sourceTable.getTableDesc());
            TblColRef col = sourceTable.makeFakeColumn(colDesc);
            info.setField(col.getName(), col, idx++);
        }

        return info;
    }

    public List<TblColRef> buildGroups() {
        List<TblColRef> groups = new ArrayList<TblColRef>();

        TblColRef c1 = model.findColumn("DEFAULT.TEST_KYLIN_FACT.CAL_DT");
        groups.add(c1);

        TblColRef c2 = model.findColumn("DEFAULT.TEST_CATEGORY_GROUPINGS.META_CATEG_NAME");
        groups.add(c2);

        return groups;
    }

    public List<FunctionDesc> buildAggregations1() {
        List<FunctionDesc> functions = new ArrayList<FunctionDesc>();

        TblColRef priceCol = model.findColumn("DEFAULT.TEST_KYLIN_FACTPRICE");

        FunctionDesc f1 = FunctionDesc.newInstance("SUM", //
                Lists.newArrayList(ParameterDesc.newInstance(priceCol)), "decimal(19,4)");
        functions.add(f1);

        return functions;
    }

    public List<FunctionDesc> buildAggregations() {
        List<FunctionDesc> functions = new ArrayList<FunctionDesc>();

        TblColRef priceCol = model.findColumn("DEFAULT.TEST_KYLIN_FACT.PRICE");
        TblColRef sellerCol = model.findColumn("DEFAULT.TEST_KYLIN_FACT.SELLER_ID");

        FunctionDesc f1 = FunctionDesc.newInstance("SUM", //
                Lists.newArrayList(ParameterDesc.newInstance(priceCol)), "decimal(19,4)");
        functions.add(f1);

        FunctionDesc f2 = FunctionDesc.newInstance("COUNT_DISTINCT", //
                Lists.newArrayList(ParameterDesc.newInstance(priceCol)), "hllc(10)");
        functions.add(f2);

        return functions;
    }
}
