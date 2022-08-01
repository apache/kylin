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

package org.apache.kylin.query.relnode;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.kylin.measure.bitmap.BitmapMeasureType;
import org.apache.kylin.measure.hllc.HLLCMeasureType;
import org.apache.kylin.metadata.model.FunctionDesc;

public class KylinAggregateCall extends AggregateCall {

    private FunctionDesc func;

    public KylinAggregateCall(AggregateCall aggCall, FunctionDesc func) {
        super(aggCall.getAggregation(), aggCall.isDistinct(), aggCall.getArgList(), aggCall.type, aggCall.name);
        this.func = func;
    }

    public boolean isSum0() {
        return "$SUM0".equals(getAggregation().getName());
    }

    public FunctionDesc getFunc() {
        return func;
    }

    public boolean isHllCountDistinctFunc() {
        return (this.getFunc().getExpression().equalsIgnoreCase(FunctionDesc.FUNC_COUNT_DISTINCT)
                && this.getFunc().getReturnDataType().getName().equalsIgnoreCase(HLLCMeasureType.DATATYPE_HLLC));
    }

    public boolean isBitmapCountDistinctFunc() {
        return (this.getFunc().getExpression().equalsIgnoreCase(FunctionDesc.FUNC_COUNT_DISTINCT)
                && this.getFunc().getReturnDataType().getName().equalsIgnoreCase(BitmapMeasureType.DATATYPE_BITMAP)
                && this.getAggregation().getName().equalsIgnoreCase(FunctionDesc.FUNC_COUNT_DISTINCT));
    }
}
