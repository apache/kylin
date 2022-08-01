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

package org.apache.kylin.measure.basic;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;

@SuppressWarnings({ "rawtypes", "serial" })
public class BasicMeasureType extends MeasureType {

    public static class Factory extends MeasureTypeFactory {

        @Override
        public MeasureType createMeasureType(String funcName, DataType dataType) {
            return new BasicMeasureType(funcName, dataType);
        }

        @Override
        public String getAggrFunctionName() {
            return null;
        }

        @Override
        public String getAggrDataTypeName() {
            return null;
        }

        @Override
        public Class getAggrDataTypeSerializer() {
            return null;
        }
    }

    private final String funcName;
    private final DataType dataType;

    public BasicMeasureType(String funcName, DataType dataType) {
        // note at query parsing phase, the data type may be null, because only function and parameters are known
        this.funcName = funcName;
        this.dataType = dataType;
    }

    @Override
    public void validate(FunctionDesc functionDesc) throws IllegalArgumentException {
        DataType rtype = dataType;

        if (funcName.equals(FunctionDesc.FUNC_SUM)) {
            if (!rtype.isNumberFamily()) {
                throw new IllegalArgumentException(
                        "Return type for function " + funcName + " must be one of " + DataType.NUMBER_FAMILY);
            }
        } else if (funcName.equals(FunctionDesc.FUNC_COUNT)) {
            if (!rtype.isIntegerFamily()) {
                throw new IllegalArgumentException(
                        "Return type for function " + funcName + " must be one of " + DataType.INTEGER_FAMILY);
            }
        } else if (funcName.equals(FunctionDesc.FUNC_MAX) || funcName.equals(FunctionDesc.FUNC_MIN)) {
            if (!rtype.isNumberFamily()) {
                throw new IllegalArgumentException(
                        "Return type for function " + funcName + " must be one of " + DataType.NUMBER_FAMILY);
            }
        } else {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            if (!config.isQueryIgnoreUnknownFunction())
                throw new IllegalArgumentException("Unrecognized function: [" + funcName + "]");
        }
    }

    @Override
    public MeasureIngester<?> newIngester() {
        if (dataType.isIntegerFamily())
            return new LongIngester();
        else if (dataType.isDecimal())
            return new BigDecimalIngester();
        else if (dataType.isNumberFamily())
            return new DoubleIngester();
        else
            throw new IllegalArgumentException("No ingester for aggregation type " + dataType);
    }

    @Override
    public MeasureAggregator<?> newAggregator() {
        if (isSum() || isCount()) {
            if (dataType.isDecimal())
                return new BigDecimalSumAggregator();
            else if (dataType.isIntegerFamily())
                return new LongSumAggregator();
            else if (dataType.isNumberFamily())
                return new DoubleSumAggregator();
        } else if (isMax()) {
            if (dataType.isDecimal())
                return new BigDecimalMaxAggregator();
            else if (dataType.isIntegerFamily())
                return new LongMaxAggregator();
            else if (dataType.isNumberFamily())
                return new DoubleMaxAggregator();
        } else if (isMin()) {
            if (dataType.isDecimal())
                return new BigDecimalMinAggregator();
            else if (dataType.isIntegerFamily())
                return new LongMinAggregator();
            else if (dataType.isNumberFamily())
                return new DoubleMinAggregator();
        }
        throw new IllegalArgumentException(
                "No aggregator for func '" + funcName + "' and return type '" + dataType + "'");
    }

    private boolean isSum() {
        return FunctionDesc.FUNC_SUM.equals(funcName);
    }

    private boolean isCount() {
        return FunctionDesc.FUNC_COUNT.equals(funcName);
    }

    private boolean isMax() {
        return FunctionDesc.FUNC_MAX.equals(funcName);
    }

    private boolean isMin() {
        return FunctionDesc.FUNC_MIN.equals(funcName);
    }

    @Override
    public boolean needRewrite() {
        return true;
    }
}
