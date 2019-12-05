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

package io.kyligence.kap.engine.spark.stats.analyzer.delegate;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.spark.sql.Row;

public class NumericalColumnAnalysisDelegate extends AbstractColumnAnalysisDelegate<NumericalColumnAnalysisDelegate> {

    private double maxValue = Double.NaN;

    private double minValue = Double.NaN;

    public NumericalColumnAnalysisDelegate(ColumnDesc columnDesc) {
        super(columnDesc);
    }

    @Override
    public void analyze(Row row, Object colValue) {
        final String strValue = colValue == null ? null : colValue.toString();
        if (StringUtils.isBlank(strValue)) {
            return;
        }

        if (!NumberUtils.isNumber(strValue)) {
            return;
        }

        final double value = NumberUtils.createDouble(strValue);
        if (Double.isNaN(maxValue) || value > maxValue) {
            maxValue = value;
        }

        if (Double.isNaN(minValue) || value < minValue) {
            minValue = value;
        }

    }

    @Override
    protected NumericalColumnAnalysisDelegate doReduce(NumericalColumnAnalysisDelegate another) {
        this.maxValue = this.getMax() > another.getMax() ? this.maxValue : another.maxValue;
        this.minValue = this.getMin() < another.getMin() ? this.minValue : another.minValue;

        return this;
    }

    public double getMax() {
        return maxValue;
    }

    public double getMin() {
        return minValue;
    }

}
