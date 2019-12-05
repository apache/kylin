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
import org.apache.kylin.measure.hllc.HLLCSerializer;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.spark.sql.Row;

import java.nio.ByteBuffer;

public class ColumnCardinalityAnalysisDelegate
        extends AbstractColumnAnalysisDelegate<ColumnCardinalityAnalysisDelegate> {

    private static final HLLCSerializer HLLC_SERIALIZER = new HLLCSerializer(DataType.getType("hllc14"));

    private transient HLLCounter hllCounter = new HLLCounter(14);

    private boolean hasNullOrBlank = false;

    public ColumnCardinalityAnalysisDelegate(ColumnDesc columnDesc) {
        super(columnDesc);
    }

    @Override
    public void analyze(Row row, Object colValue) {
        final String strValue = colValue == null ? null : colValue.toString();
        if (StringUtils.isBlank(strValue)) {
            hasNullOrBlank = true;
            return;
        }

        hllCounter.add(strValue);
    }

    @Override
    public void encode() {
        final ByteBuffer out = ByteBuffer.allocate(hllCounter.maxLength());
        HLLC_SERIALIZER.serialize(hllCounter, out);
        this.buffer = out.array();
    }

    @Override
    public void decode() {
        final ByteBuffer in = ByteBuffer.wrap(this.buffer);
        hllCounter = HLLC_SERIALIZER.deserialize(in);
    }

    @Override
    protected ColumnCardinalityAnalysisDelegate doReduce(ColumnCardinalityAnalysisDelegate another) {
        this.hasNullOrBlank = (this.hasNullOrBlank || another.hasNullOrBlank);
        this.hllCounter.merge(another.hllCounter);
        return this;
    }

    public long getCardinality() {
        if (hasNullOrBlank) {
            return hllCounter.getCountEstimate() + 1L;
        }

        return hllCounter.getCountEstimate();
    }

    public HLLCounter getHllC() {
        return hllCounter;
    }
}
