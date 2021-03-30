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

package org.apache.kylin.gridtable;

import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.kylin.common.util.BytesSerializer;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.ImmutableBitSet;

public class GTTwoLayerAggregateParam {
    final ImmutableBitSet vanishDimMask; // must be prefix
    final ImmutableBitSet outsideLayerMetrics;
    final String[] outsideLayerMetricsFuncs;
    final int[] insideLayerMetrics;
    final String[] insideLayerMetricsFuncs;

    public GTTwoLayerAggregateParam() {
        this.vanishDimMask = new ImmutableBitSet(new BitSet());
        this.outsideLayerMetrics = new ImmutableBitSet(new BitSet());
        this.outsideLayerMetricsFuncs = new String[0];
        this.insideLayerMetrics = new int[0];
        this.insideLayerMetricsFuncs = new String[0];
    }

    public GTTwoLayerAggregateParam(ImmutableBitSet vanishDimMask, ImmutableBitSet outsideLayerMetrics,
            String[] outsideLayerMetricsFuncs, int[] insideLayerMetrics, String[] insideLayerMetricsFuncs) {
        this.vanishDimMask = vanishDimMask;
        this.outsideLayerMetrics = outsideLayerMetrics;
        this.outsideLayerMetricsFuncs = outsideLayerMetricsFuncs;
        this.insideLayerMetrics = insideLayerMetrics;
        this.insideLayerMetricsFuncs = insideLayerMetricsFuncs;
    }

    public boolean ifEnabled() {
        return outsideLayerMetrics != null && outsideLayerMetrics.cardinality() > 0;
    }

    public boolean satisfyPrefix(ImmutableBitSet dimMask) {
        for (int i = 0; i < vanishDimMask.trueBitCount(); i++) {
            int c = dimMask.trueBitAt(i);
            if (!vanishDimMask.get(c)) {
                return false;
            }
        }
        return true;
    }

    public static final BytesSerializer<GTTwoLayerAggregateParam> serializer = new BytesSerializer<GTTwoLayerAggregateParam>() {
        @Override
        public void serialize(GTTwoLayerAggregateParam value, ByteBuffer out) {
            if (!value.ifEnabled()) {
                BytesUtil.writeVInt(0, out);
                return;
            }
            BytesUtil.writeVInt(1, out);
            ImmutableBitSet.serializer.serialize(value.vanishDimMask, out);
            ImmutableBitSet.serializer.serialize(value.outsideLayerMetrics, out);
            BytesUtil.writeAsciiStringArray(value.outsideLayerMetricsFuncs, out);
            BytesUtil.writeIntArray(value.insideLayerMetrics, out);
            BytesUtil.writeAsciiStringArray(value.insideLayerMetricsFuncs, out);
        }

        @Override
        public GTTwoLayerAggregateParam deserialize(ByteBuffer in) {
            int ifEnabled = BytesUtil.readVInt(in);
            if (ifEnabled != 1) {
                return new GTTwoLayerAggregateParam();
            }
            ImmutableBitSet vDimMask = ImmutableBitSet.serializer.deserialize(in);
            ImmutableBitSet outLMetrics = ImmutableBitSet.serializer.deserialize(in);
            String[] outLMetricsFuncs = BytesUtil.readAsciiStringArray(in);
            int[] inLMetrics = BytesUtil.readIntArray(in);
            String[] inLMetricsFuncs = BytesUtil.readAsciiStringArray(in);

            return new GTTwoLayerAggregateParam(vDimMask, outLMetrics, outLMetricsFuncs, inLMetrics, inLMetricsFuncs);
        }
    };
}