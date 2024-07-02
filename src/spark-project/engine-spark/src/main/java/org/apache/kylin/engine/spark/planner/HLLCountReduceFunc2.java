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

package org.apache.kylin.engine.spark.planner;

import java.io.IOException;
import java.util.Locale;

import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.spark.api.java.function.Function2;

public class HLLCountReduceFunc2 implements Function2<byte[], byte[], byte[]> {

    private final int precision;

    public HLLCountReduceFunc2(int precision) {
        this.precision = precision;
    }

    @Override
    public byte[] call(byte[] bytes1, byte[] bytes2) throws Exception {
        int f1 = HLLCountCounter.readFlag(bytes1);
        int f2 = HLLCountCounter.readFlag(bytes2);
        if (f1 != f2) {
            // this should not happen
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Bytes flag not matched %d %d", f1, f2));
        }

        switch (f1) {
        case HLLCountCounter.PLAIN:
            return mergePlain(bytes1, bytes2);
        case HLLCountCounter.HLL:
            return mergeHll(bytes1, bytes2);
        default:
            throw new IllegalArgumentException("Unknown bytes flag " + f1);
        }
    }

    private byte[] mergePlain(byte[] bytes1, byte[] bytes2) {
        long count1 = HLLCountCounter.readPlainCount(bytes1);
        long count2 = HLLCountCounter.readPlainCount(bytes2);
        return HLLCountCounter.plainBytes(count1 + count2);
    }

    private byte[] mergeHll(byte[] bytes1, byte[] bytes2) throws IOException {
        HLLCounter hll1 = HLLCountCounter.readHllCounter(bytes1, precision);
        HLLCounter hll2 = HLLCountCounter.readHllCounter(bytes2, precision);
        hll1.merge(hll2);
        return HLLCountCounter.hllBytes(hll1);
    }
}
