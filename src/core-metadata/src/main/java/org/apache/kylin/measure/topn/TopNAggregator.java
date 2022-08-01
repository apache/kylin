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

package org.apache.kylin.measure.topn;

import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.measure.MeasureAggregator;

/**
 *
 */
@SuppressWarnings("serial")
public class TopNAggregator extends MeasureAggregator<TopNCounter<ByteArray>> {

    int capacity = 0;
    TopNCounter<ByteArray> sum = null;

    @Override
    public void reset() {
        sum = null;
    }

    @Override
    public void aggregate(TopNCounter<ByteArray> value) {
        if (sum == null) {
            capacity = value.getCapacity();
            sum = new TopNCounter<>(capacity * 10);
        }
        sum.merge(value);
    }

    @Override
    public TopNCounter<ByteArray> aggregate(TopNCounter<ByteArray> value1, TopNCounter<ByteArray> value2) {
        int thisCapacity = value1.getCapacity();
        TopNCounter<ByteArray> aggregated = new TopNCounter<>(thisCapacity * 2);
        aggregated.merge(value1);
        aggregated.merge(value2);
        aggregated.retain(thisCapacity);
        return aggregated;
    }

    @Override
    public TopNCounter<ByteArray> getState() {
        sum.retain(capacity);
        return sum;
    }

    @Override
    public int getMemBytesEstimate() {
        return 8 * capacity / 4;
    }

}
