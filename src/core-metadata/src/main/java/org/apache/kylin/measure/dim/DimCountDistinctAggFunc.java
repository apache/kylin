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

package org.apache.kylin.measure.dim;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DimCountDistinctAggFunc {
    private static final Logger logger = LoggerFactory.getLogger(DimCountDistinctAggFunc.class);

    public static DimCountDistinctCounter init() {
        return null;
    }

    public static DimCountDistinctCounter initAdd(Object v) {
        DimCountDistinctCounter counter = new DimCountDistinctCounter();
        counter.add(v);
        return counter;
    }

    public static DimCountDistinctCounter add(DimCountDistinctCounter counter, Object v) {
        if (counter == null) {
            counter = new DimCountDistinctCounter();
        }
        counter.add(v);
        return counter;
    }

    public static DimCountDistinctCounter merge(DimCountDistinctCounter counter0, DimCountDistinctCounter counter1) {
        counter0.addAll(counter1);
        return counter0;
    }

    public static long result(DimCountDistinctCounter counter) {
        return counter == null ? 0L : counter.result();
    }

}
