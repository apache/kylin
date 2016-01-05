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

package org.apache.kylin.measure.bitmap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by sunyerui on 15/12/22.
 */
public class BitmapDistinctCountAggFunc {

    private static final Logger logger = LoggerFactory.getLogger(BitmapDistinctCountAggFunc.class);

    public static BitmapCounter init() {
        return null;
    }

    public static BitmapCounter add(BitmapCounter counter, Object v) {
        BitmapCounter c = (BitmapCounter) v;
        if (counter == null) {
            return new BitmapCounter(c);
        } else {
            counter.merge(c);
            return counter;
        }
    }

    public static BitmapCounter merge(BitmapCounter counter0, Object counter1) {
        return add(counter0, counter1);
    }

    public static long result(BitmapCounter counter) {
        return counter == null ? 0L : counter.getCount();
    }
}
