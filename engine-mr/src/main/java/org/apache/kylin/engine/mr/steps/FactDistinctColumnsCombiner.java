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

package org.apache.kylin.engine.mr.steps;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.kylin.engine.mr.KylinReducer;

/**
 * @author yangli9
 */
public class FactDistinctColumnsCombiner extends KylinReducer<SelfDefineSortableKey, Text, SelfDefineSortableKey, Text> {

    @Override
    protected void doSetup(Context context) throws IOException {
        super.bindCurrentConfiguration(context.getConfiguration());
    }

    @Override
    public void doReduce(SelfDefineSortableKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        // for hll, each key only has one output, no need to do local combine;
        // for normal col, values are empty text
        context.write(key, values.iterator().next());
    }

}
