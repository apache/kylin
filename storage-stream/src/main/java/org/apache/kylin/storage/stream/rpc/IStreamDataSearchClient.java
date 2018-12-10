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

package org.apache.kylin.storage.stream.rpc;

import java.util.Set;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.tuple.ITupleIterator;
import org.apache.kylin.metadata.tuple.TupleInfo;

public interface IStreamDataSearchClient {
    /**
     *
     * @param minSegmentTime minimum segment time to search for steaming data
     * @param cube
     * @param tupleInfo
     * @param tupleFilter
     * @param dimensions
     * @param groups
     * @param metrics
     * @return
     */
    ITupleIterator search(long minSegmentTime, CubeInstance cube, TupleInfo tupleInfo, TupleFilter tupleFilter,
            Set<TblColRef> dimensions, Set<TblColRef> groups, Set<FunctionDesc> metrics, int storagePushDownLimit,
            boolean allowStorageAggregation);
}
