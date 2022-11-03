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

package org.apache.kylin.metadata.realization;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.IStorageAware;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.TblColRef;

public interface IRealization extends IStorageAware {

    /**
     * Given the features of a query, check how capable the realization is to answer the query.
     */
    CapabilityResult isCapable(SQLDigest digest, List<NDataSegment> prunedSegments,
            Map<String, Set<Long>> secondStorageSegmentLayoutMap);

    CapabilityResult isCapable(SQLDigest digest, List<NDataSegment> prunedSegments,
            List<NDataSegment> prunedStreamingSegments, Map<String, Set<Long>> secondStorageSegmentLayoutMap);

    /**
     * Get whether this specific realization is a cube or InvertedIndex
     */
    String getType();

    KylinConfig getConfig();

    NDataModel getModel();

    Set<TblColRef> getAllColumns();

    List<TblColRef> getAllDimensions();

    List<MeasureDesc> getMeasures();

    List<IRealization> getRealizations();

    FunctionDesc findAggrFunc(FunctionDesc aggrFunc);

    String getProject();

    boolean isReady();

    String getUuid();

    String getCanonicalName();

    long getDateRangeStart();

    long getDateRangeEnd();

    int getCost();

    boolean hasPrecalculatedFields();

    boolean isStreaming();

}
