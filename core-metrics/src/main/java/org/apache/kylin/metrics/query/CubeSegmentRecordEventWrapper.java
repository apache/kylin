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

package org.apache.kylin.metrics.query;

import org.apache.kylin.metrics.lib.impl.RecordEvent;
import org.apache.kylin.metrics.lib.impl.RecordEventWrapper;

import com.google.common.base.Strings;

public class CubeSegmentRecordEventWrapper extends RecordEventWrapper {

    public CubeSegmentRecordEventWrapper(RecordEvent metricsEvent) {
        super(metricsEvent);

        initStats();
    }

    private void initStats() {
        this.metricsEvent.put(PropertyEnum.CALL_COUNT.toString(), 0L);
        this.metricsEvent.put(PropertyEnum.TIME_SUM.toString(), 0L);
        this.metricsEvent.put(PropertyEnum.TIME_MAX.toString(), 0L);
        this.metricsEvent.put(PropertyEnum.SKIP_COUNT.toString(), 0L);
        this.metricsEvent.put(PropertyEnum.SCAN_COUNT.toString(), 0L);
        this.metricsEvent.put(PropertyEnum.RETURN_COUNT.toString(), 0L);
        this.metricsEvent.put(PropertyEnum.AGGR_FILTER_COUNT.toString(), 0L);
        this.metricsEvent.put(PropertyEnum.AGGR_COUNT.toString(), 0L);
        this.metricsEvent.put(PropertyEnum.IF_SUCCESS.toString(), true);
    }

    public void setWrapper(String projectName, String cubeName, String segmentName, long sourceCuboidId,
            long targetCuboidId, long filterMask) {
        this.metricsEvent.put(PropertyEnum.PROJECT.toString(), projectName);
        this.metricsEvent.put(PropertyEnum.CUBE.toString(), cubeName);
        this.metricsEvent.put(PropertyEnum.SEGMENT.toString(), segmentName);
        this.metricsEvent.put(PropertyEnum.CUBOID_SOURCE.toString(), sourceCuboidId);
        this.metricsEvent.put(PropertyEnum.CUBOID_TARGET.toString(), targetCuboidId);
        this.metricsEvent.put(PropertyEnum.IF_MATCH.toString(), sourceCuboidId == targetCuboidId);
        this.metricsEvent.put(PropertyEnum.FILTER_MASK.toString(), filterMask);
    }

    public void setWeightPerHit(double weightPerHit) {
        this.metricsEvent.put(PropertyEnum.WEIGHT_PER_HIT.toString(), weightPerHit);
    }

    public void addRPCStats(long callTimeMs, long skipCount, long scanCount, long returnCount, long aggrCount,
            boolean ifSuccess) {
        Long curCallCount = (Long) this.metricsEvent.get(PropertyEnum.CALL_COUNT.toString());
        Long curTimeSum = (Long) this.metricsEvent.get(PropertyEnum.TIME_SUM.toString());
        Long curTimeMax = (Long) this.metricsEvent.get(PropertyEnum.TIME_MAX.toString());
        Long curSkipCount = (Long) this.metricsEvent.get(PropertyEnum.SKIP_COUNT.toString());
        Long curScanCount = (Long) this.metricsEvent.get(PropertyEnum.SCAN_COUNT.toString());
        Long curReturnCount = (Long) this.metricsEvent.get(PropertyEnum.RETURN_COUNT.toString());
        Long curAggrAndFilterCount = (Long) this.metricsEvent.get(PropertyEnum.AGGR_FILTER_COUNT.toString());
        Long curAggrCount = (Long) this.metricsEvent.get(PropertyEnum.AGGR_COUNT.toString());
        Boolean curIfSuccess = (Boolean) this.metricsEvent.get(PropertyEnum.IF_SUCCESS.toString());

        this.metricsEvent.put(PropertyEnum.CALL_COUNT.toString(), curCallCount + 1);
        this.metricsEvent.put(PropertyEnum.TIME_SUM.toString(), curTimeSum + callTimeMs);
        if (curTimeMax < callTimeMs) {
            this.metricsEvent.put(PropertyEnum.TIME_MAX.toString(), callTimeMs);
        }
        this.metricsEvent.put(PropertyEnum.SKIP_COUNT.toString(), curSkipCount + skipCount);
        this.metricsEvent.put(PropertyEnum.SCAN_COUNT.toString(), curScanCount + scanCount);
        this.metricsEvent.put(PropertyEnum.RETURN_COUNT.toString(), curReturnCount + returnCount);
        this.metricsEvent.put(PropertyEnum.AGGR_FILTER_COUNT.toString(),
                curAggrAndFilterCount + scanCount - returnCount);
        this.metricsEvent.put(PropertyEnum.AGGR_COUNT.toString(), curAggrCount + aggrCount);
        this.metricsEvent.put(PropertyEnum.IF_SUCCESS.toString(), curIfSuccess && ifSuccess);
    }

    public Object getProperty(String key) {
        return this.metricsEvent.get(key);
    }

    public enum PropertyEnum {
        PROJECT("PROJECT"), CUBE("CUBE_NAME"), SEGMENT("SEGMENT_NAME"), CUBOID_SOURCE("CUBOID_SOURCE"), CUBOID_TARGET(
                "CUBOID_TARGET"), IF_MATCH("IF_MATCH"), FILTER_MASK("FILTER_MASK"), IF_SUCCESS("IF_SUCCESS"), //
        TIME_SUM("STORAGE_CALL_TIME_SUM"), TIME_MAX("STORAGE_CALL_TIME_MAX"), WEIGHT_PER_HIT(
                "WEIGHT_PER_HIT"), CALL_COUNT("STORAGE_CALL_COUNT"), SKIP_COUNT("STORAGE_COUNT_SKIP"), SCAN_COUNT(
                        "STORAGE_COUNT_SCAN"), RETURN_COUNT("STORAGE_COUNT_RETURN"), AGGR_FILTER_COUNT(
                                "STORAGE_COUNT_AGGREGATE_FILTER"), AGGR_COUNT("STORAGE_COUNT_AGGREGATE");

        private final String propertyName;

        PropertyEnum(String name) {
            this.propertyName = name;
        }

        public static PropertyEnum getByName(String name) {
            if (Strings.isNullOrEmpty(name)) {
                return null;
            }
            for (PropertyEnum property : PropertyEnum.values()) {
                if (property.propertyName.equals(name.toUpperCase())) {
                    return property;
                }
            }

            return null;
        }

        @Override
        public String toString() {
            return propertyName;
        }
    }

}