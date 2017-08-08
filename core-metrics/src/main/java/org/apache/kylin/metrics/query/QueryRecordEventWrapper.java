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

public class QueryRecordEventWrapper extends RecordEventWrapper {

    public QueryRecordEventWrapper(RecordEvent metricsEvent) {
        super(metricsEvent);
        initStats();
    }

    private void initStats() {
        this.metricsEvent.put(PropertyEnum.EXCEPTION.toString(), "NULL");
        this.metricsEvent.put(PropertyEnum.TIME_COST.toString(), 0L);
        this.metricsEvent.put(PropertyEnum.CALCITE_RETURN_COUNT.toString(), 0L);
        this.metricsEvent.put(PropertyEnum.STORAGE_RETURN_COUNT.toString(), 0L);
        setDependentStats();
    }

    public void setWrapper(long queryHashCode, String queryType, String projectName, String realizationName,
            int realizationType) {
        this.metricsEvent.put(PropertyEnum.ID_CODE.toString(), queryHashCode);
        this.metricsEvent.put(PropertyEnum.TYPE.toString(), queryType);
        this.metricsEvent.put(PropertyEnum.PROJECT.toString(), projectName);
        this.metricsEvent.put(PropertyEnum.REALIZATION.toString(), realizationName);
        this.metricsEvent.put(PropertyEnum.REALIZATION_TYPE.toString(), realizationType);
    }

    public void addStorageStats(long addReturnCountByStorage) {
        Long curReturnCountByStorage = (Long) this.metricsEvent.get(PropertyEnum.STORAGE_RETURN_COUNT.toString());
        this.metricsEvent.put(PropertyEnum.STORAGE_RETURN_COUNT.toString(),
                curReturnCountByStorage + addReturnCountByStorage);
    }

    public void setStats(long callTimeMs, long returnCountByCalcite) {
        this.metricsEvent.put(PropertyEnum.TIME_COST.toString(), callTimeMs);
        this.metricsEvent.put(PropertyEnum.CALCITE_RETURN_COUNT.toString(), returnCountByCalcite);
        setDependentStats();
    }

    private void setDependentStats() {
        this.metricsEvent.put(PropertyEnum.AGGR_FILTER_COUNT.toString(),
                Math.max(0L, (Long) this.metricsEvent.get(PropertyEnum.STORAGE_RETURN_COUNT.toString())
                        - (Long) this.metricsEvent.get(PropertyEnum.CALCITE_RETURN_COUNT.toString())));
    }

    public <T extends Throwable> void setStats(Class<T> exceptionClassName) {
        this.metricsEvent.put(PropertyEnum.EXCEPTION.toString(), exceptionClassName.getName());
    }

    public enum PropertyEnum {
        ID_CODE("QUERY_HASH_CODE"), TYPE("QUERY_TYPE"), PROJECT("PROJECT"), REALIZATION(
                "REALIZATION"), REALIZATION_TYPE("REALIZATION_TYPE"), EXCEPTION("EXCEPTION"), //
        TIME_COST("QUERY_TIME_COST"), CALCITE_RETURN_COUNT("CALCITE_COUNT_RETURN"), STORAGE_RETURN_COUNT(
                "STORAGE_COUNT_RETURN"), AGGR_FILTER_COUNT("CALCITE_COUNT_AGGREGATE_FILTER");

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