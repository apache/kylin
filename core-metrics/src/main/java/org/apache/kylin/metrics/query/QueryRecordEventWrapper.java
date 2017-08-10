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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

public class QueryRecordEventWrapper extends RecordEventWrapper {

    private static final Logger logger = LoggerFactory.getLogger(QueryRecordEventWrapper.class);

    public QueryRecordEventWrapper(RecordEvent metricsEvent) {
        super(metricsEvent);
    }

    public void setWrapper(long queryHashCode, String queryType, String projectName, String realizationName,
            int realizationType, Throwable throwable) {
        this.metricsEvent.put(PropertyEnum.ID_CODE.toString(), queryHashCode);
        this.metricsEvent.put(PropertyEnum.TYPE.toString(), queryType);
        this.metricsEvent.put(PropertyEnum.PROJECT.toString(), projectName);
        this.metricsEvent.put(PropertyEnum.REALIZATION.toString(), realizationName);
        this.metricsEvent.put(PropertyEnum.REALIZATION_TYPE.toString(), realizationType);
        this.metricsEvent.put(PropertyEnum.EXCEPTION.toString(),
                throwable == null ? "NULL" : throwable.getClass().getName());
    }

    public void setStats(long callTimeMs, long returnCountByCalcite, long returnCountByStorage) {
        this.metricsEvent.put(PropertyEnum.TIME_COST.toString(), callTimeMs);
        this.metricsEvent.put(PropertyEnum.CALCITE_RETURN_COUNT.toString(), returnCountByCalcite);
        this.metricsEvent.put(PropertyEnum.STORAGE_RETURN_COUNT.toString(), returnCountByStorage);
        long countAggrAndFilter = returnCountByStorage - returnCountByCalcite;
        if (countAggrAndFilter < 0) {
            countAggrAndFilter = 0;
            logger.warn(returnCountByStorage + " rows returned by storage less than " + returnCountByCalcite
                    + " rows returned by calcite");
        }
        this.metricsEvent.put(PropertyEnum.AGGR_FILTER_COUNT.toString(), countAggrAndFilter);
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