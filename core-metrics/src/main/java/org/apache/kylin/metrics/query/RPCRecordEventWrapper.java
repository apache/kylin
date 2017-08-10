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

public class RPCRecordEventWrapper extends RecordEventWrapper {

    public RPCRecordEventWrapper(RecordEvent metricsEvent) {
        super(metricsEvent);
    }

    public void setWrapper(String projectName, String realizationName, String rpcServer, Throwable throwable) {
        this.metricsEvent.put(PropertyEnum.PROJECT.toString(), projectName);
        this.metricsEvent.put(PropertyEnum.REALIZATION.toString(), realizationName);
        this.metricsEvent.put(PropertyEnum.RPC_SERVER.toString(), rpcServer);
        this.metricsEvent.put(PropertyEnum.EXCEPTION.toString(),
                throwable == null ? "NULL" : throwable.getClass().getName());
    }

    public void setStats(long callTimeMs, long skipCount, long scanCount, long returnCount, long aggrCount) {
        this.metricsEvent.put(PropertyEnum.CALL_TIME.toString(), callTimeMs);
        this.metricsEvent.put(PropertyEnum.SKIP_COUNT.toString(), skipCount); //Number of skips on region servers based on region meta or fuzzy filter
        this.metricsEvent.put(PropertyEnum.SCAN_COUNT.toString(), scanCount); //Count scanned by region server
        this.metricsEvent.put(PropertyEnum.RETURN_COUNT.toString(), returnCount);//Count returned by region server
        this.metricsEvent.put(PropertyEnum.AGGR_FILTER_COUNT.toString(), scanCount - returnCount); //Count filtered & aggregated by coprocessor
        this.metricsEvent.put(PropertyEnum.AGGR_COUNT.toString(), aggrCount); //Count aggregated by coprocessor
    }

    public enum PropertyEnum {
        PROJECT("PROJECT"), REALIZATION("REALIZATION"), RPC_SERVER("RPC_SERVER"), EXCEPTION("EXCEPTION"), //
        CALL_TIME("CALL_TIME"), SKIP_COUNT("COUNT_SKIP"), SCAN_COUNT("COUNT_SCAN"), RETURN_COUNT(
                "COUNT_RETURN"), AGGR_FILTER_COUNT("COUNT_AGGREGATE_FILTER"), AGGR_COUNT("COUNT_AGGREGATE");

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