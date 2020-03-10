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

package org.apache.kylin.engine.spark2.utils;

import org.apache.kylin.engine.spark2.AccelerateInfo;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.util.QueryUtil;

import java.util.Collection;

public class RecAndQueryCompareUtil {

    private RecAndQueryCompareUtil() {
    }

    public static class CompareEntity {

        private String sql;
        private Collection<OLAPContext> olapContexts;
        private AccelerateInfo accelerateInfo;
        private String accelerateLayouts;
        private String queryUsedLayouts;
        private AccelerationMatchedLevel level;
        private String filePath;

        @Override
        public String toString() {
            return "CompareEntity{\n\tsql=[" + QueryUtil.removeCommentInSql(sql) + "],\n\taccelerateLayouts="
                    + accelerateLayouts + ",\n\tqueryUsedLayouts=" + queryUsedLayouts + "\n\tfilePath=" + filePath
                    + ",\n\tlevel=" + level + "\n}";
        }

        public boolean ignoredCompareLevel() {
            return getLevel() == AccelerationMatchedLevel.SNAPSHOT_QUERY
                    || getLevel() == AccelerationMatchedLevel.SIMPLE_QUERY
                    || getLevel() == AccelerationMatchedLevel.CONSTANT_QUERY;
        }

        public String getSql() {
            return this.sql;
        }

        public Collection<OLAPContext> getOlapContexts() {
            return this.olapContexts;
        }

        public AccelerateInfo getAccelerateInfo() {
            return this.accelerateInfo;
        }

        public String getAccelerateLayouts() {
            return this.accelerateLayouts;
        }

        public String getQueryUsedLayouts() {
            return this.queryUsedLayouts;
        }

        public AccelerationMatchedLevel getLevel() {
            return this.level;
        }

        public String getFilePath() {
            return this.filePath;
        }

        public void setSql(String sql) {
            this.sql = sql;
        }

        public void setOlapContexts(Collection<OLAPContext> olapContexts) {
            this.olapContexts = olapContexts;
        }

        public void setAccelerateInfo(AccelerateInfo accelerateInfo) {
            this.accelerateInfo = accelerateInfo;
        }

        public void setAccelerateLayouts(String accelerateLayouts) {
            this.accelerateLayouts = accelerateLayouts;
        }

        public void setQueryUsedLayouts(String queryUsedLayouts) {
            this.queryUsedLayouts = queryUsedLayouts;
        }

        public void setLevel(AccelerationMatchedLevel level) {
            this.level = level;
        }

        public void setFilePath(String filePath) {
            this.filePath = filePath;
        }
    }

    /**
     * Acceleration matched level
     */
    public enum AccelerationMatchedLevel {

        /**
         * simple query does not need realization
         */
        SIMPLE_QUERY,

        /**
         * constant query
         */
        CONSTANT_QUERY,

        /**
         * query blocked in stage of propose cuboids and layouts
         */
        BLOCKED_QUERY,

        /**
         * failed in matching realizations
         */
        FAILED_QUERY,

        /**
         * query used snapshot or partly used snapshot
         */
        SNAPSHOT_QUERY,

        /**
         * all matched
         */
        ALL_MATCH,

        /**
         * index matched, but layout not matched
         */
        LAYOUT_NOT_MATCH,

        /**
         * model matched, but index not matched
         */
        INDEX_NOT_MATCH,

        /**
         * model not matched
         */
        MODEL_NOT_MATCH
    }
}
