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

package org.apache.kylin.engine.spark2;

import org.apache.kylin.shaded.com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;

import java.util.Set;

public class AccelerateInfo {

    private Set<QueryLayoutRelation> relatedLayouts = Sets.newHashSet();
    private Throwable failedCause;
    private String pendingMsg;

    public AccelerateInfo() {
    }

    public boolean isFailed() {
        return this.failedCause != null;
    }

    public boolean isPending() {
        return StringUtils.isNotBlank(pendingMsg);
    }

    public boolean isNotSucceed() {
        return isPending() || isFailed();
    }

    public Set<QueryLayoutRelation> getRelatedLayouts() {
        return this.relatedLayouts;
    }

    public Throwable getFailedCause() {
        return this.failedCause;
    }

    public String getPendingMsg() {
        return this.pendingMsg;
    }

    public void setRelatedLayouts(Set<QueryLayoutRelation> relatedLayouts) {
        this.relatedLayouts = relatedLayouts;
    }

    public void setFailedCause(Throwable failedCause) {
        this.failedCause = failedCause;
    }

    public void setPendingMsg(String pendingMsg) {
        this.pendingMsg = pendingMsg;
    }

    public String toString() {
        return "AccelerateInfo(relatedLayouts=" + this.getRelatedLayouts() + ", failedCause=" + this.getFailedCause() + ", pendingMsg=" + this.getPendingMsg() + ")";
    }

    public static class QueryLayoutRelation {

        private String sql;
        private String modelId;
        private long layoutId;
        private int semanticVersion;

        public QueryLayoutRelation(String sql, String modelId, long layoutId, int semanticVersion) {
            this.sql = sql;
            this.modelId = modelId;
            this.layoutId = layoutId;
            this.semanticVersion = semanticVersion;
        }

        public String getSql() {
            return this.sql;
        }

        public String getModelId() {
            return this.modelId;
        }

        public long getLayoutId() {
            return this.layoutId;
        }

        public int getSemanticVersion() {
            return this.semanticVersion;
        }

        public boolean equals(final Object o) {
            if (o == this) return true;
            if (!(o instanceof QueryLayoutRelation)) return false;
            final QueryLayoutRelation other = (QueryLayoutRelation) o;
            if (!other.canEqual((Object) this)) return false;
            final Object thissql = this.getSql();
            final Object othersql = other.getSql();
            if (thissql == null ? othersql != null : !thissql.equals(othersql)) return false;
            final Object thismodelId = this.getModelId();
            final Object othermodelId = other.getModelId();
            if (thismodelId == null ? othermodelId != null : !thismodelId.equals(othermodelId)) return false;
            if (this.getLayoutId() != other.getLayoutId()) return false;
            if (this.getSemanticVersion() != other.getSemanticVersion()) return false;
            return true;
        }

        protected boolean canEqual(final Object other) {
            return other instanceof QueryLayoutRelation;
        }

        public int hashCode() {
            final int PRIME = 59;
            int result = 1;
            final Object sql = this.getSql();
            result = result * PRIME + (sql == null ? 43 : sql.hashCode());
            final Object modelId = this.getModelId();
            result = result * PRIME + (modelId == null ? 43 : modelId.hashCode());
            final long layoutId = this.getLayoutId();
            result = result * PRIME + (int) (layoutId >>> 32 ^ layoutId);
            result = result * PRIME + this.getSemanticVersion();
            return result;
        }

        public String toString() {
            return "AccelerateInfo.QueryLayoutRelation(modelId=" + this.getModelId() + ", layoutId=" + this.getLayoutId() + ", semanticVersion=" + this.getSemanticVersion() + ")";
        }

        public void setModelId(String modelId) {
            this.modelId = modelId;
        }

        public void setLayoutId(long layoutId) {
            this.layoutId = layoutId;
        }
    }
}
