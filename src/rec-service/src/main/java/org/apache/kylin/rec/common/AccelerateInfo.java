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

package org.apache.kylin.rec.common;

import java.io.Serializable;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.base.Throwables;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.rec.exception.ProposerJobException;

import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@ToString
public class AccelerateInfo implements Serializable {

    @JsonIgnore
    private static final Integer MAX_EXCEPTION_MSG_LEN = 4000;

    private Set<QueryLayoutRelation> relatedLayouts = Sets.newHashSet();
    private Throwable failedCause;
    private String pendingMsg;

    public boolean isFailed() {
        return this.failedCause != null;
    }

    public boolean isPending() {
        return StringUtils.isNotBlank(pendingMsg);
    }

    public boolean isNotSucceed() {
        return isPending() || isFailed();
    }

    public static Throwable transformThrowable(Throwable throwable) {
        if (Objects.isNull(throwable)) {
            return null;
        }
        Throwable rootCause = Throwables.getRootCause(throwable);
        if (StringUtils.length(rootCause.getMessage()) <= MAX_EXCEPTION_MSG_LEN) {
            return rootCause;
        }
        String subMsg = StringUtils.substring(rootCause.getMessage(), 0, MAX_EXCEPTION_MSG_LEN);
        return new ProposerJobException(subMsg);
    }

    @Getter
    @ToString
    @AllArgsConstructor
    @NoArgsConstructor
    @EqualsAndHashCode
    public static class QueryLayoutRelation implements Serializable {

        @ToString.Exclude
        @JsonIgnore
        @Setter
        private String sql;
        @Setter
        private String modelId;
        @Setter
        private long layoutId;
        private int semanticVersion;

        public String getUniqueId() {
            return modelId + "@" + layoutId;
        }

        public boolean consistent(LayoutEntity layout) {

            Preconditions.checkNotNull(layout);
            return this.semanticVersion == layout.getModel().getSemanticVersion()
                    && this.modelId.equalsIgnoreCase(layout.getModel().getId()) && this.layoutId == layout.getId();
        }
    }
}
