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

package io.kyligence.kap.metadata.recommendation.ref;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;

import com.google.common.collect.Lists;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter(AccessLevel.PROTECTED)
@EqualsAndHashCode
@NoArgsConstructor
@ToString
public abstract class RecommendationRef {

    // id >= 0 column in model
    // id < 0 column in rawRecItem
    private int id;
    private String name;
    private String content;
    @ToString.Exclude
    private String dataType;
    private boolean isBroken;
    private boolean existed;
    private boolean crossModel;
    private boolean isExcluded;
    private Object entity;
    private List<RecommendationRef> dependencies = Lists.newArrayList();

    public abstract void rebuild(String newName);

    protected <T extends RecommendationRef> List<RecommendationRef> validate(List<T> refs) {
        if (CollectionUtils.isEmpty(refs)) {
            return Lists.newArrayList();
        }
        return refs.stream().filter(ref -> !ref.isBroken() && !ref.isExisted()).collect(Collectors.toList());
    }

    // When a ref is not deleted and does not exist in model.
    public boolean isEffective() {
        return !this.isBroken() && !this.isExisted() && this.getId() < 0;
    }

    // When a ref derives from origin model or is effective.
    public boolean isLegal() {
        return !this.isBroken() && (this.getId() >= 0 || !this.isExisted());
    }
}
