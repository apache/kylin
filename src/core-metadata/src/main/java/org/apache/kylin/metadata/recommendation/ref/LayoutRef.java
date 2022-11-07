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

package org.apache.kylin.metadata.recommendation.ref;

import org.apache.kylin.metadata.cube.model.LayoutEntity;

import com.google.common.base.Preconditions;

import lombok.AccessLevel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@NoArgsConstructor
public class LayoutRef extends RecommendationRef {

    @Setter(AccessLevel.PRIVATE)
    private boolean agg;

    public LayoutRef(LayoutEntity layout, int id, boolean agg) {
        this.setId(id);
        this.setEntity(layout);
        this.setAgg(agg);
    }

    @Override
    public String getContent() {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }

    public LayoutEntity getLayout() {
        Preconditions.checkArgument(getEntity() instanceof LayoutEntity);
        return (LayoutEntity) getEntity();
    }

    @Override
    public String getDataType() {
        throw new IllegalStateException("There is no datatype of LayoutRef");
    }

    @Override
    public void rebuild(String newName) {
        throw new IllegalStateException("Rebuild layoutRef is not allowed.");
    }
}
