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

import java.util.Map;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.guava30.shaded.common.collect.Maps;

import com.fasterxml.jackson.core.JsonProcessingException;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class DimensionRef extends RecommendationRef {

    public DimensionRef(int id) {
        this.setId(id);
    }

    public DimensionRef(RecommendationRef columnRef, int id, String dataType, boolean existed) {
        this.setId(id);
        this.setName(columnRef.getName());
        this.setContent(buildContent(columnRef.getName(), dataType));
        this.setDataType(dataType);
        this.setExisted(existed);
        this.setEntity(columnRef.getEntity());
        this.setExcluded(columnRef.isExcluded());
    }

    public void init() {
        if (getDependencies().isEmpty()) {
            return;
        }
        RecommendationRef dependRef = getDependencies().get(0);
        this.setName(dependRef.getName());
        this.setDataType(dependRef.getDataType());
        this.setContent(buildContent(getName(), getDataType()));
        this.setExisted(false);
        this.setEntity(dependRef);
    }

    private String buildContent(String columnName, String dataType) {
        Map<String, String> map = Maps.newHashMap();
        map.put("column", columnName);
        map.put("data_type", dataType);
        String content;
        try {
            content = JsonUtil.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
        return content;
    }

    @Override
    public void rebuild(String newName) {
        // do nothing at present
    }
}
