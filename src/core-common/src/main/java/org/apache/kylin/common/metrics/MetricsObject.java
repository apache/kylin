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

package org.apache.kylin.common.metrics;

import java.util.Map;

public class MetricsObject {

    private boolean initStatus;
    private String fieldName;
    private String category;
    private String entity;
    private Map<String, String> tags;

    public MetricsObject(String fieldName, String category, String entity, Map<String, String> tags) {
        this.fieldName = fieldName;
        this.category = category;
        this.entity = entity;
        this.tags = tags;
        this.initStatus = false;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(fieldName);
        builder.append(",");
        builder.append(category);
        builder.append(",");
        builder.append(entity);
        builder.append(",");
        builder.append(tags == null ? "" : tags.toString());
        return builder.toString();
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getCategory() {
        return category;
    }

    public String getEntity() {
        return entity;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public boolean isInitStatus() {
        return initStatus;
    }

    public void setInitStatus(boolean initStatus) {
        this.initStatus = initStatus;
    }
}
