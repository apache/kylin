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
package org.apache.kylin.rest.request;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.rest.service.update.TableSchemaUpdateMapping;

public class TableUpdateRequest implements Serializable {
    private Map<String, TableSchemaUpdateMapping> mapping;
    private Set<String> cubeSetToAffect;
    private boolean isUseExisting;

    public Map<String, TableSchemaUpdateMapping> getMapping() {
        return mapping;
    }

    public void setMapping(Map<String, TableSchemaUpdateMapping> mapping) {
        this.mapping = mapping;
    }

    public Set<String> getCubeSetToAffect() {
        return cubeSetToAffect;
    }

    public void setCubeSetToAffect(Set<String> cubeSetToAffect) {
        this.cubeSetToAffect = cubeSetToAffect;
    }

    public boolean isUseExisting() {
        return isUseExisting;
    }

    public void setIsUseExisting(boolean useExisting) {
        isUseExisting = useExisting;
    }
}
