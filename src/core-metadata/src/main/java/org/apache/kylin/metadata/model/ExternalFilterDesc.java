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

package org.apache.kylin.metadata.model;

import java.util.Locale;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.filter.function.Functions;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * External filter enables user to register dynamic external filters out the scope of cubes.
 * External filters are maintained logically in a filter store (which may or may not share same physical store with cubes),
 * and are accessed by each cube shard at runtime.
 *
 * Currently the way to use external filter is 1. register external filter through REST 2. use UDF to specify conditions on external filter
 */
@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class ExternalFilterDesc extends RootPersistentEntity implements ISourceAware {

    @JsonProperty("name")
    private String name;
    @JsonProperty("filter_resource_identifier")
    private String filterResourceIdentifier;
    @JsonProperty("filter_table_type")
    private Functions.FilterTableType filterTableType;
    @JsonProperty("source_type")
    private int sourceType = ISourceAware.ID_EXTERNAL;
    @JsonProperty("description")
    private String description;

    @Override
    public String getResourcePath() {
        return concatResourcePath(getName());
    }

    public static String concatResourcePath(String name) {
        return ResourceStore.EXTERNAL_FILTER_RESOURCE_ROOT + "/" + name + ".json";
    }

    // ============================================================================

    @Override
    public String resourceName() {
        return name;
    }

    public String getFilterResourceIdentifier() {
        return filterResourceIdentifier;
    }

    public void setFilterResourceIdentifier(String filterResourceIdentifier) {
        this.filterResourceIdentifier = filterResourceIdentifier;
    }

    public Functions.FilterTableType getFilterTableType() {
        return filterTableType;
    }

    public void setFilterTableType(Functions.FilterTableType filterTableType) {
        this.filterTableType = filterTableType;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void init() {
        if (name != null)
            name = name.toUpperCase(Locale.ROOT);
    }

    @Override
    public String toString() {
        return "ExternalFilterDesc [ name=" + name + " filter table resource identifier "
                + this.filterResourceIdentifier + "]";
    }

    /** create a mockup table for unit test */
    public static ExternalFilterDesc mockup(String tableName) {
        ExternalFilterDesc mockup = new ExternalFilterDesc();
        mockup.setName(tableName);
        return mockup;
    }

    @Override
    public int getSourceType() {
        return sourceType;
    }

    @Override
    public KylinConfig getConfig() {
        return null;
    }

    public void setSourceType(int sourceType) {
        this.sourceType = sourceType;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
