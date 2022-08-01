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

package org.apache.kylin.metadata.acl;

import java.util.Arrays;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE)
public class DependentColumn {

    @JsonProperty
    String column;

    @JsonProperty("dependent_column_identity")
    String dependentColumnIdentity;

    @JsonProperty("dependent_values")
    String[] dependentValues;

    public DependentColumn() {
    }

    public DependentColumn(String column, String dependentColumnIdentity, String[] dependentValues) {
        this.column = column;
        this.dependentColumnIdentity = dependentColumnIdentity;
        this.dependentValues = dependentValues;
    }

    public String getColumn() {
        return column;
    }

    public String getDependentColumnIdentity() {
        return dependentColumnIdentity;
    }

    public String[] getDependentValues() {
        return dependentValues;
    }

    public DependentColumn merge(DependentColumn other) {
        Preconditions.checkArgument(other != null);
        Preconditions.checkArgument(other.column.equalsIgnoreCase(this.column));
        Preconditions.checkArgument(other.dependentColumnIdentity.equalsIgnoreCase(this.dependentColumnIdentity));
        Set<String> values = Sets.newHashSet(dependentValues);
        values.addAll(Arrays.asList(other.dependentValues));
        return new DependentColumn(column, dependentColumnIdentity, values.toArray(new String[0]));
    }
}
