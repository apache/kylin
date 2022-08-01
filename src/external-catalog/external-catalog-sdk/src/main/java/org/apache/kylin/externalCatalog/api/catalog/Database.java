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
package org.apache.kylin.externalCatalog.api.catalog;

import org.apache.kylin.externalCatalog.api.annotation.InterfaceStability.Evolving;

import java.util.Map;
import java.util.Objects;

@Evolving
public class Database {

    private String name;
    private String description;
    private String locationUri;
    private Map<String, String> parameters; // properties associated with the database

    public Database(
            String name,
            String description,
            String locationUri,
            Map<String, String> parameters) {
        this.name = name;
        this.description = description;
        this.locationUri = locationUri;
        this.parameters = parameters;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getLocationUri() {
        return locationUri;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Database database = (Database) o;
        return Objects.equals(name, database.name)
                && Objects.equals(description, database.description)
                && Objects.equals(locationUri, database.locationUri)
                && Objects.equals(parameters, database.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, description, locationUri, parameters);
    }

    @Override
    public String toString() {
        return "Database{"
                + "name='" + name + '\''
                + ", description='" + description + '\''
                + ", locationUri='" + locationUri + '\''
                + ", parameters=" + parameters
                + '}';
    }


}
