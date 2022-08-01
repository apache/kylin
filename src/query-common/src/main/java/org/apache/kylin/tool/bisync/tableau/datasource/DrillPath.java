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
package org.apache.kylin.tool.bisync.tableau.datasource;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

public class DrillPath {

    @JacksonXmlProperty(localName = "name", isAttribute = true)
    private String name;

    @JacksonXmlProperty(localName = "field")
    @JacksonXmlElementWrapper(useWrapping = false)
    private List<String> fields;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DrillPath)) {
            return false;
        }
        DrillPath drillPath = (DrillPath) o;
        return Objects.equals(getName(), drillPath.getName()) && Objects.equals(getFields(), drillPath.getFields());
    }

    @Override
    public int hashCode() {

        return Objects.hash(getName(), getFields());
    }

    private boolean fieldsEquals(List<String> thatFields) {
        if (getFields() == thatFields) {
            return true;
        }
        if (getFields() != null && thatFields != null && getFields().size() == thatFields.size()) {
            boolean flag = true;
            for (int i = 0; i < getFields().size() && flag; i++) {
                flag = Objects.equals(getFields().get(i), thatFields.get(i));
            }
            return flag;
        }
        return false;
    }
}
