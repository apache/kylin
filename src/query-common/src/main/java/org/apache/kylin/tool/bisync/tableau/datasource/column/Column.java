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
package org.apache.kylin.tool.bisync.tableau.datasource.column;

import java.util.Objects;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

public class Column {

    @JacksonXmlProperty(localName = "caption", isAttribute = true)
    private String caption;

    @JacksonXmlProperty(localName = "datatype", isAttribute = true)
    private String datatype;

    @JacksonXmlProperty(localName = "name", isAttribute = true)
    private String name;

    @JacksonXmlProperty(localName = "role", isAttribute = true)
    private String role;

    @JacksonXmlProperty(localName = "type", isAttribute = true)
    private String type;

    @JacksonXmlProperty(localName = "hidden", isAttribute = true)
    private String hidden;

    @JacksonXmlProperty(localName = "ke_cube_used", isAttribute = true)
    private String keCubeUsed;

    @JacksonXmlProperty(localName = "aggregation", isAttribute = true)
    private String aggregation;

    @JacksonXmlProperty(localName = "semantic-role", isAttribute = true)
    private String semanticRole;

    @JacksonXmlProperty(localName = "auto-column", isAttribute = true, namespace = "user")
    private String autoColumn;

    @JacksonXmlProperty(localName = "calculation")
    private Calculation calculation;

    public String getCaption() {
        return caption;
    }

    public void setCaption(String caption) {
        this.caption = caption;
    }

    public String getDatatype() {
        return datatype;
    }

    public void setDatatype(String datatype) {
        this.datatype = datatype;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Calculation getCalculation() {
        return calculation;
    }

    public void setCalculation(Calculation calculation) {
        this.calculation = calculation;
    }

    public String getHidden() {
        return hidden;
    }

    public void setHidden(String hidden) {
        this.hidden = hidden;
    }

    public String getKeCubeUsed() {
        return keCubeUsed;
    }

    public void setKeCubeUsed(String keCubeUsed) {
        this.keCubeUsed = keCubeUsed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof Column))
            return false;
        Column column = (Column) o;
        return Objects.equals(getCaption(), column.getCaption()) && Objects.equals(getDatatype(), column.getDatatype())
                && Objects.equals(getName(), column.getName()) && Objects.equals(getRole(), column.getRole())
                && Objects.equals(getType(), column.getType()) && Objects.equals(getHidden(), column.getHidden())
                && Objects.equals(getKeCubeUsed(), column.getKeCubeUsed())
                && Objects.equals(getCalculation(), column.getCalculation());
    }

    @Override
    public int hashCode() {

        return Objects.hash(getCaption(), getDatatype(), getName(), getRole(), getType(), getHidden(),
                getCalculation());
    }
}
