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

public class Calculation {

    @JacksonXmlProperty(localName = "class", isAttribute = true)
    private String className;

    @JacksonXmlProperty(localName = "formula", isAttribute = true)
    private String formula;

    public void setClassName(String className) {
        this.className = className;
    }

    public String getFormula() {
        return formula;
    }

    public void setFormula(String formula) {
        this.formula = formula;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof Calculation))
            return false;
        Calculation that = (Calculation) o;
        return Objects.equals(getFormula(), that.getFormula());
    }

    @Override
    public int hashCode() {

        return Objects.hash(getFormula());
    }
}
