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
package org.apache.kylin.sdk.datasource.framework.def;

import java.util.Locale;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

public class FunctionDef {
    @JacksonXmlProperty(localName = "ID", isAttribute = true)
    private String id;

    @JacksonXmlProperty(localName = "EXPRESSION", isAttribute = true)
    private String expression;

    public String getId() {
        return id;
    }

    void setId(String id) {
        this.id = id;
    }

    public String getExpression() {
        return expression;
    }

    void init() {
        id = id.toUpperCase(Locale.ROOT);
    }

    FunctionDef() {
    }
}
