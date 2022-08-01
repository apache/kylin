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
package org.apache.kylin.tool.bisync.tableau.mapping;

import java.util.List;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

@JacksonXmlRootElement(localName = "MAPPINGS")
public class Mappings {

    @JacksonXmlProperty(localName = "TYPE_MAPPING")
    @JacksonXmlElementWrapper(useWrapping = false)
    private List<TypeMapping> typeMappings;

    @JacksonXmlProperty(localName = "FUNC_MAPPING")
    @JacksonXmlElementWrapper(useWrapping = false)
    private List<FunctionMapping> funcMappings;

    public List<TypeMapping> getTypeMappings() {
        return typeMappings;
    }

    public void setTypeMappings(List<TypeMapping> typeMappings) {
        this.typeMappings = typeMappings;
    }

    public List<FunctionMapping> getFuncMappings() {
        return funcMappings;
    }

    public void setFuncMappings(List<FunctionMapping> funcMappings) {
        this.funcMappings = funcMappings;
    }
}
