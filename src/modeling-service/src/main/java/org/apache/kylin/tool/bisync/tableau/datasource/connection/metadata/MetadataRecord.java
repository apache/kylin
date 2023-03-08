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
package org.apache.kylin.tool.bisync.tableau.datasource.connection.metadata;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

public class MetadataRecord {

    @JacksonXmlProperty(localName = "class", isAttribute = true)
    private String clazs;

    @JacksonXmlProperty(localName = "remote-name")
    private String remoteName;

    @JacksonXmlProperty(localName = "remote-type")
    private String remoteType;

    @JacksonXmlProperty(localName = "local-name")
    private String localName;

    @JacksonXmlProperty(localName = "parent-name")
    private String parentName;

    @JacksonXmlProperty(localName = "remote-alias")
    private String remoteAlias;

    @JacksonXmlProperty(localName = "ordinal")
    private String ordinal;

    @JacksonXmlProperty(localName = "width")
    private String width;

    @JacksonXmlProperty(localName = "collation")
    private Collation collation;

    @JacksonXmlProperty(localName = "local-type")
    private String localType;

    @JacksonXmlProperty(localName = "padded-semantics")
    private String paddedSemantics;

    @JacksonXmlProperty(localName = "precision")
    private String precision;

    @JacksonXmlProperty(localName = "scale")
    private String scale;

    @JacksonXmlProperty(localName = "aggregation")
    private String aggregation;

    @JacksonXmlProperty(localName = "contains-null")
    private String containsNull;

    @JacksonXmlProperty(localName = "attributes")
    private AttributeList attributeList;

}
