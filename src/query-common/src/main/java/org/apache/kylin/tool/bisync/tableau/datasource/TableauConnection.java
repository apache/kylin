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

import java.util.Objects;

import org.apache.kylin.tool.bisync.tableau.datasource.connection.Cols;
import org.apache.kylin.tool.bisync.tableau.datasource.connection.NamedConnectionList;
import org.apache.kylin.tool.bisync.tableau.datasource.connection.metadata.MetadataRecordList;
import org.apache.kylin.tool.bisync.tableau.datasource.connection.relation.Relation;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

public class TableauConnection {

    @JacksonXmlProperty(localName = "class", isAttribute = true)
    private String className;

    @JacksonXmlProperty(localName = "named-connections")
    private NamedConnectionList namedConnectionList;

    @JacksonXmlProperty(localName = "relation")
    private Relation relation;

    @JacksonXmlProperty(localName = "cols")
    private Cols cols;

    @JacksonXmlProperty(localName = "metadata-records")
    private MetadataRecordList metadataRecords;

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public NamedConnectionList getNamedConnectionList() {
        return namedConnectionList;
    }

    public void setNamedConnectionList(NamedConnectionList namedConnectionList) {
        this.namedConnectionList = namedConnectionList;
    }

    public Relation getRelation() {
        return relation;
    }

    public void setRelation(Relation relation) {
        this.relation = relation;
    }

    public Cols getCols() {
        return cols;
    }

    public void setCols(Cols cols) {
        this.cols = cols;
    }

    public MetadataRecordList getMetadataRecords() {
        return metadataRecords;
    }

    public void setMetadataRecords(MetadataRecordList metadataRecords) {
        this.metadataRecords = metadataRecords;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TableauConnection)) {
            return false;
        }
        TableauConnection that = (TableauConnection) o;
        return Objects.equals(getNamedConnectionList(), that.getNamedConnectionList())
                && Objects.equals(getRelation(), that.getRelation()) && Objects.equals(getCols(), that.getCols());
    }

    @Override
    public int hashCode() {

        return Objects.hash(getNamedConnectionList(), getRelation(), getCols());
    }
}
