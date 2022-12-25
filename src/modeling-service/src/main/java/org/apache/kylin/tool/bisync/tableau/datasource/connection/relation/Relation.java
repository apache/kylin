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
package org.apache.kylin.tool.bisync.tableau.datasource.connection.relation;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

public class Relation {

    @JacksonXmlProperty(localName = "join", isAttribute = true)
    private String join;

    @JacksonXmlProperty(localName = "type", isAttribute = true)
    private String type;

    @JacksonXmlProperty(localName = "connection", isAttribute = true)
    private String connection;

    @JacksonXmlProperty(localName = "name", isAttribute = true)
    private String name;

    @JacksonXmlProperty(localName = "table", isAttribute = true)
    private String table;

    @JacksonXmlProperty(localName = "clause")
    private Clause clause;

    @JacksonXmlProperty(localName = "relation")
    @JacksonXmlElementWrapper(useWrapping = false)
    private List<Relation> relationList;

    public String getJoin() {
        return join;
    }

    public void setJoin(String join) {
        this.join = join;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getConnection() {
        return connection;
    }

    public void setConnection(String connection) {
        this.connection = connection;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public Clause getClause() {
        return clause;
    }

    public void setClause(Clause clause) {
        this.clause = clause;
    }

    public List<Relation> getRelationList() {
        return relationList;
    }

    public void setRelationList(List<Relation> relationList) {
        this.relationList = relationList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof Relation))
            return false;
        Relation relation = (Relation) o;
        return Objects.equals(getJoin(), relation.getJoin()) && Objects.equals(getType(), relation.getType())
                && Objects.equals(getConnection(), relation.getConnection())
                && Objects.equals(getName(), relation.getName()) && Objects.equals(getTable(), relation.getTable())
                && Objects.equals(getClause(), relation.getClause())
                && relationChildListEquals(relation.getRelationList());
    }

    @Override
    public int hashCode() {

        return Objects.hash(getJoin(), getType(), getConnection(), getName(), getTable(), getClause(),
                getRelationList());
    }

    private boolean relationChildListEquals(List<Relation> thatChildList) {
        if (getRelationList() == thatChildList) {
            return true;
        }
        // join relation childs size must be 2
        if (getRelationList() != null && thatChildList != null && getRelationList().size() == 2
                && thatChildList.size() == 2) {
            return Objects.equals(getRelationList().get(0), thatChildList.get(0))
                    && Objects.equals(getRelationList().get(1), thatChildList.get(1));
        }
        return false;
    }
}
