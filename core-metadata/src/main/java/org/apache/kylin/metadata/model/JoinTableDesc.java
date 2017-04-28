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

package org.apache.kylin.metadata.model;

import java.io.Serializable;

import org.apache.kylin.metadata.model.DataModelDesc.TableKind;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class JoinTableDesc implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("table")
    private String table;

    @JsonProperty("kind")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private TableKind kind = TableKind.LOOKUP;
    
    @JsonProperty("alias")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String alias;
    
    @JsonProperty("join")
    private JoinDesc join;
    
    private TableRef tableRef;

    public String getTable() {
        return table;
    }

    void setTable(String table) {
        this.table = table;
    }

    public TableKind getKind() {
        return kind;
    }
    
    public String getAlias() {
        return alias;
    }
    
    public JoinDesc getJoin() {
        return join;
    }

    public TableRef getTableRef() {
        return tableRef;
    }

    void setTableRef(TableRef ref) {
        this.tableRef = ref;
    }
    
    

}
