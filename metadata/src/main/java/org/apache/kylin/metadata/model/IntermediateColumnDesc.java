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

import org.apache.kylin.metadata.model.TblColRef;

/**
 * Created by Hongbin Ma(Binmahone) on 12/30/14.
 */
public class IntermediateColumnDesc {
    private String id;
    private TblColRef colRef;

    public IntermediateColumnDesc(String id, TblColRef colRef) {
        this.id = id;
        this.colRef = colRef;
    }

    public String getId() {
        return id;
    }

    public TblColRef getColRef() {
        return this.colRef;
    }

    public String getColumnName() {
        return colRef.getName();
    }

    public String getDataType() {
        return colRef.getDatatype();
    }

    public String getTableName() {
        return colRef.getTable();
    }

    public boolean isSameAs(String tableName, String columnName) {
        return colRef.isSameAs(tableName, columnName);
    }

    public String getCanonicalName() {
        return colRef.getCanonicalName();
    }

}
