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

import java.util.List;

public class DeriveInfo implements java.io.Serializable {
    public DeriveType type;
    public JoinDesc join;
    public List<Integer> columns;
    public boolean isOneToOne; // only used when ref from derived to host

    public DeriveInfo(DeriveType type, JoinDesc join, List<Integer> columns, boolean isOneToOne) {
        this.type = type;
        this.join = join;
        this.columns = columns;
        this.isOneToOne = isOneToOne;
    }

    @Override
    public String toString() {
        return "DeriveInfo [type=" + type + ", join=" + join + ", columns=" + columns + ", isOneToOne=" + isOneToOne
                + "]";
    }

    public enum DeriveType implements java.io.Serializable {
        LOOKUP, LOOKUP_NON_EQUI, PK_FK, EXTENDED_COLUMN
    }
}
