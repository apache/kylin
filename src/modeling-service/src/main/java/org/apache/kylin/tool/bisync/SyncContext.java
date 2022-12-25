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

package org.apache.kylin.tool.bisync;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.cube.model.NDataflow;

import lombok.Data;

@Data
public class SyncContext {

    public enum BI {
        TABLEAU_ODBC_TDS,
        TABLEAU_CONNECTOR_TDS
    }

    public enum ModelElement {
        AGG_INDEX_COL,
        AGG_INDEX_AND_TABLE_INDEX_COL,
        ALL_COLS,
        CUSTOM_COLS
    }

    private String projectName;

    private String modelId;

    private String host;

    private int port;

    private BI targetBI;

    private ModelElement modelElement = ModelElement.AGG_INDEX_COL;

    private NDataflow dataflow;

    private KylinConfig kylinConfig;

    private boolean isAdmin;
}
