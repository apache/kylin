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

import java.util.List;
import java.util.Set;

import org.apache.kylin.tool.bisync.model.SyncModel;
import org.apache.kylin.tool.bisync.tableau.TableauDataSourceConverter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

public class BISyncTool {

    private BISyncTool() {
    }

    @VisibleForTesting
    public static BISyncModel dumpToBISyncModel(SyncContext syncContext) {
        SyncModel syncModel = new SyncModelBuilder(syncContext).buildSourceSyncModel(ImmutableList.of(),
                ImmutableList.of());
        return getBISyncModel(syncContext, syncModel);
    }

    public static BISyncModel getBISyncModel(SyncContext syncContext, SyncModel syncModel) {
        switch (syncContext.getTargetBI()) {
        case TABLEAU_ODBC_TDS:
        case TABLEAU_CONNECTOR_TDS:
            return new TableauDataSourceConverter().convert(syncModel, syncContext);
        default:
            throw new IllegalArgumentException();
        }
    }

    @VisibleForTesting
    public static BISyncModel dumpHasPermissionToBISyncModel(SyncContext syncContext, Set<String> authTables,
            Set<String> authColumns, List<String> dimensions, List<String> measures) {
        SyncModel syncModel = new SyncModelBuilder(syncContext).buildHasPermissionSourceSyncModel(authTables,
                authColumns, dimensions, measures);
        return getBISyncModel(syncContext, syncModel);
    }

    @VisibleForTesting
    public static BISyncModel dumpBISyncModel(SyncContext syncContext, List<String> dimensions, List<String> measures) {
        SyncModel syncModel = new SyncModelBuilder(syncContext).buildSourceSyncModel(dimensions, measures);
        return getBISyncModel(syncContext, syncModel);
    }
}
