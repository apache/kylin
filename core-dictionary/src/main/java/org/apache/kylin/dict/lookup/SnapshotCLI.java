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

package org.apache.kylin.dict.lookup;

import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.source.SourceFactory;

public class SnapshotCLI {

    public static void main(String[] args) throws IOException {
        if ("rebuild".equals(args[0]))
            rebuild(args[1], args[2]);
    }

    private static void rebuild(String table, String overwriteUUID) throws IOException {
        KylinConfig conf = KylinConfig.getInstanceFromEnv();
        MetadataManager metaMgr = MetadataManager.getInstance(conf);
        SnapshotManager snapshotMgr = SnapshotManager.getInstance(conf);

        TableDesc tableDesc = metaMgr.getTableDesc(table);
        if (tableDesc == null)
            throw new IllegalArgumentException("Not table found by " + table);

        SnapshotTable snapshot = snapshotMgr.rebuildSnapshot(SourceFactory.createReadableTable(tableDesc), tableDesc, overwriteUUID);
        System.out.println("resource path updated: " + snapshot.getResourcePath());
    }
}
