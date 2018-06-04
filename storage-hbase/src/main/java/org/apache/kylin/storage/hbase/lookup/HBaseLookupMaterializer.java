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

package org.apache.kylin.storage.hbase.lookup;

import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.SnapshotTableDesc;
import org.apache.kylin.engine.mr.ILookupMaterializer;
import org.apache.kylin.engine.mr.LookupMaterializeContext;

public class HBaseLookupMaterializer implements ILookupMaterializer{

    @Override
    public void materializeLookupTable(LookupMaterializeContext context, CubeInstance cube, String lookupTableName) {
        HBaseLookupMRSteps lookupMRSteps = new HBaseLookupMRSteps(cube);
        SnapshotTableDesc snapshotTableDesc = cube.getDescriptor().getSnapshotTableDesc(lookupTableName);
        lookupMRSteps.addMaterializeLookupTableSteps(context, lookupTableName, snapshotTableDesc);
    }

    @Override
    public void materializeLookupTablesForCube(LookupMaterializeContext context, CubeInstance cube) {
        HBaseLookupMRSteps lookupMRSteps = new HBaseLookupMRSteps(cube);
        lookupMRSteps.addMaterializeLookupTablesSteps(context);
    }
}
