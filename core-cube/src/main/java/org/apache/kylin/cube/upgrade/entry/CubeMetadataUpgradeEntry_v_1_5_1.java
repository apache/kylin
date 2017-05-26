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

package org.apache.kylin.cube.upgrade.entry;

import org.apache.kylin.cube.upgrade.V1_5_1.CubeMetadataUpgrade_v_1_5_1;
import org.apache.kylin.cube.upgrade.v1_4_0.CubeMetadataUpgrade_v_1_4_0;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CubeMetadataUpgradeEntry_v_1_5_1 {
    private static final Logger logger = LoggerFactory.getLogger(CubeMetadataUpgradeEntry_v_1_5_1.class);

    public static void main(String[] args) {
        if (!(args != null && (args.length == 1))) {
            System.out.println("Usage: java CubeMetadataUpgradeEntry_v_1_5_1 <metadata_export_folder>");
            System.out.println(", where metadata_export_folder is the folder containing your current metadata's dump (Upgrade program will not modify it directly, relax.");
            return;
        }

        try {
            CubeMetadataUpgrade_v_1_4_0.upgradeOrVerify(CubeMetadataUpgrade_v_1_4_0.class, args, true, false);
            CubeMetadataUpgrade_v_1_5_1.upgradeOrVerify(CubeMetadataUpgrade_v_1_5_1.class, new String[] { args[0] + "_workspace" }, false, true);
        } catch (Exception e) {
            logger.error("something went wrong when upgrading, don't override your metadata store with this workspace folder yet!", e);
            return;
        }

        logger.info("The metadata upgrade is complete locally. You need to upload the metadata to you actual metadata store to verify locally. You need to upload the metadata to you actual metadata store to verify.");
    }
}
