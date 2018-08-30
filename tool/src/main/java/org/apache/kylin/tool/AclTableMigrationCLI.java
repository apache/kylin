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

package org.apache.kylin.tool;

import java.util.Locale;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.service.AclTableMigrationTool;

public class AclTableMigrationCLI {

    private static final String MIGRATE = "MIGRATE";

    private static final String CHECK = "CHECK";

    public static void main(String[] args) throws Exception {
        if (args == null || args.length != 1) {
            throw new IllegalArgumentException("Args num error");
        }
        String cmd = args[0].toUpperCase(Locale.ROOT);
        AclTableMigrationTool tool = new AclTableMigrationTool();
        switch (cmd) {
        case MIGRATE:
            tool.migrate(KylinConfig.getInstanceFromEnv());
            break;
        case CHECK:
            boolean needMigrate = tool.checkIfNeedMigrate(KylinConfig.getInstanceFromEnv());
            if (needMigrate) {
                System.out.println(
                        "Found ACL metadata in legacy format. Please execute command : ${KYLIN_HOME}/bin/kylin.sh org.apache.kylin.tool.AclTableMigrationCLI MIGRATE");
                System.exit(2);
            }
            break;
        default:
            throw new IllegalArgumentException("Unrecognized cmd");
        }
    }
}
