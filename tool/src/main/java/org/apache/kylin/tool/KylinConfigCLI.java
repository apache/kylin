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

import java.util.Properties;

import org.apache.kylin.common.BackwardCompatibilityConfig;
import org.apache.kylin.common.KylinConfig;

public class KylinConfigCLI {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: KylinConfigCLI conf_name");
            System.err.println("Example: KylinConfigCLI kylin.server.mode");
            System.exit(1);
        }

        Properties config = KylinConfig.getKylinProperties();
        BackwardCompatibilityConfig bcc = new BackwardCompatibilityConfig();
        String value = config.getProperty(bcc.check(args[0]));
        if (value == null) {
            value = "";
        }
        System.out.println(value);
    }
}
