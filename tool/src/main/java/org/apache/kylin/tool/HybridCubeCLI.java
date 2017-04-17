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

/**
 * 1. Create new HybridCube
 * bin/kylin.sh org.apache.kylin.tool.HybridCubeCLI -action create -name hybrid_name -project project_name -model model_name -cubes cube1,cube2
 * 2. Update existing HybridCube
 * bin/kylin.sh org.apache.kylin.tool.HybridCubeCLI -action update -name hybrid_name -project project_name -model model_name -cubes cube1,cube2,cube3
 * 3. Delete the HybridCube
 * bin/kylin.sh org.apache.kylin.tool.HybridCubeCLI -action delete -name hybrid_name -project project_name -model model_name
 */
public class HybridCubeCLI {

    public static void main(String[] args) {
        org.apache.kylin.rest.job.HybridCubeCLI cli = new org.apache.kylin.rest.job.HybridCubeCLI();
        cli.execute(args);
    }

}
