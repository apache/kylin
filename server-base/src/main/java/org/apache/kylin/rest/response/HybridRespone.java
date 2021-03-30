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

package org.apache.kylin.rest.response;

import org.apache.kylin.storage.hybrid.HybridInstance;

public class HybridRespone {

    public static final String NO_PROJECT = "NO_PROJECT";
    public static final String NO_MODEL = "NO_MODEL";

    private String projectName;
    private String modelName;
    private HybridInstance hybridInstance;

    public HybridRespone(String projectName, String modelName, HybridInstance hybridInstance) {
        this.projectName = projectName;
        this.modelName = modelName;
        this.hybridInstance = hybridInstance;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String modelName) {
        this.modelName = modelName;
    }

    public HybridInstance getHybridInstance() {
        return hybridInstance;
    }

    public void setHybridInstance(HybridInstance hybridInstance) {
        this.hybridInstance = hybridInstance;
    }
}
