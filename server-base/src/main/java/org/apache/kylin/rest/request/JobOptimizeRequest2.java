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

package org.apache.kylin.rest.request;

import java.util.Set;
import java.util.stream.Collectors;

public class JobOptimizeRequest2 {
    private Set<String> cuboidsAdd;
    private Set<String> cuboidsDelete;

    public Set<Long> getCuboidsAdd() {
        if (cuboidsAdd == null)
            return null;
        return cuboidsAdd.stream().map(s -> Long.parseLong(s)).collect(Collectors.toSet());
    }

    public Set<Long> getCuboidsDelete() {
        if (cuboidsDelete == null)
            return null;
        return cuboidsDelete.stream().map(s -> Long.parseLong(s)).collect(Collectors.toSet());
    }

}
