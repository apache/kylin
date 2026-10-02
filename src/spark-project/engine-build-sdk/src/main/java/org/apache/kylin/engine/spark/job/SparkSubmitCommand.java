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

package org.apache.kylin.engine.spark.job;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SparkSubmitCommand {

    private final List<String> arguments;
    private final Map<String, String> environment;

    public SparkSubmitCommand(List<String> arguments, Map<String, String> environment) {
        this.arguments = Collections.unmodifiableList(new ArrayList<>(arguments));
        this.environment = Collections.unmodifiableMap(new LinkedHashMap<>(environment));
    }

    public List<String> getArguments() {
        return arguments;
    }

    public Map<String, String> getEnvironment() {
        return environment;
    }
}

