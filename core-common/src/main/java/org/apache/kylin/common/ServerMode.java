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

package org.apache.kylin.common;

public enum ServerMode {

    ALL("all"), JOB("job"), QUERY("query");

    private final String name;

    ServerMode(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    private static void validate(KylinConfig config) {
        assert config != null;
    }

    private static boolean match(ServerMode serverMode, KylinConfig config) {
        validate(config);
        return serverMode.getName().equals(config.getServerMode());
    }

    public static boolean isJob(KylinConfig config) {
        return isJobOnly(config) || isAll(config);
    }

    public static boolean isJob(String serverMode) {
        return ALL.name.equals(serverMode) || JOB.name.equals(serverMode);
    }

    public static boolean isJobOnly(KylinConfig config) {
        return match(JOB, config);
    }

    public static boolean isQueryOnly(KylinConfig config) {
        return match(QUERY, config);
    }

    public static boolean isQuery(KylinConfig config) {
        return isQueryOnly(config) || isAll(config);
    }

    public static boolean isAll(KylinConfig config) {
        return match(ALL, config);
    }

    public static boolean isQuery(String serverMode) {
        return ALL.name.equals(serverMode) || QUERY.name.equals(serverMode);
    }
}
