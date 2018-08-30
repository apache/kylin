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

package org.apache.kylin.common.metrics.common;

import java.util.Locale;

public final class MetricsNameBuilder {
    public final static String METRICS = "metrics:";
    public final static String PROJECT_TEMPLATE = METRICS + "project=%s";
    public final static String CUBE_TEMPLATE = METRICS + "project=%s,cube=%s";

    public static String buildMetricName(String prefix, String name) {
        return String.format(Locale.ROOT, prefix + ",name=%s", name);
    }

    public static String buildCubeMetricPrefix(String project) {
        return String.format(Locale.ROOT, PROJECT_TEMPLATE, project);
    }

    public static String buildCubeMetricPrefix(String project, String cube) {
        return String.format(Locale.ROOT, CUBE_TEMPLATE, project, cube);
    }

}
