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

package org.apache.kylin.common.notify.util;

import java.util.Map;

public class DingTalkTemplateProvider {

    private static DingTalkTemplateProvider DEFAULT_INSTANCE = new DingTalkTemplateProvider();

    public static DingTalkTemplateProvider getInstance() {
        return DEFAULT_INSTANCE;
    }

    public String buildDingTalkContent(String state, String title, Map<String, Object> data) {
        String titleStart = "<font color=%s size=3>";
        String titleEnd = "</font>";
        StringBuilder sb = new StringBuilder(String.format(titleStart, titleColor(state)))
                .append(title)
                .append(titleEnd)
                .append("  \n");
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            sb.append(" **")
                    .append(entry.getKey())
                    .append(" :** ")
                    .append(entry.getValue())
                    .append("  \n");
        }

        return sb.toString();
    }

    private String titleColor(String state) {
        switch (state) {
            case Notify.SUCCEED:
            case Notify.MIGRATION_COMPLETED:
                return "#5cb85c";
            case Notify.DISCARDED:
                return "#607D8B";
            case Notify.ERROR:
            case Notify.METADATA_PERSIST_FAIL:
            case Notify.MIGRATION_FAILED:
            case Notify.MIGRATION_REJECTED:
                return "#d9534f";
            case Notify.MIGRATION_APPROVED:
            case Notify.MIGRATION_REQUEST:
                return "#337ab7";
            default:
                return "#000000";
        }
    }

}
