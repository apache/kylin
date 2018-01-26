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

package org.apache.kylin.common.util;

import com.google.common.base.Strings;

public enum EmailTemplateEnum {
    JOB_DISCARD("JOB_DISCARD"), JOB_ERROR("JOB_ERROR"), JOB_SUCCEED("JOB_SUCCEED"), //
    METADATA_PERSIST_FAIL("METADATA_PERSIST_FAIL");

    private final String templateName;

    EmailTemplateEnum(String name) {
        this.templateName = name;
    }

    public static EmailTemplateEnum getByName(String name) {
        if (Strings.isNullOrEmpty(name)) {
            return null;
        }
        for (EmailTemplateEnum value : EmailTemplateEnum.values()) {
            if (value.templateName.equalsIgnoreCase(name)) {
                return value;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return templateName;
    }
}
