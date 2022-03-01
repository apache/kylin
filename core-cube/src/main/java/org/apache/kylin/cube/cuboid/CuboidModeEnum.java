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

package org.apache.kylin.cube.cuboid;

import java.util.Locale;

import org.apache.kylin.shaded.com.google.common.base.Strings;

public enum CuboidModeEnum {
    CURRENT("CURRENT"), CURRENT_WITH_BASE("CURRENT_WITH_BASE"), RECOMMEND("RECOMMEND"), RECOMMEND_EXISTING("RECOMMEND_EXISTING"), RECOMMEND_MISSING(
            "RECOMMEND_MISSING"), RECOMMEND_MISSING_WITH_BASE("RECOMMEND_MISSING_WITH_BASE");

    private final String modeName;

    CuboidModeEnum(String modeName) {
        this.modeName = modeName;
    }

    public String toString() {
        return modeName;
    }

    public static CuboidModeEnum getByModeName(String modeName) {
        if (Strings.isNullOrEmpty(modeName)) {
            return null;
        }
        for (CuboidModeEnum mode : CuboidModeEnum.values()) {
            if (mode.modeName.equals(modeName.toUpperCase(Locale.ROOT))) {
                return mode;
            }
        }
        return null;
    }
}
