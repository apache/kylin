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

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.kylin.common.KylinConfigBase;

import com.google.common.base.Preconditions;

public class ResourceUtils {

    public static final String KYLIN_CONF_DIR = "/conf";

    public static URL getServerConfUrl(String fileName) throws IOException {
        URL resource = Thread.currentThread().getContextClassLoader().getResource(fileName);
        File file = new File(KylinConfigBase.getKylinHome() + KYLIN_CONF_DIR, fileName);

        if (file.exists()) {
            URL url = file.toURI().toURL();
            return Preconditions.checkNotNull(url);
        }
        return Preconditions.checkNotNull(resource);
    }

    private ResourceUtils() {
        throw new IllegalStateException("Utility class");
    }

}
