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
package org.apache.spark.dict;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NGlobalDictStoreFactory {
    private NGlobalDictStoreFactory() {
    }

    public static NGlobalDictStore getResourceStore(String baseDir) {
        try {
            Class<? extends NGlobalDictStore> clz = ClassUtil.forName(getGlobalDictStoreImpl(), NGlobalDictStore.class);
            log.trace("Use global dict store impl {}", clz.getCanonicalName());
            return clz.getConstructor(String.class).newInstance(baseDir);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to create global dict store", e);
        }
    }

    private static String getGlobalDictStoreImpl() {
        try {
            return KylinConfig.getInstanceFromEnv().getGlobalDictV2StoreImpl();
        } catch (Exception e) {
            return System.getProperty("kylin.engine.global-dict.store.impl",
                    "org.apache.spark.dict.NGlobalDictHDFSStore");
        }
    }
}
