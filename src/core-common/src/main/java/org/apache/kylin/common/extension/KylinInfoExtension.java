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

package org.apache.kylin.common.extension;

import org.apache.kylin.common.KylinConfig;

public class KylinInfoExtension {
    private static final ExtensionFactoryLoader<KylinInfoExtension.Factory> loader = new ExtensionFactoryLoader<>();
    private static KylinInfoExtension.Factory extensionFactory = null;

    private KylinInfoExtension() {
    }

    public static Factory getFactory() {
        if (extensionFactory == null) {
            synchronized (KylinInfoExtension.class) {
                extensionFactory = loader.loadFactory(KylinInfoExtension.Factory.class,
                        KylinConfig.getInstanceFromEnv().getKylinInfoExtensionFactory());
            }
        }
        return extensionFactory;
    }

    public static class Factory {

        public Factory() {
            // Do nothing
        }

        public boolean checkKylinInfo() {
            return true;
        }
    }
}
