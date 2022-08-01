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

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.ClassUtil;

public class ExtensionFactoryLoader<E> {

    /**
     * Use reflection to create instance of `extensionFactoryFullName`,
     * and set it to Singleton `extensionFactoryObj` .
     */
    public E loadFactory(Class<E> extensionFactoryClazz, String extensionFactoryFullName) {
        try {
            return ClassUtil.forName(extensionFactoryFullName, extensionFactoryClazz).newInstance();
        } catch (Exception e) {
            throw new KylinException(INVALID_PARAMETER, "Extensions class can't be loaded normally", e);
        }
    }
}
