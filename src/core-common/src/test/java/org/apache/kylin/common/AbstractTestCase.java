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

package org.apache.kylin.common;

import static org.apache.kylin.common.util.Unsafe.restoreAllSystemProp;

import java.util.Map;

import org.apache.kylin.common.util.Unsafe;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractTestCase {

    private static final Map<String, String> BEFORE_CLASS_PROPERTY_MAP = Maps.newHashMap();
    private final Map<String, String> METHOD_PROPERTY_MAP = Maps.newHashMap();

    /** overwrite system property with annotation {@link BeforeClass}. */
    public static void overwriteSystemPropBeforeClass(String property, String name) {
        Unsafe.overwriteSystemProp(BEFORE_CLASS_PROPERTY_MAP, property, name);
    }

    /** Don't overwrite this method in sub-class. */
    @AfterClass
    public static void restoreSystemPropsOverwriteBeforeClass() {
        restoreAllSystemProp(BEFORE_CLASS_PROPERTY_MAP);
    }

    /** overwrite system property with annotation {@link org.junit.Before} and {@link org.junit.Test}*/
    public final void overwriteSystemProp(String property, String value) {
        Unsafe.overwriteSystemProp(METHOD_PROPERTY_MAP, property, value);
    }

    /** Don't overwrite this method in sub-class. */
    @After
    public final void restoreSystemProps() {
        restoreAllSystemProp(METHOD_PROPERTY_MAP);
    }

    /** Clear system property in test method with annotation {@link org.junit.Test} */
    public final void restoreSystemProp(String property) {
        if (!METHOD_PROPERTY_MAP.containsKey(property) || METHOD_PROPERTY_MAP.get(property) == null) {
            System.clearProperty(property);
        } else {
            System.setProperty(property, METHOD_PROPERTY_MAP.get(property));
        }
        METHOD_PROPERTY_MAP.remove(property);
    }
}
