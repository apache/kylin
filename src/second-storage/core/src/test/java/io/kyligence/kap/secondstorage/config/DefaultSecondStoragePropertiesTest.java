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
package io.kyligence.kap.secondstorage.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

public class DefaultSecondStoragePropertiesTest {

    @Test
    public void testGetOptional() {
        Properties properties = new Properties();
        properties.put("k1", "v1");
        SecondStorageProperties secondStorageProperties = new DefaultSecondStorageProperties(properties);

        Assertions.assertEquals("v1", secondStorageProperties.get(new ConfigOption<>("k1", "", String.class)));

        Assertions.assertEquals("v2", secondStorageProperties.get(new ConfigOption<>("k2", "v2", String.class)));


        Exception exception = Assertions.assertThrows(NullPointerException.class, () -> secondStorageProperties.get(new ConfigOption<>(null, String.class)));
        Assertions.assertEquals(exception.getMessage(), "Key must not be null.");


        Exception exception2 = Assertions.assertThrows(IllegalArgumentException.class, () -> secondStorageProperties.get(new ConfigOption<>("k1", MyClass.class)));
        Assertions.assertEquals(exception2.getMessage(), "Could not parse value 'v1' for key 'k1'.");

    }

    // for ut
    public class MyClass {

    }
}
