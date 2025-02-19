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

package org.apache.kylin.rest.source;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.celeborn.shaded.com.google.common.collect.Sets;
import org.apache.kylin.sdk.datasource.security.JdbcSourceValidationSettings;
import org.junit.Test;

public class CommonJdbcSourceConnectionValidatorTest {

    @Test
    public void testValidate() {
        JdbcSourceValidationSettings settings = JdbcSourceValidationSettings.builder()
                .validUrlParamKeys(Sets.newHashSet("q1", "q2", "q3"))
                .build();

        {
            CommonJdbcSourceConnectionValidator validator = new CommonJdbcSourceConnectionValidator();
            validator.settings(settings).url("jdbc:mysql://localhost:123/data_db?q1=v1&q2=v2&q3=v3");
            assertTrue(validator.isValid());
        }
        {
            CommonJdbcSourceConnectionValidator validator = new CommonJdbcSourceConnectionValidator();
            validator.settings(settings).url("jdbc:mysql://localhost:123/data_db?q1=v1&q2=v2&q4=v4");
            assertFalse(validator.isValid());
        }
    }

    @Test
    public void testValidateFailed() {
        JdbcSourceValidationSettings settings = JdbcSourceValidationSettings.builder()
                .validUrlParamKeys(Sets.newHashSet("q1", "q2", "q3"))
                .build();

        {
            CommonJdbcSourceConnectionValidator validator = new CommonJdbcSourceConnectionValidator();
            validator.settings(settings).url("");
            assertFalse(validator.isValid());
        }
        {
            CommonJdbcSourceConnectionValidator validator = new CommonJdbcSourceConnectionValidator();
            validator.settings(settings).url("mysql://localhost:123/data_db");
            assertFalse(validator.isValid());
        }
    }
}
