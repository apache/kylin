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

package org.apache.kylin.rec.common;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.rec.exception.ProposerJobException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.val;

class AccelerateInfoTest {

    @Test
    void testTransformThrowable() {
        {
            // test null
            val throwable = AccelerateInfo.transformThrowable(null);
            Assertions.assertNull(throwable);
        }

        {
            // test msg length <= 4000
            String msg = RandomStringUtils.randomAlphanumeric(10);
            val e2 = new RuntimeException(msg);
            val e1 = new RuntimeException("msg", e2);
            val throwable = AccelerateInfo.transformThrowable(e1);
            Assertions.assertTrue(throwable instanceof RuntimeException);
            Assertions.assertEquals(msg, throwable.getMessage());
        }

        {
            // test msg length > 4000
            String msg = RandomStringUtils.randomAlphanumeric(4001);
            val e2 = new RuntimeException(msg);
            val e1 = new RuntimeException("msg", e2);
            val throwable = AccelerateInfo.transformThrowable(e1);
            Assertions.assertTrue(throwable instanceof ProposerJobException);
            Assertions.assertEquals(StringUtils.substring(msg, 0, 4000), throwable.getMessage());
            Assertions.assertEquals(4000, throwable.getMessage().length());
        }
    }
}
