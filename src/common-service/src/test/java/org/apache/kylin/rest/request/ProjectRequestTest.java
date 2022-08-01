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

package org.apache.kylin.rest.request;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.validation.FieldError;

public class ProjectRequestTest {
    @Test
    public void testErrorMessage() {
        FieldError fieldError = new FieldError("test", "nameValid", "test");
        Assert.assertEquals(
                "Please use number, letter, and underline to name your project, and start with a number or a letter.",
                new ProjectRequest().getErrorMessage(Arrays.asList(fieldError)));
        fieldError = new FieldError("test", "test", "test");
        String msg = new ProjectRequest().getErrorMessage(Arrays.asList(fieldError));
        Assert.assertEquals("", msg);
    }
}
