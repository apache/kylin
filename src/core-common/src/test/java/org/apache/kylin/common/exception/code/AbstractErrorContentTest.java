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

package org.apache.kylin.common.exception.code;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class AbstractErrorContentTest {

    @Test
    public void testErrorMsg() {
        ErrorMsg errorMsg = new ErrorMsg("KE-030001201");

        assertEquals("KE-030001201", errorMsg.getCodeString());

        ErrorMsg.setMsg("en");
        assertEquals(
                "Can't build, the expected number of rows for index \"%s\" does not match the actual built number of rows.",
                errorMsg.getLocalizedString());

        //        ErrorMsg.setMsg("cn");
        //        assertEquals("无法构建，索引“%s” 的预期行数与实际构建行数不匹配。", errorMsg.getLocalizedString());
    }

    @Test
    public void testErrorSuggestion() {
        ErrorSuggestion errorSuggestion = new ErrorSuggestion("KE-030001201");

        assertEquals("KE-030001201", errorSuggestion.getCodeString());

        ErrorSuggestion.setMsg("en");
        assertEquals("", errorSuggestion.getLocalizedString());

        //        ErrorSuggestion.setMsg("cn");
        //        assertEquals("", errorSuggestion.getLocalizedString());
    }
}
