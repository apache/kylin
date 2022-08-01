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

import org.junit.Assert;
import org.junit.Test;

public class ErrorCodeTest {

    @Test
    public void testNonKeException() {
        ErrorCodeCommon nonKeException = ErrorCodeCommon.NON_KE_EXCEPTION;
        Assert.assertEquals("KE-060100201", nonKeException.getErrorCode().getCode());
        Assert.assertEquals("Please check whether the external environment(other systems, components, etc.) is normal.",
                nonKeException.getErrorSuggest().getLocalizedString());
        Assert.assertEquals("An Exception occurred outside Kyligence Enterprise.", nonKeException.getMsg());
        Assert.assertEquals("An Exception occurred outside Kyligence Enterprise.",
                nonKeException.getErrorMsg().getLocalizedString());
        Assert.assertEquals("Please check whether the external environment(other systems, components, etc.) is normal.",
                nonKeException.getErrorSuggestion().getLocalizedString());
        Assert.assertEquals("KE-060100201: An Exception occurred outside Kyligence Enterprise.",
                nonKeException.getCodeMsg());
    }
}
