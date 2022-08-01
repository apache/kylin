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
package org.apache.kylin.sdk.datasource.framework.conv;

import org.junit.Assert;
import org.junit.Test;

public class ParamNodeParserTest {
    @Test
    public void testParseParamIdx() {
        Assert.assertEquals(-1, ParamNodeParser.parseParamIdx("f**k"));
        Assert.assertEquals(-1, ParamNodeParser.parseParamIdx(null));
        Assert.assertEquals(-1, ParamNodeParser.parseParamIdx(""));
        Assert.assertEquals(-1, ParamNodeParser.parseParamIdx("1"));
        Assert.assertEquals(-1, ParamNodeParser.parseParamIdx("$$0"));
        Assert.assertEquals(-1, ParamNodeParser.parseParamIdx("$0$"));
        Assert.assertEquals(-1, ParamNodeParser.parseParamIdx("$10000000000000000000000000"));

        Assert.assertEquals(1, ParamNodeParser.parseParamIdx("$1"));
        Assert.assertEquals(0, ParamNodeParser.parseParamIdx("$0"));
        Assert.assertEquals(1, ParamNodeParser.parseParamIdx("$01"));
    }
}
