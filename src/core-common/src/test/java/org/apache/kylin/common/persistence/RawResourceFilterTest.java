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
package org.apache.kylin.common.persistence;

import static org.apache.kylin.common.persistence.RawResourceFilter.Operator.EQUAL_CASE_INSENSITIVE;
import static org.apache.kylin.common.persistence.RawResourceFilter.Operator.LIKE_CASE_INSENSITIVE;

import java.util.Arrays;
import java.util.Collections;

import org.apache.kylin.common.persistence.resources.ProjectRawResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RawResourceFilterTest {

    @Test
    void testBasicFilter() {
        ProjectRawResource project = new ProjectRawResource();
        project.setName("p1");
        project.setMetaKey("p1");
        project.setUuid("uuid_p1");
        project.setMvcc(0L);
        project.setProject("_global");
        project.setTs(System.currentTimeMillis());

        RawResourceFilter filter = new RawResourceFilter();
        filter.addConditions("uuid", Arrays.asList("uuid_p1", "uuid_p2"), RawResourceFilter.Operator.IN);
        filter.addConditions("mvcc", Collections.singletonList(4L), RawResourceFilter.Operator.LT);
        filter.addConditions("mvcc", Collections.singletonList(0L), RawResourceFilter.Operator.LE);
        filter.addConditions("ts", Collections.singletonList(100L), RawResourceFilter.Operator.GT);
        filter.addConditions("name", Collections.singletonList("p1"), RawResourceFilter.Operator.EQUAL);
        filter.addConditions("metaKey", Collections.singletonList("p1"), EQUAL_CASE_INSENSITIVE);
        filter.addConditions("project", Collections.singletonList("ob"), LIKE_CASE_INSENSITIVE);

        Assertions.assertTrue(filter.isMatch(project));

        filter.addConditions("mvcc", Collections.singletonList(1L), RawResourceFilter.Operator.GE);
        Assertions.assertFalse(filter.isMatch(project));
    }

    @Test
    void testReturnFalseWhenValueIsNull() {
        ProjectRawResource project = new ProjectRawResource();
        Assertions.assertFalse(RawResourceFilter.equalFilter("reservedFiled1", null).isMatch(project));
        Assertions.assertFalse(RawResourceFilter.equalFilter("reservedFiled2", null).isMatch(project));
        Assertions.assertFalse(RawResourceFilter.equalFilter("reservedFiled3", null).isMatch(project));
    }

    @Test
    void testUnsupportedFiled() {
        ProjectRawResource project = new ProjectRawResource();
        project.setId(1L);
        byte[] bytes = "abc".getBytes();
        project.setContent(bytes);
        RawResourceFilter filter = new RawResourceFilter();
        filter.addConditions("id", Collections.singletonList("1"), RawResourceFilter.Operator.EQUAL);
        filter.addConditions("content", Collections.singletonList(bytes), RawResourceFilter.Operator.EQUAL);
        Assertions.assertThrows(IllegalArgumentException.class, () -> filter.isMatch(project));
    }

    @Test
    void testUnknownFiled() {
        ProjectRawResource project = new ProjectRawResource();
        RawResourceFilter filter = new RawResourceFilter();
        filter.addConditions("undefinedKey", Collections.singletonList("1"), RawResourceFilter.Operator.EQUAL);
        Assertions.assertThrows(IllegalArgumentException.class, () -> filter.isMatch(project));
    }

    @Test
    void testNotValidValue() {
        ProjectRawResource project = new ProjectRawResource();
        project.setMvcc(10L);

        RawResourceFilter filter = RawResourceFilter.simpleFilter(EQUAL_CASE_INSENSITIVE, "mvcc", 1L);
        Assertions.assertThrows(AssertionError.class, () -> filter.isMatch(project));

        RawResourceFilter filter2 = RawResourceFilter.simpleFilter(LIKE_CASE_INSENSITIVE, "mvcc", 1L);
        Assertions.assertThrows(AssertionError.class, () -> filter2.isMatch(project));
    }
}
