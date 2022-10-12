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

package org.apache.kylin.metadata;

import java.io.IOException;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TempStatementManagerTest extends LocalFileMetadataTestCase {
    @BeforeEach
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @AfterEach
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    void testGetInstance() {
        Assertions.assertNotNull(TempStatementManager.getInstance(getTestConfig()));
    }

    @Test
    void testAddTempStatement() throws IOException {
        TempStatementManager manager = TempStatementManager.getInstance(getTestConfig());
        manager.updateTempStatement("temp_table3", "AAAAA");
        Assertions.assertEquals(3, manager.reloadAllTempStatement().size());
    }

    @Test
    void testRemoveTempStatement() throws IOException {
        TempStatementManager manager = TempStatementManager.getInstance(getTestConfig());
        manager.removeTempStatement("temp_table1");
        Assertions.assertEquals(1, manager.reloadAllTempStatement().size());
    }

    @Test
    void testUpdateTempStatement() throws IOException {
        TempStatementManager manager = TempStatementManager.getInstance(getTestConfig());
        manager.updateTempStatement("temp_table1", "AAAAA");
        Assertions.assertEquals(2, manager.reloadAllTempStatement().size());
        Assertions.assertEquals("AAAAA", manager.getTempStatement("temp_table1"));
    }
}
