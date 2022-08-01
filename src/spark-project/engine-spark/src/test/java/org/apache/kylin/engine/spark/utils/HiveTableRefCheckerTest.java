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

package org.apache.kylin.engine.spark.utils;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.engine.spark.stats.utils.HiveTableRefChecker;
import org.junit.Assert;
import org.junit.Test;

public class HiveTableRefCheckerTest extends NLocalFileMetadataTestCase {

    @Test
    public void testIsNeedCleanUpTransactionalTableJob() {
        Assert.assertTrue(
                HiveTableRefChecker.isNeedCleanUpTransactionalTableJob(Boolean.TRUE, Boolean.FALSE, Boolean.TRUE));
        Assert.assertFalse(
                HiveTableRefChecker.isNeedCleanUpTransactionalTableJob(Boolean.TRUE, Boolean.FALSE, Boolean.FALSE));
        Assert.assertTrue(
                HiveTableRefChecker.isNeedCleanUpTransactionalTableJob(Boolean.FALSE, Boolean.TRUE, Boolean.FALSE));
        Assert.assertTrue(
                HiveTableRefChecker.isNeedCleanUpTransactionalTableJob(Boolean.TRUE, Boolean.TRUE, Boolean.FALSE));
        Assert.assertFalse(
                HiveTableRefChecker.isNeedCleanUpTransactionalTableJob(Boolean.FALSE, Boolean.FALSE, Boolean.TRUE));
    }

    @Test
    public void testIsNeedCreateHiveTemporaryTable() {
        Assert.assertTrue(HiveTableRefChecker.isNeedCreateHiveTemporaryTable(Boolean.TRUE, Boolean.TRUE, Boolean.TRUE));
        Assert.assertFalse(
                HiveTableRefChecker.isNeedCreateHiveTemporaryTable(Boolean.FALSE, Boolean.TRUE, Boolean.FALSE));
        Assert.assertTrue(
                HiveTableRefChecker.isNeedCreateHiveTemporaryTable(Boolean.TRUE, Boolean.FALSE, Boolean.FALSE));
        Assert.assertTrue(
                HiveTableRefChecker.isNeedCreateHiveTemporaryTable(Boolean.FALSE, Boolean.TRUE, Boolean.TRUE));
        Assert.assertFalse(
                HiveTableRefChecker.isNeedCreateHiveTemporaryTable(Boolean.FALSE, Boolean.FALSE, Boolean.TRUE));
        Assert.assertFalse(
                HiveTableRefChecker.isNeedCreateHiveTemporaryTable(Boolean.FALSE, Boolean.FALSE, Boolean.FALSE));
    }

}
