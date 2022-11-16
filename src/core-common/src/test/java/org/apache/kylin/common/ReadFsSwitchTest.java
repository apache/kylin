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

package org.apache.kylin.common;

import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.junit.annotation.OverwriteProp;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

@MetadataInfo
public class ReadFsSwitchTest {

    @OverwriteProp.OverwriteProps({ //
            @OverwriteProp(key = "kylin.storage.columnar.file-system-backup-reset-sec", value = "1"), //
            @OverwriteProp(key = "kylin.storage.columnar.file-system", value = "a://"), //
            @OverwriteProp(key = "kylin.storage.columnar.file-system-backup", value = "b://")//
    })
    @Test
    public void test() throws InterruptedException {
        for (int i = 0; i < 15; i++) {
            if (i == 2)
                Assert.assertTrue(ReadFsSwitch.turnOnSwitcherIfBackupFsAllowed(ex(),
                        KapConfig.getInstanceFromEnv().getSwitchBackupFsExceptionAllowString()));
            if (i == 3)
                Assert.assertFalse(ReadFsSwitch.turnOnSwitcherIfBackupFsAllowed(ex(),
                        KapConfig.getInstanceFromEnv().getSwitchBackupFsExceptionAllowString()));

            String fs = KapConfig.getInstanceFromEnv().getParquetReadFileSystem();
            if (i == 1)
                Assert.assertEquals("a://", fs);
            if (i >= 2 && i < 12)
                Assert.assertEquals("b://", fs);
            if (i > 12)
                Assert.assertEquals("a://", fs);

            Thread.sleep(100);
        }
    }

    private RuntimeException ex() {
        return new RuntimeException(
                "java.lang.RuntimeException: java.io.IOException: alluxio.exception.AlluxioException: Expected to lock inode amzU8cb_kylin_newten_instance but locked inode");
    }

}
