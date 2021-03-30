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
package org.apache.kylin.source.jdbc.extensible;

import java.io.IOException;

import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.source.IReadableTable;
import org.apache.kylin.source.ISampleDataDeployer;
import org.apache.kylin.source.ISource;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceManager;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class JdbcSourceTest extends TestBase {
    @Rule
    public ExpectedException expectedCannotAdaptEx = ExpectedException.none();

    @Test
    public void testBasics() throws IOException {
        ISource source = SourceManager.getSource(new JdbcSourceAware());
        ISourceMetadataExplorer explorer = source.getSourceMetadataExplorer();
        ISampleDataDeployer deployer = source.getSampleDataDeployer();

        Assert.assertTrue(source instanceof JdbcSource);
        Assert.assertTrue(explorer instanceof JdbcExplorer);
        Assert.assertTrue(deployer instanceof JdbcExplorer);

        IMRInput input = source.adaptToBuildEngine(IMRInput.class);
        Assert.assertNotNull(input);

        Class adaptTo = Object.class;
        expectedCannotAdaptEx.expect(RuntimeException.class);
        expectedCannotAdaptEx.expectMessage("Cannot adapt to " + adaptTo);
        source.adaptToBuildEngine(adaptTo);

        TableMetadataManager tblManager = TableMetadataManager.getInstance(getTestConfig());
        IReadableTable table = source.createReadableTable(tblManager.getTableDesc("test_kylin_fact", "default"), null);
        Assert.assertTrue(table instanceof JdbcTable);

        source.close();
    }
}
