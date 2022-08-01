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
package org.apache.kylin.tool;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.OptionBuilder;
import org.apache.kylin.tool.util.ToolUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

public class AbstractInfoExtractorToolTest extends NLocalFileMetadataTestCase {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetOptions() throws ParseException {
        MockInfoExtractorTool mock = new MockInfoExtractorTool();
        OptionsHelper optionsHelper = new OptionsHelper();
        optionsHelper.parseOptions(mock.getOptions(), new String[] { "-destDir", "output", "-startTime", "2000" });
        Assert.assertEquals("output",
                mock.getStringOption(optionsHelper, MockInfoExtractorTool.OPTION_DEST, "destDir"));
        Assert.assertTrue(mock.getBooleanOption(optionsHelper, MockInfoExtractorTool.OPTION_COMPRESS, true));
        Assert.assertEquals(2000, mock.getLongOption(optionsHelper, MockInfoExtractorTool.OPTION_START_TIME, 1000L));

        Option OPTION_THREADS = OptionBuilder.getInstance().withArgName("threads").hasArg().isRequired(false)
                .withDescription("Specify number of threads for parallel extraction.").create("threads");
        Assert.assertEquals(4, mock.getLongOption(optionsHelper, OPTION_THREADS, 4));
    }

    @Test
    public void testAddFile() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        MockInfoExtractorTool mock = new MockInfoExtractorTool();
        mock.addFile(new File(ToolUtil.getKylinHome(), "kylin.properties"), mainDir);
        Assert.assertTrue(new File(mainDir, "kylin.properties").exists());
    }

    @Test
    public void testAddShellOutput() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        MockInfoExtractorTool mock = new MockInfoExtractorTool();
        mock.addShellOutput("echo \"hello world\"", mainDir, "hello");
        mock.addShellOutput("echo \"hello java\"", mainDir, "hello", true);

        Assert.assertTrue(new File(mainDir, "hello").exists());
        Assert.assertTrue(FileUtils.readFileToString(new File(mainDir, "hello")).startsWith("hello world"));
        Assert.assertTrue(FileUtils.readFileToString(new File(mainDir, "hello")).contains("hello java"));
    }

    @Test
    public void TestDumpLicenseInfo() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        MockInfoExtractorTool mock = new MockInfoExtractorTool();

        File license = new File(ToolUtil.getKylinHome(), "LICENSE");
        String licenseInfo = "Evaluation license for Kyligence Enterprise\n" + "Category: 4.x\n" + "SLA Service: NO\n"
                + "Volume: 1\n" + "Level: professional\n"
                + "Insight License: 5 users; evaluation; 2019-06-01,2019-07-30\n" + "====\n" + "Kyligence Enterprise\n"
                + "2019-06-01,2019-07-30\n"
                + "19d4801b6dardchr83bp3i7wadbdvycs8ay7ibicu2msfogl6kiwz7z3dmdizepmicl3bgqznn34794jt5g51sutofcfpn9jeiw5k3cvt2750faxw7ip1fp08mt3og6xijt4x02euf1zkrn5m7huwal8lqms3gmn0d5i8y2dqlvkvpqtwz3m9tqcnq6n4lznthbdtfncdqsly7a8v9pndh1cav2tdcczzs17ns6e0d4izeatwybr25lir5f5s6qe4ry10x2fkqco7unb4h4ivx8jo6vdb5sp3r4738zhlvrbdwfa38s3wh82lrnugrhxq8eap3rebq9dz8xka713aui4v2acquulicdadt63cv0biz7y7eccfh1tri60526b2bmon71k29n6p29tsbhyl2wdx5hsjuxg2wd993hcndot1fc5oz8kebopqrudyf4o7tjc5ca0bvtysnw3gn64c1sd2iw2rlhlxk7c5szp6kde8dvitteoqo1oufum5eyjbk1q2fegf9vpyng3bs6c6qfoibc2wvxgjn4hnismbsr4ovwe5gvam74ikdromn8dxv91e5wuvcqml92jgfoj4g0xzrns05hsqs55a5a9ao44f6m2eccscq4crfm5dxwdl7xbmmmj1yfgpygco4mvh9ksitsxoy30v6dgse76wmyemjymyaa2f6my83vu55z9vhywv6a4har3tep32dg3mvol1arsia8bllis4awfqjpw57lpv1fmt5n8ns8vqvle09cpehrlkt5kjcaucwb64c25q8zvikgtm2p0ywfnsapm97fxloymcqp0vgwmqzt3feaq8o6mzjaqmgap7r7gtn1k1awwxjs1sd91g4y1emab14hs";
        FileUtils.writeStringToFile(license, licenseInfo);

        mock.dumpLicenseInfo(mainDir);

        FileUtils.deleteQuietly(license);
        Assert.assertTrue(new File(mainDir, "info").exists());
        String info = FileUtils.readFileToString(new File(mainDir, "info"));
        Assert.assertTrue(info.contains("MetaStoreID:"));
        Assert.assertTrue(info.contains("Host:"));
    }

    @Test
    public void TestExtractCommitFile() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        overwriteSystemProp("KYLIN_HOME", mainDir.getAbsolutePath());
        MockInfoExtractorTool mock = new MockInfoExtractorTool();

        List<File> clearFileList = new ArrayList<>();
        File commitFile = new File(KylinConfig.getKylinHome(), "commit_SHA1");
        if (!commitFile.exists()) {
            String sha1 = "6a38664fe087f7f466ec4ad9ac9dc28415d99e52@KAP\nBuild with MANUAL at 2019-08-31 20:02:22";
            FileUtils.writeStringToFile(commitFile, sha1);
            clearFileList.add(commitFile);
        }

        File output = new File(mainDir, "output");
        FileUtils.forceMkdir(output);

        mock.extractCommitFile(output);

        for (File file : clearFileList) {
            FileUtils.deleteQuietly(file);
        }
        Assert.assertTrue(new File(output, "commit_SHA1").exists());
        Assert.assertTrue(FileUtils.readFileToString(new File(output, "commit_SHA1")).contains("6a38664fe087f7f4"));
    }

    @Test
    public void testExecute() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);

        MockInfoExtractorTool mock = new MockInfoExtractorTool();

        mock.execute(new String[] { "-destDir", mainDir.getAbsolutePath(), "-systemProp", "true" });

        for (File file1 : mainDir.listFiles()) {
            for (File file2 : file1.listFiles()) {
                if (!file2.getName().contains("_base_") || !file2.getName().endsWith(".zip")) {
                    Assert.fail();
                }
            }
        }
    }

    class MockInfoExtractorTool extends AbstractInfoExtractorTool {

        @Override
        protected void executeExtract(OptionsHelper optionsHelper, File exportDir) throws Exception {
            // Do nothing.
        }
    }

}
