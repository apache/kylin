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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.springframework.test.util.ReflectionTestUtils;

import lombok.val;

public class ClickhouseDiagToolTest extends NLocalFileMetadataTestCase {

    private static final String logs1 = "2020.05.18 18:15:50.745336 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushing system log, 7 entries to flush\n"
            + "2020.05.18 18:15:50.760828 [ 12121 ] {} <Debug> DiskLocal: Reserving 1.00 MiB on disk `default`, having unreserved 258.53 MiB.\n"
            + "2021.05.18 15:15:50.923367 [ 12121 ] {} <Trace> system.metric_log (4ed24813-70a5-4891-a22a-5af9d7654f71): Renaming temporary part tmp_insert_202105_68538_68538_0 to 202105_87671_87671_0.\n"
            + "2021.05.18 15:15:50.924274 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushed system log\n"
            + "2021.05.18 18:15:58.424386 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushing system log, 8 entries to flush\n"
            + "2021.05.18 18:15:58.430833 [ 12121 ] {} <Debug> DiskLocal: Reserving 1.00 MiB on disk `default`, having unreserved 539.80 MiB.\n"
            + "2021.05.18 18:15:58.434465 [ 12121 ] {} <Trace> system.metric_log (4ed24813-70a5-4891-a22a-5af9d7654f71): Renaming temporary part tmp_insert_202105_68539_68539_0 to 202105_87672_87672_0.\n"
            + "2021.05.18 18:15:58.435039 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushed system log\n"
            + "2021.05.18 18:16:05.935125 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushing system log, 7 entries to flush\n"
            + "2021.05.18 18:16:05.940605 [ 12121 ] {} <Debug> DiskLocal: Reserving 1.00 MiB on disk `default`, having unreserved 642.16 MiB.\n"
            + "2021.05.18 18:16:05.944313 [ 12121 ] {} <Trace> system.metric_log (4ed24813-70a5-4891-a22a-5af9d7654f71): Renaming temporary part tmp_insert_202105_68540_68540_0 to 202105_87673_87673_0.\n"
            + "2021.05.18 18:16:05.945236 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushed system log\n"
            + "2021.05.18 18:16:13.445337 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushing system log, 8 entries to flush\n"
            + "2021.05.18 18:16:13.450534 [ 12121 ] {} <Debug> DiskLocal: Reserving 1.00 MiB on disk `default`, having unreserved 876.80 MiB.\n"
            + "2021.05.18 18:16:13.453724 [ 12121 ] {} <Trace> system.metric_log (4ed24813-70a5-4891-a22a-5af9d7654f71): Renaming temporary part tmp_insert_202105_68541_68541_0 to 202105_87674_87674_0.\n"
            + "2021.05.18 18:16:13.454482 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushed system log\n"
            + "2021.05.18 18:16:20.954598 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushing system log, 7 entries to flush\n"
            + "2021.05.18 18:16:20.960346 [ 12121 ] {} <Debug> DiskLocal: Reserving 1.00 MiB on disk `default`, having unreserved 1.09 GiB.\n"
            + "2021.05.18 18:16:20.963832 [ 12121 ] {} <Trace> system.metric_log (4ed24813-70a5-4891-a22a-5af9d7654f71): Renaming temporary part tmp_insert_202105_68542_68542_0 to 202105_87675_87675_0.\n"
            + "2021.05.18 18:16:20.964657 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushed system log\n"
            + "2022.05.18 18:16:20.975851 [ 12151 ] {} <Debug> system.metric_log (4ed24813-70a5-4891-a22a-5af9d7654f71) (MergerMutator): Selected 2 parts from 202105_82621_87669_963 to 202105_87670_87670_0\n"
            + "2022.05.18 19:16:20.975914 [ 12151 ] {} <Debug> DiskLocal: Reserving 1.89 MiB on disk `default`, having unreserved 1.09 GiB.\n"
            + "2022.05.18 19:16:20.989143 [ 8600 ] {} <Debug> system.metric_log (4ed24813-70a5-4891-a22a-5af9d7654f71) (MergerMutator): Merging 2 parts: from 202105_82621_87669_963 to 202105_87670_8";

    private static final String logs2 = "2021.05.18 18:15:58.424386 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushing system log, 8 entries to flush\n"
            + "2021.05.18 18:15:58.430833 [ 12121 ] {} <Debug> DiskLocal: Reserving 1.00 MiB on disk `default`, having unreserved 539.80 MiB.\n"
            + "2021.05.18 18:15:58.434465 [ 12121 ] {} <Trace> system.metric_log (4ed24813-70a5-4891-a22a-5af9d7654f71): Renaming temporary part tmp_insert_202105_68539_68539_0 to 202105_87672_87672_0.\n"
            + "2021.05.18 18:15:58.435039 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushed system log\n"
            + "2021.05.18 18:16:05.935125 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushing system log, 7 entries to flush\n"
            + "2021.05.18 18:16:05.940605 [ 12121 ] {} <Debug> DiskLocal: Reserving 1.00 MiB on disk `default`, having unreserved 642.16 MiB.\n"
            + "2021.05.18 18:16:05.944313 [ 12121 ] {} <Trace> system.metric_log (4ed24813-70a5-4891-a22a-5af9d7654f71): Renaming temporary part tmp_insert_202105_68540_68540_0 to 202105_87673_87673_0.\n"
            + "2021.05.18 18:16:05.945236 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushed system log\n"
            + "2021.05.18 18:16:13.445337 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushing system log, 8 entries to flush\n"
            + "2021.05.18 18:16:13.450534 [ 12121 ] {} <Debug> DiskLocal: Reserving 1.00 MiB on disk `default`, having unreserved 876.80 MiB.\n"
            + "2021.05.18 18:16:13.453724 [ 12121 ] {} <Trace> system.metric_log (4ed24813-70a5-4891-a22a-5af9d7654f71): Renaming temporary part tmp_insert_202105_68541_68541_0 to 202105_87674_87674_0.\n"
            + "2021.05.18 18:16:13.454482 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushed system log\n"
            + "2021.05.18 18:16:20.954598 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushing system log, 7 entries to flush\n"
            + "2021.05.18 18:16:20.960346 [ 12121 ] {} <Debug> DiskLocal: Reserving 1.00 MiB on disk `default`, having unreserved 1.09 GiB.\n"
            + "2021.05.18 18:16:20.963832 [ 12121 ] {} <Trace> system.metric_log (4ed24813-70a5-4891-a22a-5af9d7654f71): Renaming temporary part tmp_insert_202105_68542_68542_0 to 202105_87675_87675_0.\n"
            + "2021.05.18 18:16:20.964657 [ 12121 ] {} <Trace> SystemLog (system.metric_log): Flushed system log\n"
            + "2022.05.18 18:16:20.975851 [ 12151 ] {} <Debug> system.metric_log (4ed24813-70a5-4891-a22a-5af9d7654f71) (MergerMutator): Selected 2 parts from 202105_82621_87669_963 to 202105_87670_87670_0\n"
            + "2022.05.18 19:16:20.975914 [ 12151 ] {} <Debug> DiskLocal: Reserving 1.89 MiB on disk `default`, having unreserved 1.09 GiB.\n"
            + "2022.05.18 19:16:20.989143 [ 8600 ] {} <Debug> system.metric_log (4ed24813-70a5-4891-a22a-5af9d7654f71) (MergerMutator): Merging 2 parts: from 202105_82621_87669_963 to 202105_87670_8\n";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public TestName testName = new TestName();

    @Test
    @Ignore
    public void testExtractCkLog() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());

        File subDir = new File(mainDir, ClickhouseDiagTool.SUB_DIR);

        File tmpDir = new File(subDir, "tmp");
        File targetDir = new File(subDir, "targetDir");

        FileUtils.forceMkdir(mainDir);
        FileUtils.forceMkdir(subDir);
        FileUtils.forceMkdir(tmpDir);
        FileUtils.forceMkdir(targetDir);

        File ckLog = new File(tmpDir, "click-server.log");

        val dataFormat = ReflectionTestUtils.getField(ClickhouseDiagTool.class, "SECOND_DATE_FORMAT").toString();

        //2021.05.18 18:15:00 - 2021.05.18 19:15:00
        val timeRange = new Pair<>(new DateTime(1621332900000L).toString(dataFormat),
                new DateTime(1621336500000L).toString(dataFormat));

        FileUtils.writeStringToFile(ckLog, logs1);
        Assert.assertTrue(ckLog.setLastModified(1621336500000L));

        val ckLogCount = Files.lines(Paths.get(ckLog.getAbsolutePath())).count();

        ClickhouseDiagTool clickhouseDiagTool = new ClickhouseDiagTool();

        ReflectionTestUtils.invokeMethod(clickhouseDiagTool, "extractCkLogByRange", timeRange, targetDir, tmpDir);

        File extractedLogFile = new File(targetDir, "click-server.log");

        val count = Files.lines(Paths.get(extractedLogFile.getAbsolutePath())).count();
        Assert.assertEquals(ckLogCount - count, 4);

        val extractedLogFileStr = FileUtils.readFileToString(extractedLogFile);
        Assert.assertEquals(extractedLogFileStr, logs2);

    }

    @Test
    public void testGenerateCompressedFile() {
        ClickhouseDiagTool clickhouseDiagTool = new ClickhouseDiagTool();

        {
            String serverLog = ReflectionTestUtils.invokeMethod(clickhouseDiagTool, "getCompressedFileMatcher",
                    "*server-log.log", 2);
            Assert.assertEquals(serverLog, "{*server-log.log.0.gz,*server-log.log.1.gz}");
        }

        {
            String serverLog = ReflectionTestUtils.invokeMethod(clickhouseDiagTool, "getCompressedFileMatcher",
                    "*server-log.log", 1);
            Assert.assertEquals(serverLog, "{*server-log.log.0.gz}");
        }

    }

    @Test
    public void testCleanEmptyFile() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        FileUtils.forceMkdir(mainDir);
        FileUtils.touch(new File(mainDir, "click-server.log"));
        val fileFilter = FileFilterUtils.suffixFileFilter("log");

        val fileList = FileUtils.listFiles(mainDir, fileFilter, null);

        Assert.assertEquals(1, fileList.size());
        fileList.forEach(file -> {
            Assert.assertEquals(0, FileUtils.sizeOf(file));
        });

        ClickhouseDiagTool clickhouseDiagTool = new ClickhouseDiagTool();

        ReflectionTestUtils.invokeMethod(clickhouseDiagTool, "cleanEmptyFile", mainDir);

        Assert.assertEquals(0, FileUtils.listFiles(mainDir, fileFilter, null).size());

    }

    @Test
    public void testUnzipLogFile() throws IOException {
        File mainDir = new File(temporaryFolder.getRoot(), testName.getMethodName());
        val fileFilter = FileFilterUtils.suffixFileFilter("log");
        File tarTempDir = new File(mainDir, "tarTemp");
        FileUtils.forceMkdir(mainDir);
        FileUtils.forceMkdir(tarTempDir);
        val logFile = new File(mainDir, "click-server.log");
        FileUtils.touch(logFile);
        FileUtils.writeStringToFile(logFile, "abc");

        writeFileToTar(new File(tarTempDir, "t.gz"), logFile);

        ClickhouseDiagTool clickhouseDiagTool = new ClickhouseDiagTool();
        ReflectionTestUtils.invokeMethod(clickhouseDiagTool, "unzipLogFile", tarTempDir);

        Assert.assertEquals(1, FileUtils.listFiles(tarTempDir, fileFilter, null).size());

    }

    private void writeFileToTar(File tarTempFile, File logFile) throws IOException {
        GZIPOutputStream out = new GZIPOutputStream(new FileOutputStream(tarTempFile));
        InputStream in = new FileInputStream(logFile);

        byte[] buf = new byte[1024];
        int len;
        while ((len = in.read(buf)) > 0) {
            out.write(buf, 0, len);
        }
        in.close();

        out.finish();
        out.close();
    }
}
