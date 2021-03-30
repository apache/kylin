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

package org.apache.kylin.engine.mr.steps;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.ByteBufferBackedInputStream;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.cli.DictionaryGeneratorCLI;
import org.apache.kylin.dict.DictionaryProvider;
import org.apache.kylin.dict.DistinctColumnValuesProvider;
import org.apache.kylin.engine.mr.SortedColumnDFSFile;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.IReadableTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateDictionaryJob extends AbstractHadoopJob {

    private static final Logger logger = LoggerFactory.getLogger(CreateDictionaryJob.class);

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_SEGMENT_ID);
        options.addOption(OPTION_INPUT_PATH);
        options.addOption(OPTION_DICT_PATH);
        options.addOption(OPTION_CUBING_JOB_ID);
        parseOptions(options, args);

        final String cubeName = getOptionValue(OPTION_CUBE_NAME);
        final String segmentID = getOptionValue(OPTION_SEGMENT_ID);
        final String jobId = getOptionValue(OPTION_CUBING_JOB_ID);
        final String factColumnsInputPath = getOptionValue(OPTION_INPUT_PATH);
        final String dictPath = getOptionValue(OPTION_DICT_PATH);

        final KylinConfig config = KylinConfig.getInstanceFromEnv();

        DictionaryGeneratorCLI.processSegment(config, cubeName, segmentID, jobId, new DistinctColumnValuesProvider() {
            @Override
            public IReadableTable getDistinctValuesFor(TblColRef col) {
                return new SortedColumnDFSFile(factColumnsInputPath + "/" + col.getIdentity(), col.getType());
            }
        }, new DictionaryProvider() {

            @Override
            public Dictionary<String> getDictionary(TblColRef col) throws IOException {
                CubeManager cubeManager = CubeManager.getInstance(config);
                CubeInstance cube = cubeManager.getCube(cubeName);
                List<TblColRef> uhcColumns = cube.getDescriptor().getAllUHCColumns();
                KylinConfig cubeConfig = cube.getConfig();
                Path colDir;
                if (cubeConfig.isBuildUHCDictWithMREnabled() && uhcColumns.contains(col)) {
                    colDir = new Path(dictPath, col.getIdentity());
                } else {
                    colDir = new Path(factColumnsInputPath, col.getIdentity());
                }
                FileSystem fs = HadoopUtil.getWorkingFileSystem();

                Path dictFile = HadoopUtil.getFilterOnlyPath(fs, colDir,
                        col.getName() + FactDistinctColumnsReducer.DICT_FILE_POSTFIX);
                if (dictFile == null) {
                    logger.info("Dict for '{}' not pre-built.", col.getName());
                    return null;
                }

                try (SequenceFile.Reader reader = new SequenceFile.Reader(HadoopUtil.getCurrentConfiguration(),
                        SequenceFile.Reader.file(dictFile))) {
                    NullWritable key = NullWritable.get();
                    ArrayPrimitiveWritable value = new ArrayPrimitiveWritable();
                    reader.next(key, value);

                    ByteBuffer buffer = new ByteArray((byte[]) value.get()).asBuffer();
                    try (DataInputStream is = new DataInputStream(new ByteBufferBackedInputStream(buffer))) {
                        String dictClassName = is.readUTF();
                        Dictionary<String> dict = (Dictionary<String>) ClassUtil.newInstance(dictClassName);
                        dict.readFields(is);
                        logger.info("DictionaryProvider read dict from file: {}", dictFile);
                        return dict;
                    }
                }
            }
        });

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CreateDictionaryJob(), args);
        System.exit(exitCode);
    }

}
