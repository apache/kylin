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

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.cli.DictionaryGeneratorCLI;
import org.apache.kylin.dict.DictionaryProvider;
import org.apache.kylin.dict.DistinctColumnValuesProvider;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.engine.mr.SortedColumnDFSFile;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.ReadableTable;

import java.io.IOException;

/**
 * @author ysong1
 */

public class CreateDictionaryJob extends AbstractHadoopJob {

    private int returnCode = 0;

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(OPTION_CUBE_NAME);
        options.addOption(OPTION_SEGMENT_ID);
        options.addOption(OPTION_INPUT_PATH);
        parseOptions(options, args);

        final String cubeName = getOptionValue(OPTION_CUBE_NAME);
        final String segmentID = getOptionValue(OPTION_SEGMENT_ID);
        final String factColumnsInputPath = getOptionValue(OPTION_INPUT_PATH);

        final KylinConfig config = KylinConfig.getInstanceFromEnv();

        DictionaryGeneratorCLI.processSegment(config, cubeName, segmentID, new DistinctColumnValuesProvider() {
            @Override
            public ReadableTable getDistinctValuesFor(TblColRef col) {
                return new SortedColumnDFSFile(factColumnsInputPath + "/" + col.getName(), col.getType());
            }
        }, new DictionaryProvider() {

            @Override
            public Dictionary<String> getDictionary(TblColRef col) {
                if (!config.isReducerLocalBuildDict()) {
                    return null;
                }
                FSDataInputStream is = null;
                try {
                    Path colDir = new Path(factColumnsInputPath, col.getName());
                    Path outputFile = new Path(colDir, col.getName() + FactDistinctColumnsReducer.DICT_FILE_POSTFIX);
                    Configuration conf = HadoopUtil.getCurrentConfiguration();
                    FileSystem fs = HadoopUtil.getFileSystem(outputFile.getName());
                    is = fs.open(outputFile);
                    String dictClassName = is.readUTF();
                    Dictionary<String> dict = (Dictionary<String>) ClassUtil.newInstance(dictClassName);
                    dict.readFields(is);
                    logger.info("DictionaryProvider read dict form file : " + outputFile.getName());
                    return dict;
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                } finally {
                    if (is != null) {
                        try {
                            is.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        });

        return returnCode;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CreateDictionaryJob(), args);
        System.exit(exitCode);
    }

}
