/*
 * Copyright 2013-2014 eBay Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kylinolap.job.hadoop.dict;

import com.kylinolap.common.KylinConfig;
import com.kylinolap.cube.CubeInstance;
import com.kylinolap.cube.CubeManager;
import com.kylinolap.job.hadoop.AbstractHadoopJob;
import org.apache.commons.cli.Options;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author ysong1
 */
public class CreateInvertedIndexDictionaryJob extends AbstractHadoopJob {

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_CUBE_NAME);
            options.addOption(OPTION_INPUT_PATH);
            parseOptions(options, args);

            String cubeName = getOptionValue(OPTION_CUBE_NAME);
            String factColumnsInputPath = getOptionValue(OPTION_INPUT_PATH);
            KylinConfig config = KylinConfig.getInstanceFromEnv();

            CubeManager mgr = CubeManager.getInstance(config);
            CubeInstance cube = mgr.getCube(cubeName);
            if (cube == null || cube.isInvertedIndex() == false)
                throw new IllegalArgumentException("No Inverted Index Cube found by name " + cubeName);

            mgr.buildInvertedIndexDictionary(cube.getFirstSegment(), factColumnsInputPath);
            return 0;
        } catch (Exception e) {
            printUsage(options);
            e.printStackTrace(System.err);
            log.error(e.getLocalizedMessage(), e);
            return 2;
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CreateInvertedIndexDictionaryJob(), args);
        System.exit(exitCode);
    }

}
