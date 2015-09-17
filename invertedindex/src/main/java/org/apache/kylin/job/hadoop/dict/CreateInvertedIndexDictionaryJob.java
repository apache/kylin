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

package org.apache.kylin.job.hadoop.dict;

import org.apache.commons.cli.Options;
import org.apache.hadoop.util.ToolRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.dict.DistinctColumnValuesProvider;
import org.apache.kylin.engine.mr.DFSFileTable;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.invertedindex.IIInstance;
import org.apache.kylin.invertedindex.IIManager;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.source.ReadableTable;

/**
 */
public class CreateInvertedIndexDictionaryJob extends AbstractHadoopJob {

    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        try {
            options.addOption(OPTION_II_NAME);
            options.addOption(OPTION_INPUT_PATH);
            parseOptions(options, args);

            final String iiname = getOptionValue(OPTION_II_NAME);
            final String factColumnsInputPath = getOptionValue(OPTION_INPUT_PATH);
            final KylinConfig config = KylinConfig.getInstanceFromEnv();

            IIManager mgr = IIManager.getInstance(config);
            IIInstance ii = mgr.getII(iiname);

            mgr.buildInvertedIndexDictionary(ii.getFirstSegment(), new DistinctColumnValuesProvider() {
                @Override
                public ReadableTable getDistinctValuesFor(TblColRef col) {
                    return new DFSFileTable(factColumnsInputPath + "/" + col.getName(), -1);
                }
            });
            return 0;
        } catch (Exception e) {
            printUsage(options);
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CreateInvertedIndexDictionaryJob(), args);
        System.exit(exitCode);
    }

}
