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

package org.apache.kylin.tool.query;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.model.CubeDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

public class QueryGeneratorCLI extends AbstractApplication {

    private static final Logger logger = LoggerFactory.getLogger(QueryGeneratorCLI.class);

    private static final Option OPTION_MAX_DIM_NUM = OptionBuilder.withArgName("maxDimNum").hasArg().isRequired(false)
            .withDescription("Specify the maximum number of dimensions for generating a query").create("maxDimNum");
    private static final Option OPTION_CUBE = OptionBuilder.withArgName("cube").hasArg().isRequired(true)
            .withDescription("Specify for which cube to generate query").create("cube");
    private static final Option OPTION_SIZE = OptionBuilder.withArgName("size").hasArg().isRequired(true)
            .withDescription("Specify the size of query set to be generated").create("size");
    private static final Option OPTION_OUTPUT = OptionBuilder.withArgName("output").hasArg().isRequired(true)
            .withDescription("Specify the output path for generated query set").create("output");

    public static final String SQL_SEPARATOR = "#############";

    protected final Options options;

    private int sizeOfQueryList;
    private String outputPath;
    private int maxNumOfDim = 3;

    public QueryGeneratorCLI() {
        options = new Options();
        options.addOption(OPTION_MAX_DIM_NUM);
        options.addOption(OPTION_CUBE);
        options.addOption(OPTION_SIZE);
        options.addOption(OPTION_OUTPUT);
    }

    protected Options getOptions() {
        return options;
    }

    protected void execute(OptionsHelper optionsHelper) throws Exception {
        String temp = optionsHelper.getOptionValue(OPTION_MAX_DIM_NUM);
        if (!Strings.isNullOrEmpty(temp)) {
            this.maxNumOfDim = Integer.parseInt(temp);
        }

        this.outputPath = optionsHelper.getOptionValue(OPTION_OUTPUT);
        this.sizeOfQueryList = Integer.parseInt(optionsHelper.getOptionValue(OPTION_SIZE));

        String cubeName = optionsHelper.getOptionValue(OPTION_CUBE);
        run(cubeName, true);
    }

    public Pair<List<String>, double[]> execute(String cubeName, int sizeOfQueryList, String outputPath)
            throws Exception {
        this.outputPath = outputPath;
        this.sizeOfQueryList = sizeOfQueryList;

        return run(cubeName, true);
    }

    public Pair<List<String>, double[]> execute(String cubeName, int sizeOfQueryList) throws Exception {
        this.sizeOfQueryList = sizeOfQueryList;
        return run(cubeName, false);
    }

    private Pair<List<String>, double[]> run(String cubeName, boolean needToStore) throws Exception {
        CubeDesc cubeDesc = CubeDescManager.getInstance(KylinConfig.getInstanceFromEnv()).getCubeDesc(cubeName);

        //Generate query list
        List<String> queryList = QueryGenerator.generateQueryList(cubeDesc, sizeOfQueryList, maxNumOfDim);
        ProbabilityGeneratorCLI probabilityGeneratorCLI = new ProbabilityGeneratorCLI();
        double[] pCumArray;
        if (needToStore) {
            storeQuery(queryList, outputPath + "/" + cubeName);
            pCumArray = probabilityGeneratorCLI.execute(queryList.size(), outputPath + "/" + cubeName);
        } else {
            pCumArray = probabilityGeneratorCLI.execute(queryList.size());
        }
        return new Pair<>(queryList, pCumArray);
    }

    public static void storeQuery(List<String> querySet, String outputPath) throws IOException {
        String fileName = outputPath + ".sql";
        File parentFile = new File(fileName).getParentFile();
        if (!parentFile.exists()) {
            parentFile.mkdirs();
        }
        try (BufferedWriter bufferedWriter = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(fileName), StandardCharsets.UTF_8))) {
            for (String query : querySet) {
                bufferedWriter.append(query);
                bufferedWriter.append(SQL_SEPARATOR + "\n");
                logger.info(query);
            }
        }
    }

    public static void main(String[] args) {
        QueryGeneratorCLI queryGeneratorCLI = new QueryGeneratorCLI();
        queryGeneratorCLI.execute(args);
    }
}
