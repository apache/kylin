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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.util.AbstractApplication;
import org.apache.kylin.common.util.OptionsHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProbabilityGeneratorCLI extends AbstractApplication {

    private static final Logger logger = LoggerFactory.getLogger(ProbabilityGeneratorCLI.class);

    private static final Option OPTION_SIZE = OptionBuilder.withArgName("size").hasArg().isRequired(true)
            .withDescription("Specify the size of query set to be generated").create("size");
    private static final Option OPTION_OUTPUT = OptionBuilder.withArgName("output").hasArg().isRequired(false)
            .withDescription("Specify the output path for generated probability set").create("output");

    protected final Options options;
    private int size;
    private String outputPath;

    public ProbabilityGeneratorCLI() {
        options = new Options();
        options.addOption(OPTION_SIZE);
        options.addOption(OPTION_OUTPUT);
    }

    protected Options getOptions() {
        return options;
    }

    protected void execute(OptionsHelper optionsHelper) throws Exception {
        this.size = Integer.parseInt(optionsHelper.getOptionValue(OPTION_SIZE));
        this.outputPath = optionsHelper.getOptionValue(OPTION_OUTPUT);

        run();
    }

    public double[] execute(int sizeOfQueryList, String outputPath) throws Exception {
        this.size = sizeOfQueryList;
        this.outputPath = outputPath;

        return run();
    }

    private double[] run() throws Exception {
        double[] probArray = ProbabilityGenerator.generateProbabilityList(this.size);
        storeProbability(probArray, outputPath);
        return ProbabilityGenerator.generateProbabilityCumulative(probArray);
    }

    public double[] execute(int size) throws Exception {
        this.size = size;
        double[] pQueryArray = ProbabilityGenerator.generateProbabilityList(this.size);
        return ProbabilityGenerator.generateProbabilityCumulative(pQueryArray);
    }

    public static void storeProbability(double[] probArray, String outputPath) throws IOException {
        try (BufferedWriter bufferedWriter = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(outputPath + ".prob"), StandardCharsets.UTF_8))) {
            for (double elem : probArray) {
                bufferedWriter.append(String.valueOf(elem));
                bufferedWriter.append("\n");
                logger.info(String.valueOf(elem));
            }
        }
    }

    public static void main(String[] args) {
        ProbabilityGeneratorCLI probabilityGeneratorCLI = new ProbabilityGeneratorCLI();
        probabilityGeneratorCLI.execute(args);
    }
}
