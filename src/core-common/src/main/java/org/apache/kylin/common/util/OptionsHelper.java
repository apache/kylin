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

package org.apache.kylin.common.util;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 */
public class OptionsHelper {
    private CommandLine commandLine;

    public void parseOptions(Options options, String[] args) throws ParseException {
        CommandLineParser parser = new GnuParser();
        commandLine = parser.parse(options, args);
    }

    public Option[] getOptions() {
        return commandLine.getOptions();
    }

    public String getOptionsAsString() {
        StringBuilder buf = new StringBuilder();
        for (Option option : commandLine.getOptions()) {
            buf.append(" ");
            buf.append(option.getOpt());
            if (option.hasArg()) {
                buf.append("=");
                buf.append(option.getValue());
            }
        }
        return buf.toString();
    }

    public String getOptionValue(Option option) {
        return commandLine.getOptionValue(option.getOpt());
    }

    public boolean hasOption(Option option) {
        return commandLine.hasOption(option.getOpt());
    }

    public void printUsage(String programName, Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(programName, options);
    }

    public static String convertToFileURL(String path) {
        if (File.separatorChar != '/') {
            path = path.replace(File.separatorChar, '/');
        }

        return path;
    }

}
