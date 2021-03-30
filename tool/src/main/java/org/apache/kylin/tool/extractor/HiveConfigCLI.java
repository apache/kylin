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

package org.apache.kylin.tool.extractor;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

public class HiveConfigCLI {

    private static final Logger logger = LoggerFactory.getLogger(HiveConfigCLI.class);

    private String inputFileName;
    private String outputFileName;

    public HiveConfigCLI(String inputFileName, String outputFileName) {
        this.inputFileName = inputFileName;
        this.outputFileName = outputFileName;
        logger.info("{} will be parsed to {}", inputFileName, outputFileName);
    }

    public void parse() throws ParserConfigurationException, IOException, SAXException {
        File input = new File(inputFileName);
        File output = new File(outputFileName);
        StringBuilder buffer = new StringBuilder();
        if (input.exists()) {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(input);
            NodeList nl = doc.getElementsByTagName("property");
            for (int i = 0; i < nl.getLength(); i++) {
                String name = doc.getElementsByTagName("name").item(i).getFirstChild().getNodeValue();
                String value = doc.getElementsByTagName("value").item(i).getFirstChild().getNodeValue();
                buffer.append("--hiveconf ");
                buffer.append(name);
                buffer.append("=");
                buffer.append(value);
                buffer.append(" ");
                logger.info("Parsing key: {}, value: {}", name, value);
            }
            FileUtils.writeStringToFile(output, buffer.toString(), Charset.defaultCharset());
        }
    }

    public static void main(String[] args) throws IOException, SAXException, ParserConfigurationException {
        if (args.length != 2) {
            System.out.println("Usage: HiveConfigCLI <inputFileName> <outputFileName>");
            System.exit(1);
        }

        HiveConfigCLI cli = new HiveConfigCLI(args[0], args[1]);
        cli.parse();
    }
}
