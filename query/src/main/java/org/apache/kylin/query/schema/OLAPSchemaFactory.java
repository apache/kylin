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

package org.apache.kylin.query.schema;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.metadata.model.DatabaseDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.ProjectManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 */
public class OLAPSchemaFactory implements SchemaFactory {
    public static final Logger logger = LoggerFactory.getLogger(OLAPSchemaFactory.class);

    private final static String SCHEMA_PROJECT = "project";

    @Override
    public Schema create(SchemaPlus parentSchema, String schemaName, Map<String, Object> operand) {
        String project = (String) operand.get(SCHEMA_PROJECT);
        Schema newSchema = new OLAPSchema(project, schemaName, exposeMore(project));
        return newSchema;
    }

    private static Map<String, File> cachedJsons = Maps.newConcurrentMap();

    public static boolean exposeMore(String project) {
        return ProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getProject(project).getConfig()
                .isPushDownEnabled();
    }

    public static File createTempOLAPJson(String project, KylinConfig config) {

        ProjectManager projectManager = ProjectManager.getInstance(config);
        KylinConfig projConfig = projectManager.getProject(project).getConfig();
        Collection<TableDesc> tables = projectManager.listExposedTables(project, exposeMore(project));

        // "database" in TableDesc correspond to our schema
        // the logic to decide which schema to be "default" in calcite:
        // if some schema are named "default", use it.
        // other wise use the schema with most tables
        HashMap<String, Integer> schemaCounts = DatabaseDesc.extractDatabaseOccurenceCounts(tables);
        String majoritySchemaName = "";
        int majoritySchemaCount = 0;
        for (Map.Entry<String, Integer> e : schemaCounts.entrySet()) {
            if (e.getKey().equalsIgnoreCase("default")) {
                majoritySchemaCount = Integer.MAX_VALUE;
                majoritySchemaName = e.getKey();
            }

            if (e.getValue() >= majoritySchemaCount) {
                majoritySchemaCount = e.getValue();
                majoritySchemaName = e.getKey();
            }
        }

        try {

            StringBuilder out = new StringBuilder();
            out.append("{\n");
            out.append("    \"version\": \"1.0\",\n");
            out.append("    \"defaultSchema\": \"" + majoritySchemaName + "\",\n");
            out.append("    \"schemas\": [\n");

            int counter = 0;

            String schemaFactory = projConfig.getSchemaFactory();
            for (String schemaName : schemaCounts.keySet()) {
                out.append("        {\n");
                out.append("            \"type\": \"custom\",\n");
                out.append("            \"name\": \"" + schemaName + "\",\n");
                out.append("            \"factory\": \"" + schemaFactory + "\",\n");
                out.append("            \"operand\": {\n");
                out.append("                \"" + SCHEMA_PROJECT + "\": \"" + project + "\"\n");
                out.append("            },\n");
                createOLAPSchemaFunctions(projConfig.getUDFs(), out);
                out.append("        }\n");

                if (++counter != schemaCounts.size()) {
                    out.append(",\n");
                }
            }

            out.append("    ]\n");
            out.append("}\n");

            String jsonContent = out.toString();
            File file = cachedJsons.get(jsonContent);
            if (file == null) {
                file = File.createTempFile("olap_model_", ".json");
                file.deleteOnExit();
                FileUtils.writeStringToFile(file, jsonContent);

                logger.debug("Adding new schema file {} to cache", file.getName());
                logger.debug("Schema json: " + jsonContent);
                cachedJsons.put(jsonContent, file);
            }

            return file;

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void createOLAPSchemaFunctions(Map<String, String> definedUdfs, StringBuilder out)
            throws IOException {
        Map<String, String> udfs = Maps.newHashMap();
        if (definedUdfs != null)
            udfs.putAll(definedUdfs);

        for (Entry<String, Class<?>> entry : MeasureTypeFactory.getUDAFs().entrySet()) {
            udfs.put(entry.getKey(), entry.getValue().getName());
        }

        int index = 0;
        out.append("            \"functions\": [\n");
        for (Map.Entry<String, String> udf : udfs.entrySet()) {
            String udfName = udf.getKey().trim().toUpperCase(Locale.ROOT);
            String udfClassName = udf.getValue().trim();
            out.append("               {\n");
            out.append("                   name: '" + udfName + "',\n");
            out.append("                   className: '" + udfClassName + "'\n");
            if (index < udfs.size() - 1) {
                out.append("               },\n");
            } else {
                out.append("               }\n");
            }
            index++;
        }
        out.append("            ]\n");
    }
}
