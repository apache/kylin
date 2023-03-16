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
package org.apache.kylin.engine.spark.source;

import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation;
import org.apache.spark.sql.catalyst.catalog.CatalogTableType;

import org.apache.kylin.guava30.shaded.common.collect.Sets;

import lombok.val;

public class SparkSqlUtil {
    public static Dataset<Row> query(SparkSession ss, String sql) {
        return ss.sql(sql);
    }

    public static List<Row> queryForList(SparkSession ss, String sql) {
        return ss.sql(sql).collectAsList();
    }

    public static List<Row> queryAll(SparkSession ss, String table) {
        String sql = String.format(Locale.ROOT, "select * from %s", table);
        return queryForList(ss, sql);
    }

    public static Set<String> getViewOrignalTables(String viewName, SparkSession spark) throws AnalysisException {
        String viewText = spark.sql("desc formatted " + viewName).where("col_name = 'View Text'").head().getString(1);
        val logicalPlan = spark.sessionState().sqlParser().parsePlan(viewText);
        Set<String> viewTables = Sets.newHashSet();
        for (Object l : scala.collection.JavaConverters.seqAsJavaListConverter(logicalPlan.collectLeaves()).asJava()) {
            if (l instanceof UnresolvedRelation) {
                val tableName = ((UnresolvedRelation) l).tableName();
                val size = ((UnresolvedRelation) l).multipartIdentifier().size();
                //In the Hive view, multipartIdentifier size <= 2 and exists in the spark.catalog
                if (size > 2 || !spark.catalog().tableExists(tableName)) {
                    viewTables.add(tableName);
                    continue;
                }
                //if nested view
                if (spark.catalog().getTable(tableName).tableType().equals(CatalogTableType.VIEW().name())) {
                    viewTables.addAll(getViewOrignalTables(tableName, spark));
                } else {
                    viewTables.add(tableName);
                }
            }
        }
        return viewTables;
    }
}
