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

import java.math.BigInteger;
import java.util.BitSet;
import java.util.List;
import java.util.Set;

import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableDesc;
import org.apache.kylin.cube.model.DimensionDesc;
import org.apache.kylin.job.JoinedFlatTable;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class QueryGenerator {

    private static final Logger logger = LoggerFactory.getLogger(QueryGenerator.class);

    public static List<String> generateQueryList(CubeDesc cubeDesc, int nOfQuery, int maxNumOfDimension) {
        int nDimension = cubeDesc.getDimensions().size();
        if (maxNumOfDimension > nDimension) {
            maxNumOfDimension = nDimension;
        } else if (maxNumOfDimension < 1) {
            maxNumOfDimension = 1;
        }

        int queryMaxSize = getQueryMaxSize(maxNumOfDimension, nDimension);
        queryMaxSize = (int) Math.ceil(queryMaxSize * 0.5);
        if (nOfQuery > queryMaxSize) {
            nOfQuery = queryMaxSize;
        }

        logger.info("Will generate {} queries", nOfQuery);

        List<String> sqlList = Lists.newArrayListWithExpectedSize(nOfQuery);
        Set<BitSet> selected = Sets.newHashSetWithExpectedSize(nOfQuery);
        while (sqlList.size() < nOfQuery) {
            sqlList.add(generateQuery(cubeDesc, selected, maxNumOfDimension));
        }

        return sqlList;
    }

    public static int getQueryMaxSize(int m, int nDimension) {
        int a = nDimension - m >= m ? nDimension - m : m;
        int b = nDimension - a;

        BigInteger result = new BigInteger(String.valueOf(1));
        for (int i = a + 1; i <= nDimension; i++) {
            result = result.multiply(new BigInteger(String.valueOf(i)));
        }
        for (int i = 2; i <= b; i++) {
            result = result.divide(new BigInteger(String.valueOf(i)));
        }
        return result.intValue();
    }

    public static String generateQuery(CubeDesc cubeDesc, Set<BitSet> selected, int maxNumOfDimension) {
        IJoinedFlatTableDesc flatDesc = new CubeJoinedFlatTableDesc(cubeDesc);

        String dimensionStatement = createDimensionStatement(cubeDesc.getDimensions(), selected, maxNumOfDimension);
        String measureStatement = createMeasureStatement(cubeDesc.getMeasures());

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT" + "\n");
        sql.append(dimensionStatement);
        sql.append(measureStatement);

        StringBuilder joinPart = new StringBuilder();
        JoinedFlatTable.appendJoinStatement(flatDesc, joinPart, false, null);
        sql.append(joinPart.toString().replaceAll("DEFAULT\\.", ""));

        sql.append("GROUP BY" + "\n");
        sql.append(dimensionStatement);
        String ret = sql.toString();
        ret = ret.replaceAll("`", "\"");
        return ret;
    }

    public static String createMeasureStatement(List<MeasureDesc> measureList) {
        StringBuilder sql = new StringBuilder();

        for (MeasureDesc measureDesc : measureList) {
            FunctionDesc functionDesc = measureDesc.getFunction();
            if (functionDesc.isSum() || functionDesc.isMax() || functionDesc.isMin()) {
                sql.append("," + functionDesc.getExpression() + "(" + functionDesc.getParameter().getValue() + ")\n");
                break;
            } else if (functionDesc.isCountDistinct()) {
                sql.append(",COUNT" + "(DISTINCT " + functionDesc.getParameter().getValue() + ")\n");
                break;
            }
        }

        return sql.toString();
    }

    public static String createDimensionStatement(List<DimensionDesc> dimensionList, Set<BitSet> selected,
            final int maxNumOfDimension) {
        StringBuilder sql = new StringBuilder();

        BitSet bitSet;

        do {
            bitSet = generateIfSelectList(dimensionList.size(),
                    Math.ceil(maxNumOfDimension * Math.random()) / dimensionList.size());
        } while (bitSet.cardinality() > maxNumOfDimension || bitSet.cardinality() <= 0 || selected.contains(bitSet));
        selected.add(bitSet);

        List<String> selectedCols = Lists.newArrayList();
        int j = 0;
        for (int i = 0; i < dimensionList.size(); i++) {
            if (bitSet.get(i)) {
                DimensionDesc dimensionDesc = dimensionList.get(i);
                String tableName = getTableName(dimensionDesc.getTable());
                String columnName = dimensionDesc.getColumn();
                if (Strings.isNullOrEmpty(columnName) || columnName.equals("{FK}")) {
                    String[] derivedCols = dimensionDesc.getDerived();
                    BitSet subBitSet;
                    do {
                        subBitSet = generateIfSelectList(derivedCols.length, 0.5);
                    } while (subBitSet.cardinality() <= 0);

                    for (int k = 0; k < derivedCols.length; k++) {
                        if (subBitSet.get(k)) {
                            if (j > 0) {
                                sql.append(",");
                            }
                            sql.append(tableName + ".\"" + derivedCols[k] + "\"\n");
                            selectedCols.add(derivedCols[k]);
                            j++;
                        }
                    }
                } else {
                    if (j > 0) {
                        sql.append(",");
                    }
                    sql.append(tableName + ".\"" + columnName + "\"\n");
                    selectedCols.add(columnName);
                    j++;
                }
            }
        }

        return sql.toString();
    }

    public static BitSet generateIfSelectList(int n, double threshold) {
        BitSet bitSet = new BitSet(n);
        for (int i = 0; i < n; i++) {
            if (Math.random() < threshold) {
                bitSet.set(i);
            }
        }
        return bitSet;
    }

    public static String getTableName(String name) {
        int lastIndexOfDot = name.lastIndexOf(".");
        if (lastIndexOfDot >= 0) {
            name = name.substring(lastIndexOfDot + 1);
        }
        return name;
    }
}
