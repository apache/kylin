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

package org.apache.kylin.job.hadoop.cube;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.mr.KylinReducer;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.job.constant.BatchConstants;
import org.apache.kylin.job.hadoop.AbstractHadoopJob;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author yangli9
 */
public class FactDistinctColumnsReducer extends KylinReducer<ShortWritable, Text, NullWritable, Text> {
    private static Logger logger = LoggerFactory.getLogger(FactDistinctColumnsReducer.class);

    private String outputPath;
    private FileSystem fs;
    private List<TblColRef> columnList = new ArrayList<TblColRef>();
    private Set<String> toBeExtractedColumns = new HashSet<String>();

    @Override
    protected void setup(Context context) throws IOException {
        super.publishConfiguration(context.getConfiguration());

        Configuration conf = context.getConfiguration();
        KylinConfig config = AbstractHadoopJob.loadKylinPropsAndMetadata(conf);
        String cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        CubeInstance cube = CubeManager.getInstance(config).getCube(cubeName);
        CubeDesc cubeDesc = cube.getDescriptor();

        long baseCuboidId = Cuboid.getBaseCuboidId(cubeDesc);
        Cuboid baseCuboid = Cuboid.findById(cubeDesc, baseCuboidId);
        columnList = baseCuboid.getColumns();

        outputPath = conf.get(BatchConstants.OUTPUT_PATH);
        fs = FileSystem.get(conf);

        String factDictColNamesStr = conf.get(BatchConstants.CFG_FACT_DICT_COLUMN_NAMES);
        String[] factDictColNames = StringUtils.isEmpty(factDictColNamesStr) ? new String[0] : factDictColNamesStr.split(",");
        toBeExtractedColumns.addAll(Arrays.asList(factDictColNames));
    }

    @Override
    public void reduce(ShortWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        TblColRef col = columnList.get(key.get());
        toBeExtractedColumns.remove(col.getName());

        HashSet<ByteArray> set = new HashSet<ByteArray>();
        for (Text textValue : values) {
            ByteArray value = new ByteArray(Bytes.copy(textValue.getBytes(), 0, textValue.getLength()));
            set.add(value);
        }

        FSDataOutputStream out = fs.create(new Path(outputPath, col.getName()));

        try {
            for (ByteArray value : set) {
                out.write(value.data);
                out.write('\n');
            }
        } finally {
            out.close();
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        // KYLIN-921 create empty file for dimension with all nulls
        for (String colName : toBeExtractedColumns) {
            logger.info("create empty file for column " + colName);
            boolean success = fs.createNewFile(new Path(outputPath, colName));
            if (!success) {
                throw new IOException("failed to create empty file for column " + colName);
            }
        }
    }
}
