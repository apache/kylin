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

package org.apache.kylin.engine.mr.steps;

import org.apache.kylin.shaded.com.google.common.collect.Lists;
import org.apache.kylin.shaded.com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.CubeJoinedFlatTableEnrich;
import org.apache.kylin.dict.ShrunkenDictionary;
import org.apache.kylin.dict.ShrunkenDictionaryBuilder;
import org.apache.kylin.engine.EngineFactory;
import org.apache.kylin.engine.mr.IMRInput;
import org.apache.kylin.engine.mr.KylinMapper;
import org.apache.kylin.engine.mr.MRUtil;
import org.apache.kylin.engine.mr.common.AbstractHadoopJob;
import org.apache.kylin.engine.mr.common.BatchConstants;
import org.apache.kylin.engine.mr.common.DictionaryGetterUtil;
import org.apache.kylin.metadata.model.TblColRef;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExtractDictionaryFromGlobalMapper<KEYIN, Object> extends KylinMapper<KEYIN, Object, Text, Text> {
    private String cubeName;
    private CubeDesc cubeDesc;
    private CubeInstance cube;
    private CubeSegment cubeSeg;

    private IMRInput.IMRTableInputFormat flatTableInputFormat;
    private CubeJoinedFlatTableEnrich intermediateTableDesc;

    private List<TblColRef> globalColumns;
    private int[] globalColumnIndex;
    private List<Set<String>> globalColumnValues;

    private String splitKey;
    private KylinConfig config;

    @Override
    protected void doSetup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        bindCurrentConfiguration(conf);
        config = AbstractHadoopJob.loadKylinPropsAndMetadata();

        cubeName = conf.get(BatchConstants.CFG_CUBE_NAME);
        cube = CubeManager.getInstance(config).getCube(cubeName);
        cubeDesc = cube.getDescriptor();
        cubeSeg = cube.getSegmentById(conf.get(BatchConstants.CFG_CUBE_SEGMENT_ID));
        flatTableInputFormat = MRUtil.getBatchCubingInputSide(cubeSeg).getFlatTableInputFormat();

        intermediateTableDesc = new CubeJoinedFlatTableEnrich(EngineFactory.getJoinedFlatTableDesc(cubeSeg), cubeDesc);

        globalColumns = cubeDesc.getAllGlobalDictColumns();
        globalColumnIndex = new int[globalColumns.size()];
        globalColumnValues = Lists.newArrayListWithExpectedSize(globalColumns.size());

        for (int i = 0; i < globalColumns.size(); i++) {
            TblColRef colRef = globalColumns.get(i);
            int columnIndexOnFlatTbl = intermediateTableDesc.getColumnIndex(colRef);
            globalColumnIndex[i] = columnIndexOnFlatTbl;
            globalColumnValues.add(Sets.<String> newHashSet());
        }

        splitKey = DictionaryGetterUtil.getInputSplitSignature(cubeSeg, context.getInputSplit());
    }

    @Override
    public void doMap(KEYIN key, Object record, Context context) throws IOException, InterruptedException {
        Collection<String[]> rowCollection = flatTableInputFormat.parseMapperInput(record);

        for (String[] row : rowCollection) {
            for (int i = 0; i < globalColumnIndex.length; i++) {
                String fieldValue = row[globalColumnIndex[i]];
                if (fieldValue == null)
                    continue;

                globalColumnValues.get(i).add(fieldValue);
            }
        }
    }

    @Override
    protected void doCleanup(Context context) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(context.getConfiguration());
        Path outputDirBase = new Path(context.getConfiguration().get(FileOutputFormat.OUTDIR));

        Map<TblColRef, Dictionary<String>> globalDictionaryMap = cubeSeg.buildGlobalDictionaryMap(globalColumns.size());

        ShrunkenDictionary.StringValueSerializer strValueSerializer = new ShrunkenDictionary.StringValueSerializer();
        for (int i = 0; i < globalColumns.size(); i++) {
            List<String> colDistinctValues = Lists.newArrayList(globalColumnValues.get(i));
            if (colDistinctValues.size() == 0) {
                continue;
            }
            // sort values to accelerate the encoding process by reducing the swapping of global dictionary slices
            Collections.sort(colDistinctValues);

            ShrunkenDictionaryBuilder<String> dictBuilder = new ShrunkenDictionaryBuilder<>(globalDictionaryMap.get(globalColumns.get(i)));
            for (String colValue : colDistinctValues) {
                dictBuilder.addValue(colValue);
            }
            Dictionary<String> shrunkenDict = dictBuilder.build(strValueSerializer);

            Path colDictDir = new Path(outputDirBase, globalColumns.get(i).getIdentity());
            if (!fs.exists(colDictDir)) {
                fs.mkdirs(colDictDir);
            }
            try (DataOutputStream dos = fs.create(new Path(colDictDir, splitKey))) {
                shrunkenDict.write(dos);
            }
        }
    }
}
