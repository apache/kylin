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

package org.apache.kylin.measure;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.kylin.common.persistence.ResourceTool;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeDescManager;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.adapter.AbstractHBaseMappingAdapter;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.cube.model.HBaseMappingDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MeasureManagerTest extends LocalFileMetadataTestCase {
    private static final String MEASURE_DATA_DIR = "../examples/test_case_data/measure/";
    private static final String MEASURE_DATA_SUFFIX = ".json";

    @Before
    public void setUp() throws Exception {
        // BasicConfigurator.configure();
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testInitAllCube() throws IOException {
        ResourceTool.main(new String[]{"remove", "/measure"});
        MeasureManager.main(new String[]{"init", "all"});
        // clear manager for reload
        getTestConfig().clearManagers();

        List<CubeInstance> allCubes = getCubeManager().listAllCubes();

        for (CubeInstance cube : allCubes) {
            Set<MeasureDesc> measureFromCubeDesc = Sets.newHashSet(cube.getMeasures());
            Set<MeasureDesc> measureFromCache = getMeasureManager()
                    .getMeasuresInCube(cube.getProject(), cube.getName())
                    .stream()
                    .map(m -> m.getMeasureDesc())
                    .collect(Collectors.toSet());
            assertEquals(measureFromCubeDesc, measureFromCache);
        }
    }

    @Test
    public void testAddMeasureOnNewCube() throws IOException {
        String newCubeName = "ci_inner_join_cube";
        CubeInstance cubeInstance = getCubeManager().getCube(newCubeName);
        CubeDesc cubeDesc = cubeInstance.getDescriptor();
        MeasureDesc newMeasure = getMAX_SELLER_ID();
        CubeDesc updatedCubeDesc = addMeasure(cubeDesc, newMeasure);
        getCubeDescManager().updateCubeDesc(updatedCubeDesc);
        List<MeasureInstance> measuresInsInCube = getMeasureManager().getMeasuresInCube(cubeInstance.getProject(), cubeInstance.getName());
        List<MeasureDesc> measuresInCube = measuresInsInCube
                .stream()
                .map(m -> m.getMeasureDesc())
                .collect(Collectors.toList());
        assertTrue(measuresInCube.contains(newMeasure));

        measuresInsInCube.forEach(m -> assertTrue(m.getKey() + " has some segments: " + m.getSegmentsName(), m.getSegments().size() == 0));
    }

    @Test
    public void testDeleteMeasureOnNewCube() throws IOException {
        String newCubeName = "ci_inner_join_cube";
        CubeInstance cubeInstance = getCubeManager().getCube(newCubeName);
        CubeDesc cubeDesc = cubeInstance.getDescriptor();
        MeasureDesc needDeleteMeasure = cubeDesc.getMeasures().get(cubeDesc.getMeasures().size() >> 1);
        CubeDesc updatedCubeDesc = deleteMeasure(cubeDesc, needDeleteMeasure);
        getCubeDescManager().updateCubeDesc(updatedCubeDesc);
        List<MeasureInstance> measuresInsInCube = getMeasureManager().getMeasuresInCube(cubeInstance.getProject(), cubeInstance.getName());
        List<MeasureDesc> measuresInCube = measuresInsInCube
                .stream()
                .map(m -> m.getMeasureDesc())
                .collect(Collectors.toList());
        assertTrue(!measuresInCube.contains(needDeleteMeasure));
    }



    @Test
    public void testAddMeasureOnBuiltCube() throws IOException {
        CubeInstance cubeInstance = getCubeManager().getCube("test_kylin_cube_with_slr_ready_2_segments");

        List<MeasureInstance> measuresInsInCubeBeforeAdd = getMeasureManager().getMeasuresInCube(cubeInstance.getProject(), cubeInstance.getName());

        measuresInsInCubeBeforeAdd.forEach(m -> assertTrue(m.getSegments().size() == cubeInstance.getSegments(SegmentStatusEnum.READY).size()));

        CubeDesc cubeDesc = cubeInstance.getDescriptor();
        MeasureDesc newMeasure = getMAX_SELLER_ID();
        CubeDesc updatedCubeDesc = addMeasure(cubeDesc, newMeasure);
        getCubeDescManager().updateCubeDesc(updatedCubeDesc);

        List<MeasureInstance> measuresInsInCubeAfterAdd = getMeasureManager().getMeasuresInCube(cubeInstance.getProject(), cubeInstance.getName());
        List<MeasureDesc> measuresInCube = measuresInsInCubeAfterAdd
                .stream()
                .map(m -> m.getMeasureDesc())
                .collect(Collectors.toList());
        assertTrue(measuresInCube.contains(newMeasure));

        for (MeasureInstance m : measuresInsInCubeAfterAdd) {
            if (m.getName().equals(newMeasure.getName())) {

                assertTrue(m.getSegments().size() == 0);
            } else {
                assertTrue(m.getSegments().size() == cubeInstance.getSegments(SegmentStatusEnum.READY).size());
            }
        }
    }

    @Test
    public void testGetMeasureAfterDropSegment() throws IOException {
        getMeasureManager();
        CubeInstance cubeInstance = getCubeManager().getCube("test_kylin_cube_with_slr_ready_3_segments");
        CubeSegment firstSegment = cubeInstance.getFirstSegment();
        CubeInstance cubeAfterDelete = getCubeManager().updateCubeDropSegments(cubeInstance, firstSegment);

        List<MeasureInstance> measuresInCube = getMeasureManager().getMeasuresInCube(cubeInstance.getProject(), cubeInstance.getName());
        for (MeasureInstance m : measuresInCube) {
            assertTrue(m.getSegments().size() == cubeAfterDelete.getSegments(SegmentStatusEnum.READY).size());
            assertTrue(!m.getSegments().contains(firstSegment));
        }
    }

    private CubeDescManager getCubeDescManager() {
        return CubeDescManager.getInstance(getTestConfig());
    }

    private CubeManager getCubeManager() {
        return CubeManager.getInstance(getTestConfig());
    }

    public MeasureDesc getMAX_SELLER_ID() {
        return parseMeasureData("MAX_SELLER_ID");
    }

    private MeasureDesc parseMeasureData(String path) {
        File file = new File(MEASURE_DATA_DIR + path + MEASURE_DATA_SUFFIX);
        try {
            return JsonUtil.readValue(file, MeasureDesc.class);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private CubeDesc addMeasure(CubeDesc cubeDesc, MeasureDesc needAdd) {
        List<MeasureDesc> measures = Lists.newArrayList(cubeDesc.getMeasures());
        measures.add(needAdd);
        cubeDesc.setMeasures(measures);
        HBaseMappingDesc hBaseMappingDesc = AbstractHBaseMappingAdapter.getHBaseAdapter(getTestConfig()).addMeasure(cubeDesc, Arrays.asList(needAdd.getName()));
        cubeDesc.setHbaseMapping(hBaseMappingDesc);
        return cubeDesc;
    }
    private CubeDesc deleteMeasure(CubeDesc cubeDesc, MeasureDesc needDeleteMeasure) {
        List<MeasureDesc> newMeasures = Lists.newArrayList(cubeDesc.getMeasures());
        newMeasures.remove(needDeleteMeasure);
        cubeDesc.setMeasures(newMeasures);
        HBaseMappingDesc hBaseMappingDesc = AbstractHBaseMappingAdapter.getHBaseAdapter(getTestConfig()).getHBaseMappingOnlyOnce(cubeDesc);
        cubeDesc.setHbaseMapping(hBaseMappingDesc);
        return cubeDesc;
    }
    private MeasureManager getMeasureManager() {
        return MeasureManager.getInstance(getTestConfig());
    }
}
