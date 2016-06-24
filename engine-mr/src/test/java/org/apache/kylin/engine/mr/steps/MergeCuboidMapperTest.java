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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.CubeUpdate;
import org.apache.kylin.dict.DictionaryGenerator;
import org.apache.kylin.dict.DictionaryInfo;
import org.apache.kylin.dict.DictionaryManager;
import org.apache.kylin.dict.IterableDictionaryValueEnumerator;
import org.apache.kylin.dict.TrieDictionary;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.source.ReadableTable.TableSignature;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("rawtypes")
public class MergeCuboidMapperTest extends LocalFileMetadataTestCase {

    private static final Logger logger = LoggerFactory.getLogger(MergeCuboidMapperTest.class);

    MapDriver<Text, Text, Text, Text> mapDriver;
    CubeManager cubeManager;
    CubeInstance cube;
    DictionaryManager dictionaryManager;

    TblColRef lfn;
    TblColRef lsi;
    TblColRef ssc;

    private DictionaryInfo makeSharedDict() throws IOException {
        TableSignature signature = new TableSignature();
        signature.setSize(100);
        signature.setLastModifiedTime(System.currentTimeMillis());
        signature.setPath("fake_common_dict");

        DictionaryInfo newDictInfo = new DictionaryInfo("", "", 0, "string", signature);

        List<byte[]> values = new ArrayList<byte[]>();
        values.add(new byte[] { 101, 101, 101 });
        values.add(new byte[] { 102, 102, 102 });
        Dictionary<?> dict = DictionaryGenerator.buildDictionary(DataType.getType(newDictInfo.getDataType()), new IterableDictionaryValueEnumerator(values));
        dictionaryManager.trySaveNewDict(dict, newDictInfo);
        ((TrieDictionary) dict).dump(System.out);

        return newDictInfo;
    }

    @Before
    public void setUp() throws Exception {

        createTestMetadata();

        logger.info("The metadataUrl is : " + getTestConfig());

        MetadataManager.clearCache();
        CubeManager.clearCache();
        ProjectManager.clearCache();
        DictionaryManager.clearCache();

        // hack for distributed cache
        // CubeManager.removeInstance(KylinConfig.createInstanceFromUri("../job/meta"));//to
        // make sure the following mapper could get latest CubeManger
        FileUtils.deleteDirectory(new File("../job/meta"));

        MergeCuboidMapper mapper = new MergeCuboidMapper();
        mapDriver = MapDriver.newMapDriver(mapper);

        cubeManager = CubeManager.getInstance(getTestConfig());
        cube = cubeManager.getCube("test_kylin_cube_without_slr_left_join_ready_2_segments");
        dictionaryManager = DictionaryManager.getInstance(getTestConfig());
        lfn = cube.getDescriptor().findColumnRef("DEFAULT.TEST_KYLIN_FACT", "LSTG_FORMAT_NAME");
        lsi = cube.getDescriptor().findColumnRef("DEFAULT.TEST_KYLIN_FACT", "CAL_DT");
        ssc = cube.getDescriptor().findColumnRef("DEFAULT.TEST_CATEGORY_GROUPINGS", "META_CATEG_NAME");

        DictionaryInfo sharedDict = makeSharedDict();

        boolean isFirstSegment = true;
        for (CubeSegment segment : cube.getSegments()) {

            TableSignature signature = new TableSignature();
            signature.setSize(100);
            signature.setLastModifiedTime(System.currentTimeMillis());
            signature.setPath("fake_dict_for" + lfn.getName() + segment.getName());

            DictionaryInfo newDictInfo = new DictionaryInfo(lfn.getTable(), lfn.getColumnDesc().getName(), lfn.getColumnDesc().getZeroBasedIndex(), "string", signature);

            List<byte[]> values = new ArrayList<byte[]>();
            values.add(new byte[] { 97, 97, 97 });
            if (isFirstSegment)
                values.add(new byte[] { 99, 99, 99 });
            else
                values.add(new byte[] { 98, 98, 98 });
            Dictionary<?> dict = DictionaryGenerator.buildDictionary(DataType.getType(newDictInfo.getDataType()), new IterableDictionaryValueEnumerator(values));
            dictionaryManager.trySaveNewDict(dict, newDictInfo);
            ((TrieDictionary) dict).dump(System.out);

            segment.putDictResPath(lfn, newDictInfo.getResourcePath());
            segment.putDictResPath(lsi, sharedDict.getResourcePath());
            segment.putDictResPath(ssc, sharedDict.getResourcePath());

            // cubeManager.saveResource(segment.getCubeInstance());
            // cubeManager.afterCubeUpdated(segment.getCubeInstance());

            isFirstSegment = false;
        }

        CubeUpdate cubeBuilder = new CubeUpdate(cube);
        cubeBuilder.setToUpdateSegs(cube.getSegments().toArray(new CubeSegment[cube.getSegments().size()]));
        cube = cubeManager.updateCube(cubeBuilder);

    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
        FileUtils.deleteDirectory(new File("../job/meta"));
    }

    @Test
    public void test() throws IOException, ParseException {

        //        String cubeName = "test_kylin_cube_without_slr_left_join_ready_2_segments";

        CubeSegment newSeg = cubeManager.mergeSegments(cube, 0L, Long.MAX_VALUE, 0, 0, false);
        //        String segmentName = newSeg.getName();

        final Dictionary<?> dictionary = cubeManager.getDictionary(newSeg, lfn);
        assertTrue(dictionary == null);
        //        ((TrieDictionary) dictionary).dump(System.out);

        // hack for distributed cache
        //        File metaDir = new File("../job/meta");
        //        FileUtils.copyDirectory(new File(getTestConfig().getMetadataUrl()), metaDir);
        //
        //        mapDriver.getConfiguration().set(BatchConstants.CFG_CUBE_NAME, cubeName);
        //        mapDriver.getConfiguration().set(BatchConstants.CFG_CUBE_SEGMENT_NAME, segmentName);
        //        // mapDriver.getConfiguration().set(KylinConfig.KYLIN_METADATA_URL,
        //        // "../job/meta");
        //
        //        byte[] key = new byte[] { 0, 0, 0, 0, 0, 0, 0, -92, 1, 1, 1 };
        //        byte[] value = new byte[] { 1, 2, 3 };
        //        byte[] newkey = new byte[] { 0, 0, 0, 0, 0, 0, 0, -92, 1, 1, 2 };
        //        byte[] newvalue = new byte[] { 1, 2, 3 };
        //
        //        mapDriver.withInput(new Text(key), new Text(value));
        //        mapDriver.withOutput(new Text(newkey), new Text(newvalue));
        //        mapDriver.setMapInputPath(new Path("/apps/hdmi-prod/b_kylin/prod/kylin-f24668f6-dcff-4cb6-a89b-77f1119df8fa/vac_sw_cube_v4/cuboid/15d_cuboid"));
        //
        //        mapDriver.runTest();
    }
}
