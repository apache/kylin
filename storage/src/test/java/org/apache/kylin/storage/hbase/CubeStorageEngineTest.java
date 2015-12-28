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

package org.apache.kylin.storage.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.measure.MeasureAggregator;
import org.apache.kylin.measure.MeasureIngester;
import org.apache.kylin.measure.MeasureType;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Created by dongli on 12/28/15.
 */
public class CubeStorageEngineTest extends LocalFileMetadataTestCase {

    private CubeInstance cube;

    public CubeManager getCubeManager() {
        return CubeManager.getInstance(getTestConfig());
    }

    private CubeInstance getTestKylinCubeWithSeller() {
        return getCubeManager().getCube("test_kylin_cube_with_slr_empty");
    }

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        MetadataManager.clearCache();

        cube = getTestKylinCubeWithSeller();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testIdentifyCuboid() {
        CubeDesc cubeDesc = cube.getDescriptor();
        CubeStorageEngine engine = new CubeStorageEngine(cube);
        long baseCuboidId = cubeDesc.getRowkey().getFullMask();

        try {
            Method method = engine.getClass().getDeclaredMethod("identifyCuboid", Set.class, Collection.class);
            method.setAccessible(true);

            Set<TblColRef> dimensions = Sets.newHashSet();
            List<FunctionDesc> metrics = Lists.newArrayList();

            Object ret = method.invoke(engine, dimensions, metrics);

            assertTrue(ret instanceof Cuboid);
            assertNotEquals(baseCuboidId, ((Cuboid) ret).getId());

            for (MeasureDesc measureDesc : cubeDesc.getMeasures()) {
                Collections.addAll(metrics, measureDesc.getFunction());
            }

            FunctionDesc mockUpFuncDesc = new FunctionDesc();
            Field field = FunctionDesc.class.getDeclaredField("measureType");
            field.setAccessible(true);
            field.set(mockUpFuncDesc, new MockUpMeasureType());
            metrics.add(mockUpFuncDesc);

            ret = method.invoke(engine, dimensions, metrics);
            assertEquals(baseCuboidId, ((Cuboid) ret).getId());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    class MockUpMeasureType extends MeasureType<String> {

        @Override
        public MeasureIngester<String> newIngester() {
            return null;
        }

        @Override
        public MeasureAggregator<String> newAggregator() {
            return null;
        }

        @Override
        public boolean needRewrite() {
            return false;
        }

        @Override
        public Class<?> getRewriteCalciteAggrFunctionClass() {
            return null;
        }

        @Override
        public boolean onlyAggrInBaseCuboid() {
            return true;
        }
    }
}
