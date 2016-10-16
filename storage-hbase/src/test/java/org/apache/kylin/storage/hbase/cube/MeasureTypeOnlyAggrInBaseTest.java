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

package org.apache.kylin.storage.hbase.cube;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
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
import org.apache.kylin.storage.IStorageQuery;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class MeasureTypeOnlyAggrInBaseTest extends LocalFileMetadataTestCase {

    private CubeInstance cube;
    private CubeDesc cubeDesc;

    private Set<TblColRef> dimensions;
    private List<FunctionDesc> metrics;

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
        cubeDesc = cube.getDescriptor();

        dimensions = Sets.newHashSet();
        metrics = Lists.newArrayList();
        for (MeasureDesc measureDesc : cubeDesc.getMeasures()) {
            Collections.addAll(metrics, measureDesc.getFunction());
        }

        FunctionDesc mockUpFuncDesc = new FunctionDesc();
        Field field = FunctionDesc.class.getDeclaredField("measureType");
        field.setAccessible(true);
        field.set(mockUpFuncDesc, new MockUpMeasureType());
        metrics.add(mockUpFuncDesc);
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testIdentifyCuboidV1() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, NoSuchFieldException {
        IStorageQuery query = new org.apache.kylin.storage.hbase.cube.v1.CubeStorageQuery(cube);
        long baseCuboidId = cubeDesc.getRowkey().getFullMask();

        Method method = query.getClass().getDeclaredMethod("identifyCuboid", Set.class, Collection.class);
        method.setAccessible(true);

        Object ret = method.invoke(query, Sets.newHashSet(), Lists.newArrayList());

        assertTrue(ret instanceof Cuboid);
        assertNotEquals(baseCuboidId, ((Cuboid) ret).getId());

        ret = method.invoke(query, dimensions, metrics);
        assertEquals(baseCuboidId, ((Cuboid) ret).getId());
    }

    @Test
    public void testIdentifyCuboidV2() throws InvocationTargetException, NoSuchMethodException, IllegalAccessException, NoSuchFieldException {
        CubeDesc cubeDesc = cube.getDescriptor();
        Cuboid ret = Cuboid.identifyCuboid(cubeDesc, Sets.<TblColRef> newHashSet(), Lists.<FunctionDesc> newArrayList());
        long baseCuboidId = cubeDesc.getRowkey().getFullMask();
        assertNotEquals(baseCuboidId, ret.getId());
        ret = Cuboid.identifyCuboid(cubeDesc, dimensions, metrics);
        assertEquals(baseCuboidId, ret.getId());
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
        public boolean onlyAggrInBaseCuboid() {
            return true;
        }
    }
}
