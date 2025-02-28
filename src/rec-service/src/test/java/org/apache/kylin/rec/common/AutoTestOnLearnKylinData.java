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

package org.apache.kylin.rec.common;

import java.io.File;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.AbstractTestCase;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfig.SetAndUnsetThreadLocalConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.guava30.shaded.common.io.Files;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.rec.AbstractContext;
import org.junit.After;
import org.junit.Before;

public abstract class AutoTestOnLearnKylinData extends AbstractTestCase {

    protected String proj = "learn_kylin";
    private File tmpMeta;
    private SetAndUnsetThreadLocalConfig localConfig;

    @Before
    public void setUp() throws Exception {
        overwriteSystemProp("spark.local", "true");
        String metaDir = "src/test/resources/nsmart/learn_kylin/meta";
        File tmpHome = Files.createTempDir();
        tmpMeta = new File(tmpHome, "metadata");
        overwriteSystemProp("KYLIN_HOME", tmpHome.getAbsolutePath());
        FileUtils.copyDirectory(new File(metaDir), tmpMeta);
        FileUtils.touch(new File(tmpHome.getAbsolutePath() + "/kylin.properties"));
        KylinConfig.setKylinConfigForLocalTest(tmpHome.getCanonicalPath());
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        kylinConfig.setProperty("kylin.query.security.acl-tcr-enabled", "false");
        kylinConfig.setProperty("kylin.smart.conf.propose-runner-type", "in-memory");
        kylinConfig.setProperty("kylin.env", "UT");
        localConfig = KylinConfig.setAndUnsetThreadLocalConfig(kylinConfig);
        Class.forName("org.h2.Driver");
    }

    public KylinConfig getTestConfig() {
        return localConfig.get();
    }

    @After
    public void tearDown() throws Exception {
        if (tmpMeta != null)
            FileUtils.forceDelete(tmpMeta);
        ResourceStore.clearCache(localConfig.get());
        localConfig.close();
    }

    protected List<LayoutEntity> collectAllLayouts(List<IndexEntity> indexEntities) {
        List<LayoutEntity> layouts = Lists.newArrayList();
        for (IndexEntity indexEntity : indexEntities) {
            layouts.addAll(indexEntity.getLayouts());
        }
        return layouts;
    }

    protected Set<OlapContext> collectAllOlapContexts(AbstractContext smartContext) {
        Preconditions.checkArgument(smartContext != null);
        Set<OlapContext> olapContexts = Sets.newHashSet();
        final List<AbstractContext.ModelContext> modelContexts = smartContext.getModelContexts();
        modelContexts.forEach(modelCtx -> olapContexts.addAll(modelCtx.getModelTree().getOlapContexts()));

        return olapContexts;
    }
}
