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
package org.apache.kylin.metadata.model;

import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;

import java.util.Collections;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.TransactionException;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.junit.annotation.JdbcMetadataInfo;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.model.graph.JoinsGraph;
import org.apache.kylin.metadata.model.util.ComputedColumnUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@MetadataInfo
@JdbcMetadataInfo
public class ComputedColumnManagerTest {

    @BeforeEach
    void SetRexStrExtractor() {
        ComputedColumnUtil.setEXTRACTOR((model, config, cc) -> cc);
    }

    @Test
    void testComputedColumnManager() {
        String project = "default";
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        ComputedColumnManager ccManager = ComputedColumnManager.getInstance(config, project);

        NDataModel model = Mockito.mock(NDataModel.class);
        JoinsGraph graph = Mockito.mock(JoinsGraph.class);
        Mockito.when(model.getUuid()).thenReturn(RandomUtil.randomUUIDStr());
        Mockito.when(model.getJoinsGraph()).thenReturn(graph);
        Mockito.when(graph.getSubGraphByAlias(anySet())).thenReturn(graph);

        ComputedColumnDesc cc1 = new ComputedColumnDesc();
        cc1.setProject(project);
        cc1.setColumnName("cc1");
        cc1.setTableIdentity("SSB.TEST_KYLIN_FACT");
        cc1.setExpression("TEST_ORDER.ORDER_ID + 1");
        ComputedColumnDesc cc2 = new ComputedColumnDesc();
        cc2.setProject(project);
        cc2.setColumnName("cc2");
        cc2.setTableIdentity("SSB.TEST_ORDER");
        cc2.setExpression("TEST_ORDER.ORDER_ID + 1");
        UnitOfWork.doInTransactionWithRetry(() -> {
            ComputedColumnManager mgr = ComputedColumnManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            mgr.saveCCWithCheck(model, cc1);
            mgr.saveCCWithCheck(model, cc2);
            return true;
        }, project);
        Assertions.assertEquals(2, ccManager.listAll().size());

        // conflict with same expression
        ComputedColumnDesc cc3 = new ComputedColumnDesc();
        cc3.setProject("default");
        cc3.setColumnName("cc3");
        cc3.setTableIdentity("SSB.TEST_KYLIN_FACT");
        cc3.setExpression("TEST_ORDER.ORDER_ID + 1");
        Assertions.assertThrows(TransactionException.class, () -> {
            UnitOfWork.doInTransactionWithRetry(() -> {
                ComputedColumnManager mgr = ComputedColumnManager.getInstance(KylinConfig.getInstanceFromEnv(),
                        project);
                mgr.saveCCWithCheck(model, cc3);
                return true;
            }, project);
        });

        // conflict with same name
        ComputedColumnDesc cc11 = new ComputedColumnDesc();
        cc11.setProject("default");
        cc11.setColumnName("cc1");
        cc11.setTableIdentity("SSB.TEST_KYLIN_FACT");
        cc11.setExpression("TEST_ORDER.ORDER_ID + 2");
        Assertions.assertThrows(TransactionException.class, () -> {
            UnitOfWork.doInTransactionWithRetry(() -> {
                ComputedColumnManager mgr = ComputedColumnManager.getInstance(KylinConfig.getInstanceFromEnv(),
                        project);
                ComputedColumnManager mgrSpy = Mockito.spy(mgr);
                Mockito.when(mgrSpy.relationModels(anyString(), anyString())).thenReturn(Collections.singletonList("mockUuid"));
                mgrSpy.saveCCWithCheck(model, cc11);
                return true;
            }, project);
        });

        // test with very long expression
        StringBuilder longStr = new StringBuilder("TEST_ORDER.ORDER_ID");
        for (int i = 0; i < 100; i++) {
            longStr.append(" + TEST_ORDER.ORDER_ID");
        }
        ComputedColumnDesc cc4 = new ComputedColumnDesc();
        cc4.setProject("default");
        cc4.setColumnName("cc4");
        cc4.setTableIdentity("SSB.TEST_KYLIN_FACT");
        cc4.setExpression(longStr + " + 1");
        ComputedColumnDesc cc5 = new ComputedColumnDesc();
        cc5.setProject("default");
        cc5.setColumnName("cc5");
        cc5.setTableIdentity("SSB.TEST_KYLIN_FACT");
        cc5.setExpression(longStr + " + 2");
        UnitOfWork.doInTransactionWithRetry(() -> {
            ComputedColumnManager mgr = ComputedColumnManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            mgr.saveCCWithCheck(model, cc4);
            mgr.saveCCWithCheck(model, cc5);
            return true;
        }, project);
        Assertions.assertEquals(4, ccManager.listAll().size());
    }
}
