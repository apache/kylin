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
//package io.kyligence.kap.rest.service.handler;
//
//import java.util.List;
//
//import org.junit.After;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Ignore;
//import org.junit.Test;
//
//import com.google.common.collect.Lists;
//
//import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
//import io.kyligence.kap.metadata.cube.model.IndexPlan;
//import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
//import io.kyligence.kap.metadata.cube.model.LayoutEntity;
//import io.kyligence.kap.event.manager.EventDao;
//import io.kyligence.kap.event.model.AccelerateEvent;
//import io.kyligence.kap.event.model.Event;
//import io.kyligence.kap.event.model.EventContext;
//import lombok.extern.slf4j.Slf4j;
//
//@Slf4j
//public class AccelerateEventHandlerTest extends NLocalFileMetadataTestCase {
//
//    private static final String DEFAULT_PROJECT = "default";
//    @Before
//    public void setUp() throws Exception {
//        this.createTestMetadata();
//    }
//
//    @After
//    public void tearDown() throws Exception {
//        this.cleanupTestMetadata();
//    }
//
//    @Test
//    @Ignore("reopen it after #8219")
//    public void testHandlerIdempotent() throws Exception {
//
//        getTestConfig().setProperty("kylin.server.mode", "query");
//
//        AccelerateEvent event = new AccelerateEvent();
//        event.setModels(Lists.newArrayList());
//        event.setFavoriteMark(true);
//        event.setProject(DEFAULT_PROJECT);
//        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
//        IndexPlan cubePlan1 = indexPlanManager.getIndexPlan("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96");
//        logLayouts(cubePlan1);
//        int layoutCount1 = cubePlan1.getAllLayouts().size();
//
//        event.setSqlPatterns(Lists.newArrayList("select CAL_DT, LSTG_FORMAT_NAME, sum(PRICE), sum(ITEM_COUNT) from TEST_KYLIN_FACT where CAL_DT = '2012-01-02' group by CAL_DT, LSTG_FORMAT_NAME"));
//        EventContext eventContext = new EventContext(event, getTestConfig());
//        AccelerateEventHandler handler = new AccelerateEventHandler();
//        // add favorite sql to update model and post an new AddCuboidEvent
//        handler.handle(eventContext);
//
//        IndexPlan cubePlan2 = indexPlanManager.getIndexPlan("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96");
//        logLayouts(cubePlan2);
//        int layoutCount2 = cubePlan2.getAllLayouts().size();
//        Assert.assertEquals(layoutCount1 + 1, layoutCount2);
//
//        List<Event> events = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT).getEvents();
//        Assert.assertNotNull(events);
//        Assert.assertEquals(2, events.size());
//
//        // run again, and model will not update and will not post an new AddCuboidEvent
//        handler.handle(eventContext);
//
//        IndexPlan cubePlan3 = indexPlanManager.getIndexPlan("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96");
//        int layoutCount3 = cubePlan3.getAllLayouts().size();
//        Assert.assertEquals(layoutCount3, layoutCount2);
//
//        // cancel favorite sql will not update model and indexPlan, just post an new RemoveCuboidEvent
//        event.setFavoriteMark(false);
//        handler.handle(eventContext);
//
//        IndexPlan cubePlan4 = indexPlanManager.getIndexPlan("abe3bf1a-c4bc-458d-8278-7ea8b00f5e96");
//        int layoutCount4 = cubePlan4.getAllLayouts().size();
//        Assert.assertEquals(layoutCount3, layoutCount4);
//
//        events = EventDao.getInstance(getTestConfig(), DEFAULT_PROJECT).getEvents();
//        Assert.assertNotNull(events);
//        Assert.assertEquals(3, events.size());
//
//        getTestConfig().setProperty("kylin.server.mode", "all");
//
//    }
//
//    private void logLayouts(IndexPlan indexPlan) {
//        for (LayoutEntity layout : indexPlan.getAllLayouts()) {
//            log.debug("layout id:{} -- {}, auto:{}, manual:{}, col:{}, sort:{}", layout.getId(),
//                    layout.getIndex().getId(), layout.isAuto(), layout.isManual(), layout.getColOrder(),
//                    layout.getSortByColumns());
//        }
//    }
//
//
//}
