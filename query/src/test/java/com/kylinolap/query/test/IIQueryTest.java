package com.kylinolap.query.test;

import com.google.common.collect.Maps;
import com.kylinolap.metadata.realization.RealizationType;
import com.kylinolap.query.routing.RoutingRules.RealizationPriorityRule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Map;

/**
 * Created by Hongbin Ma(Binmahone) on 2/2/15.
 */
public class IIQueryTest extends KylinQueryTest {
    @BeforeClass
    public static void setUp() throws Exception {
        KylinQueryTest.setUp();//invoke super class
        distinctCountSupported = false;

        Map<RealizationType, Integer> priorities = Maps.newHashMap();
        priorities.put(RealizationType.INVERTED_INDEX, 0);
        priorities.put(RealizationType.CUBE, 1);
        RealizationPriorityRule.setPriorities(priorities);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        KylinQueryTest.tearDown();//invoke super class
        distinctCountSupported = true;

        Map<RealizationType, Integer> priorities = Maps.newHashMap();
        priorities.put(RealizationType.INVERTED_INDEX, 1);
        priorities.put(RealizationType.CUBE, 0);
        RealizationPriorityRule.setPriorities(priorities);
    }

    @Test
    public void testDetailedQuery() throws Exception {
        execAndCompQuery("src/test/resources/query/sql_ii", null, true);
    }

    @Test
    public void testSingleRunQuery() throws Exception {
        String queryFileName = "src/test/resources/query/sql_ii/query04.sql";

        File sqlFile = new File(queryFileName);
        runSQL(sqlFile, true, true);
        runSQL(sqlFile, true, false);
    }
}
