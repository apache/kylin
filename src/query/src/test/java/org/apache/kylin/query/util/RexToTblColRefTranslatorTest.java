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

package org.apache.kylin.query.util;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexToSqlNodeConverter;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.ColumnRowType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RexToTblColRefTranslatorTest {

    private static final RelDataTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

    private RelDataType boolRelDataType = TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN);
    private RelDataType timestampRelDataType = TYPE_FACTORY.createSqlType(SqlTypeName.TIMESTAMP);
    private final RelDataType bingIntRelDataType = TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT);

    private final SqlIntervalQualifier second = new SqlIntervalQualifier(TimeUnit.SECOND, null, SqlParserPos.ZERO);
    private final SqlIntervalQualifier minute = new SqlIntervalQualifier(TimeUnit.MINUTE, null, SqlParserPos.ZERO);
    private final SqlIntervalQualifier hour = new SqlIntervalQualifier(TimeUnit.HOUR, null, SqlParserPos.ZERO);
    private final SqlIntervalQualifier day = new SqlIntervalQualifier(TimeUnit.DAY, null, SqlParserPos.ZERO);
    private final SqlIntervalQualifier week = new SqlIntervalQualifier(TimeUnit.WEEK, null, SqlParserPos.ZERO);
    private final SqlIntervalQualifier month = new SqlIntervalQualifier(TimeUnit.MONTH, null, SqlParserPos.ZERO);
    private final SqlIntervalQualifier quarter = new SqlIntervalQualifier(TimeUnit.QUARTER, null, SqlParserPos.ZERO);
    private final SqlIntervalQualifier year = new SqlIntervalQualifier(TimeUnit.YEAR, null, SqlParserPos.ZERO);

    private RexNode x, y, z;
    private RexNode literalOne, literalTwo, literalThree;
    private final List<String> properties = Lists.newArrayList();

    @Before
    public void setUp() throws IOException {
        File tmpFile = File.createTempFile("RexToTblColRefTranslatorTest", "");
        FileUtils.deleteQuietly(tmpFile);
        FileUtils.forceMkdir(tmpFile);
        File metadata = new File(tmpFile.getAbsolutePath() + "/metadata");
        FileUtils.forceMkdir(metadata);

        File tempKylinProperties = new File(tmpFile, "kylin.properties");
        tmpFile.deleteOnExit();
        FileUtils.touch(tempKylinProperties);
        FileUtils.writeLines(tempKylinProperties, properties);

        //implicitly set KYLIN_CONF
        KylinConfig.setKylinConfigForLocalTest(tmpFile.getCanonicalPath());

        x = new RexInputRef(0, TYPE_FACTORY.createTypeWithNullability(boolRelDataType, true));
        y = new RexInputRef(1, TYPE_FACTORY.createTypeWithNullability(boolRelDataType, true));
        z = new RexInputRef(2, TYPE_FACTORY.createTypeWithNullability(boolRelDataType, true));

        literalOne = REX_BUILDER.makeLiteral("1");
        literalTwo = REX_BUILDER.makeLiteral("2");
        literalThree = REX_BUILDER.makeLiteral("3");
    }

    @After
    public void testDown() {
        boolRelDataType = null;
        timestampRelDataType = null;
        x = y = z = null;
    }

    private RexNode lessThan(RexNode a0, RexNode a1) {
        return REX_BUILDER.makeCall(SqlStdOperatorTable.LESS_THAN, a0, a1);
    }

    private RexNode greaterThan(RexNode a0, RexNode a1) {
        return REX_BUILDER.makeCall(SqlStdOperatorTable.GREATER_THAN, a0, a1);
    }

    @Test
    public void testTransformRexNode() {
        RexNode node1 = greaterThan(x, literalOne);
        RexNode node2 = lessThan(y, literalTwo);
        RexNode node3 = greaterThan(z, literalThree);

        // test flatten and
        RexNode structuredAND = REX_BUILDER.makeCall(SqlStdOperatorTable.AND,
                REX_BUILDER.makeCall(SqlStdOperatorTable.AND, node1, node2), node3);
        RexNode flattenAnd = RexUtil.flatten(REX_BUILDER, structuredAND);
        RexNode transformedAndRexNode = RexToTblColRefTranslator.createLeftCall(flattenAnd);
        Assert.assertEquals("AND(AND(>($0, '1'), <($1, '2')), >($2, '3'))", structuredAND.toString());
        Assert.assertEquals("AND(>($0, '1'), <($1, '2'), >($2, '3'))", flattenAnd.toString());
        Assert.assertEquals("AND(AND(>($0, '1'), <($1, '2')), >($2, '3'))", transformedAndRexNode.toString());

        // test flatten or
        RexNode structuredOR = REX_BUILDER.makeCall(SqlStdOperatorTable.OR,
                REX_BUILDER.makeCall(SqlStdOperatorTable.OR, node1, node2), node3);
        Assert.assertEquals("OR(OR(>($0, '1'), <($1, '2')), >($2, '3'))", structuredOR.toString());
        RexNode originOrRexNode = RexUtil.flatten(REX_BUILDER, structuredOR);
        RexNode transformedOrRexNode = RexToTblColRefTranslator.createLeftCall(originOrRexNode);
        Assert.assertEquals("OR(>($0, '1'), <($1, '2'), >($2, '3'))", originOrRexNode.toString());
        Assert.assertEquals("OR(OR(>($0, '1'), <($1, '2')), >($2, '3'))", transformedOrRexNode.toString());

        // test will not flatten case
        RexNode complex = REX_BUILDER.makeCall(SqlStdOperatorTable.OR,
                REX_BUILDER.makeCall(SqlStdOperatorTable.AND, node1, node2),
                REX_BUILDER.makeCall(SqlStdOperatorTable.AND, node2, node3));
        RexNode complexNotFlatten = RexUtil.flatten(REX_BUILDER, complex);
        RexNode transformedComplex = RexToTblColRefTranslator.createLeftCall(complexNotFlatten);
        String expected = "OR(AND(>($0, '1'), <($1, '2')), AND(<($1, '2'), >($2, '3')))";
        Assert.assertEquals(expected, complex.toString());
        Assert.assertEquals(expected, complexNotFlatten.toString());
        Assert.assertEquals(expected, transformedComplex.toString());
    }

    /**
     * verify timestampdiff(second, col1, col2)
     * RexNode is: /(Reinterpret(-($0, $1)), 1000)
     */
    @Test
    public void testTimestampDiffWithTimeUnitSecond() {
        Map<RexNode, TblColRef> oriRexToTblColRefMap = Maps.newHashMap();
        prepareRexToTblColRefOfTimestamp(oriRexToTblColRefMap);

        final RexNode innerNode = REX_BUILDER.makeReinterpretCast(bingIntRelDataType,
                REX_BUILDER.makeCall(TYPE_FACTORY.createSqlIntervalType(second), SqlStdOperatorTable.MINUS_DATE,
                        createStableRexNodes(oriRexToTblColRefMap)),
                x);
        final RexCall rexNode = (RexCall) REX_BUILDER.makeCall(bingIntRelDataType, SqlStdOperatorTable.DIVIDE_DATE,
                Lists.newArrayList(innerNode, REX_BUILDER.makeBigintLiteral(new BigDecimal(1000))));
        RexToSqlNodeConverter rexNodeToSqlConverter = new RexToTblColRefTranslator(
                Sets.newHashSet(oriRexToTblColRefMap.values()), oriRexToTblColRefMap).new ExtendedRexToSqlNodeConverter(
                        new RexToTblColRefTranslator.OlapRexSqlStdConvertletTable(rexNode, Maps.newHashMap()));
        check("TIMESTAMPDIFF(SECOND, `T_1_CED5FEB`.`TIME1`, `T_1_CED5FEB`.`TIME0`)",
                rexNodeToSqlConverter.convertCall(rexNode));
    }

    /**
     * verify timestampdiff(minute, col1, col2)
     * RexNode is: /(Reinterpret(-($0, $1)), '60000')
     */
    @Test
    public void testTimestampDiffWithTimeUnitMinute() {

        Map<RexNode, TblColRef> oriRexToTblColRefMap = Maps.newHashMap();
        prepareRexToTblColRefOfTimestamp(oriRexToTblColRefMap);

        final RexNode innerNode = REX_BUILDER.makeReinterpretCast(bingIntRelDataType,
                REX_BUILDER.makeCall(TYPE_FACTORY.createSqlIntervalType(minute), SqlStdOperatorTable.MINUS_DATE,
                        createStableRexNodes(oriRexToTblColRefMap)),
                x);
        final RexCall rexNode = (RexCall) REX_BUILDER.makeCall(bingIntRelDataType, SqlStdOperatorTable.DIVIDE_DATE,
                Lists.newArrayList(innerNode, REX_BUILDER.makeBigintLiteral(new BigDecimal(60000))));
        RexToSqlNodeConverter rexNodeToSqlConverter = new RexToTblColRefTranslator(
                Sets.newHashSet(oriRexToTblColRefMap.values()), oriRexToTblColRefMap).new ExtendedRexToSqlNodeConverter(
                        new RexToTblColRefTranslator.OlapRexSqlStdConvertletTable(rexNode, Maps.newHashMap()));
        check("TIMESTAMPDIFF(MINUTE, `T_1_CED5FEB`.`TIME1`, `T_1_CED5FEB`.`TIME0`)",
                rexNodeToSqlConverter.convertCall(rexNode));
    }

    /**
     * verify timestampdiff(hour, col1, col2)
     * RexNode is: /(Reinterpret(-($0, $1)), 3600000)
     */
    @Test
    public void testTimestampDiffWithTimeUnitHour() {

        Map<RexNode, TblColRef> oriRexToTblColRefMap = Maps.newHashMap();
        prepareRexToTblColRefOfTimestamp(oriRexToTblColRefMap);

        final RexNode innerNode = REX_BUILDER.makeReinterpretCast(bingIntRelDataType,
                REX_BUILDER.makeCall(TYPE_FACTORY.createSqlIntervalType(hour), SqlStdOperatorTable.MINUS_DATE,
                        createStableRexNodes(oriRexToTblColRefMap)),
                x);
        final RexCall rexNode = (RexCall) REX_BUILDER.makeCall(bingIntRelDataType, SqlStdOperatorTable.DIVIDE_DATE,
                Lists.newArrayList(innerNode, REX_BUILDER.makeBigintLiteral(new BigDecimal(3600000))));
        RexToSqlNodeConverter rexNodeToSqlConverter = new RexToTblColRefTranslator(
                Sets.newHashSet(oriRexToTblColRefMap.values()), oriRexToTblColRefMap).new ExtendedRexToSqlNodeConverter(
                        new RexToTblColRefTranslator.OlapRexSqlStdConvertletTable(rexNode, Maps.newHashMap()));
        check("TIMESTAMPDIFF(HOUR, `T_1_CED5FEB`.`TIME1`, `T_1_CED5FEB`.`TIME0`)",
                rexNodeToSqlConverter.convertCall(rexNode));
    }

    /**
     * verify timestampdiff(day, col1, col2)
     * RexNode is: /(Reinterpret(-($0, $1)), 86400000)
     */
    @Test
    public void testTimestampDiffWithTimeUnitDay() {

        Map<RexNode, TblColRef> oriRexToTblColRefMap = Maps.newHashMap();
        prepareRexToTblColRefOfTimestamp(oriRexToTblColRefMap);

        final RexNode innerNode = REX_BUILDER.makeReinterpretCast(bingIntRelDataType,
                REX_BUILDER.makeCall(TYPE_FACTORY.createSqlIntervalType(day), SqlStdOperatorTable.MINUS_DATE,
                        createStableRexNodes(oriRexToTblColRefMap)),
                x);
        final RexCall rexNode = (RexCall) REX_BUILDER.makeCall(bingIntRelDataType, SqlStdOperatorTable.DIVIDE_DATE,
                Lists.newArrayList(innerNode, REX_BUILDER.makeBigintLiteral(new BigDecimal(86400000))));
        RexToSqlNodeConverter rexNodeToSqlConverter = new RexToTblColRefTranslator(
                Sets.newHashSet(oriRexToTblColRefMap.values()), oriRexToTblColRefMap).new ExtendedRexToSqlNodeConverter(
                        new RexToTblColRefTranslator.OlapRexSqlStdConvertletTable(rexNode, Maps.newHashMap()));
        check("TIMESTAMPDIFF(DAY, `T_1_CED5FEB`.`TIME1`, `T_1_CED5FEB`.`TIME0`)",
                rexNodeToSqlConverter.convertCall(rexNode));
    }

    /**
     * verify timestampdiff(week, col1, col2)
     * RexNode is: /(/(Reinterpret(-($0, $1)), 1000), 604800)
     */
    @Test
    public void testTimestampDiffWithTimeUnitWeek() {

        Map<RexNode, TblColRef> oriRexToTblColRefMap = Maps.newHashMap();
        prepareRexToTblColRefOfTimestamp(oriRexToTblColRefMap);

        final RexNode innerNode = REX_BUILDER.makeReinterpretCast(bingIntRelDataType,
                REX_BUILDER.makeCall(TYPE_FACTORY.createSqlIntervalType(second), SqlStdOperatorTable.MINUS_DATE,
                        createStableRexNodes(oriRexToTblColRefMap)),
                x);
        final RexNode medianNode = REX_BUILDER.makeCall(bingIntRelDataType, SqlStdOperatorTable.DIVIDE_DATE,
                Lists.newArrayList(innerNode, REX_BUILDER.makeLiteral("1000")));
        final RexCall rexNode = (RexCall) REX_BUILDER.makeCall(bingIntRelDataType, SqlStdOperatorTable.DIVIDE_DATE,
                Lists.newArrayList(medianNode, REX_BUILDER.makeBigintLiteral(new BigDecimal(604800))));
        RexToSqlNodeConverter rexNodeToSqlConverter = new RexToTblColRefTranslator(
                Sets.newHashSet(oriRexToTblColRefMap.values()), oriRexToTblColRefMap).new ExtendedRexToSqlNodeConverter(
                        new RexToTblColRefTranslator.OlapRexSqlStdConvertletTable(rexNode, Maps.newHashMap()));
        check("TIMESTAMPDIFF(WEEK, `T_1_CED5FEB`.`TIME1`, `T_1_CED5FEB`.`TIME0`)",
                rexNodeToSqlConverter.convertCall(rexNode));
    }

    /**
     * verify timestampdiff(month, col1, col2)
     * RexNode is: Reinterpret(-($0, $1))
     */
    @Test
    public void testTimestampDiffWithTimeUnitMonth() {

        Map<RexNode, TblColRef> oriRexToTblColRefMap = Maps.newHashMap();
        prepareRexToTblColRefOfTimestamp(oriRexToTblColRefMap);

        final RexCall rexNode = (RexCall) REX_BUILDER.makeReinterpretCast(bingIntRelDataType,
                REX_BUILDER.makeCall(TYPE_FACTORY.createSqlIntervalType(month), SqlStdOperatorTable.MINUS_DATE,
                        createStableRexNodes(oriRexToTblColRefMap)),
                x);
        RexToSqlNodeConverter rexNodeToSqlConverter = new RexToTblColRefTranslator(
                Sets.newHashSet(oriRexToTblColRefMap.values()), oriRexToTblColRefMap).new ExtendedRexToSqlNodeConverter(
                        new RexToTblColRefTranslator.OlapRexSqlStdConvertletTable(rexNode, Maps.newHashMap()));
        check("TIMESTAMPDIFF(MONTH, `T_1_CED5FEB`.`TIME1`, `T_1_CED5FEB`.`TIME0`)",
                rexNodeToSqlConverter.convertCall(rexNode));
    }

    /**
     * verify timestampdiff(month, col1, col2)
     * RexNode is: /(Reinterpret(-($0, $1)), 3)
     */
    @Test
    public void testTimestampDiffWithTimeUnitQuarter() {

        Map<RexNode, TblColRef> oriRexToTblColRefMap = Maps.newHashMap();
        prepareRexToTblColRefOfTimestamp(oriRexToTblColRefMap);

        final RexNode innerNode = REX_BUILDER.makeReinterpretCast(bingIntRelDataType,
                REX_BUILDER.makeCall(TYPE_FACTORY.createSqlIntervalType(month), SqlStdOperatorTable.MINUS_DATE,
                        createStableRexNodes(oriRexToTblColRefMap)),
                x);
        final RexCall rexNode = (RexCall) REX_BUILDER.makeCall(bingIntRelDataType, SqlStdOperatorTable.DIVIDE_DATE,
                Lists.newArrayList(innerNode, REX_BUILDER.makeBigintLiteral(new BigDecimal(3))));
        RexToSqlNodeConverter rexNodeToSqlConverter = new RexToTblColRefTranslator(
                Sets.newHashSet(oriRexToTblColRefMap.values()), oriRexToTblColRefMap).new ExtendedRexToSqlNodeConverter(
                        new RexToTblColRefTranslator.OlapRexSqlStdConvertletTable(rexNode, Maps.newHashMap()));
        check("TIMESTAMPDIFF(QUARTER, `T_1_CED5FEB`.`TIME1`, `T_1_CED5FEB`.`TIME0`)",
                rexNodeToSqlConverter.convertCall(rexNode));
    }

    /**
     * verify timestampdiff(year, col1, col2)
     * RexNode is: /(Reinterpret(-($0, $1)), 12)
     */
    @Test
    public void testTimestampDiffWithTimeUnitYear() {

        Map<RexNode, TblColRef> oriRexToTblColRefMap = Maps.newHashMap();
        prepareRexToTblColRefOfTimestamp(oriRexToTblColRefMap);

        final RexNode innerNode = REX_BUILDER.makeReinterpretCast(bingIntRelDataType,
                REX_BUILDER.makeCall(TYPE_FACTORY.createSqlIntervalType(year), SqlStdOperatorTable.MINUS_DATE,
                        createStableRexNodes(oriRexToTblColRefMap)),
                x);
        final RexCall rexNode = (RexCall) REX_BUILDER.makeCall(bingIntRelDataType, SqlStdOperatorTable.DIVIDE_DATE,
                Lists.newArrayList(innerNode, REX_BUILDER.makeBigintLiteral(new BigDecimal(12))));
        RexToSqlNodeConverter rexNodeToSqlConverter = new RexToTblColRefTranslator(
                Sets.newHashSet(oriRexToTblColRefMap.values()), oriRexToTblColRefMap).new ExtendedRexToSqlNodeConverter(
                        new RexToTblColRefTranslator.OlapRexSqlStdConvertletTable(rexNode, Maps.newHashMap()));
        check("TIMESTAMPDIFF(YEAR, `T_1_CED5FEB`.`TIME1`, `T_1_CED5FEB`.`TIME0`)",
                rexNodeToSqlConverter.convertCall(rexNode));
    }

    /**
     * verify timestampadd(second, 1, col2)
     * RexNode is: DATETIME_PLUS($0, 1000)
     */
    @Test
    public void testTimestampAddWithTimeUnitSecond() {
        Map<RexNode, TblColRef> oriRexToTblColRefMap = Maps.newHashMap();
        prepareRexToTblColRefOfTimestamp(oriRexToTblColRefMap);
        final List<RexNode> stableRexNodes = createStableRexNodes(oriRexToTblColRefMap);

        final RexCall rexNode = (RexCall) REX_BUILDER.makeCall(timestampRelDataType, SqlStdOperatorTable.DATETIME_PLUS,
                Lists.newArrayList(stableRexNodes.get(0),
                        REX_BUILDER.makeIntervalLiteral(new BigDecimal(1000), second)));
        RexToSqlNodeConverter rexNodeToSqlConverter = new RexToTblColRefTranslator(
                Sets.newHashSet(oriRexToTblColRefMap.values()), oriRexToTblColRefMap).new ExtendedRexToSqlNodeConverter(
                        new RexToTblColRefTranslator.OlapRexSqlStdConvertletTable(rexNode, Maps.newHashMap()));
        check("TIMESTAMPADD(SECOND, 1, `T_1_CED5FEB`.`TIME0`)", rexNodeToSqlConverter.convertCall(rexNode));
    }

    /**
     * verify timestampadd(minute, 1, col2)
     * RexNode is: DATETIME_PLUS($0, 60000)
     */
    @Test
    public void testTimestampAddWithTimeUnitMinute() {
        Map<RexNode, TblColRef> oriRexToTblColRefMap = Maps.newHashMap();
        prepareRexToTblColRefOfTimestamp(oriRexToTblColRefMap);
        final List<RexNode> stableRexNodes = createStableRexNodes(oriRexToTblColRefMap);

        final RexCall rexNode = (RexCall) REX_BUILDER.makeCall(timestampRelDataType, SqlStdOperatorTable.DATETIME_PLUS,
                Lists.newArrayList(stableRexNodes.get(0),
                        REX_BUILDER.makeIntervalLiteral(new BigDecimal(60000), minute)));
        RexToSqlNodeConverter rexNodeToSqlConverter = new RexToTblColRefTranslator(
                Sets.newHashSet(oriRexToTblColRefMap.values()), oriRexToTblColRefMap).new ExtendedRexToSqlNodeConverter(
                        new RexToTblColRefTranslator.OlapRexSqlStdConvertletTable(rexNode, Maps.newHashMap()));
        check("TIMESTAMPADD(MINUTE, 1, `T_1_CED5FEB`.`TIME0`)", rexNodeToSqlConverter.convertCall(rexNode));
    }

    /**
     * verify timestampadd(hour, 1, col2)
     * RexNode is: DATETIME_PLUS($0, 3600000)
     */
    @Test
    public void testTimestampAddWithTimeUnitHour() {
        Map<RexNode, TblColRef> oriRexToTblColRefMap = Maps.newHashMap();
        prepareRexToTblColRefOfTimestamp(oriRexToTblColRefMap);
        final List<RexNode> stableRexNodes = createStableRexNodes(oriRexToTblColRefMap);

        final RexCall rexNode = (RexCall) REX_BUILDER.makeCall(timestampRelDataType, SqlStdOperatorTable.DATETIME_PLUS,
                Lists.newArrayList(stableRexNodes.get(0),
                        REX_BUILDER.makeIntervalLiteral(new BigDecimal(3600000), hour)));
        RexToSqlNodeConverter rexNodeToSqlConverter = new RexToTblColRefTranslator(
                Sets.newHashSet(oriRexToTblColRefMap.values()), oriRexToTblColRefMap).new ExtendedRexToSqlNodeConverter(
                        new RexToTblColRefTranslator.OlapRexSqlStdConvertletTable(rexNode, Maps.newHashMap()));
        check("TIMESTAMPADD(HOUR, 1, `T_1_CED5FEB`.`TIME0`)", rexNodeToSqlConverter.convertCall(rexNode));
    }

    /**
     * verify timestampadd(day, 1, col2)
     * RexNode is: DATETIME_PLUS($0, 86400000)
     */
    @Test
    public void testTimestampAddWithTimeUnitDay() {
        Map<RexNode, TblColRef> oriRexToTblColRefMap = Maps.newHashMap();
        prepareRexToTblColRefOfTimestamp(oriRexToTblColRefMap);
        final List<RexNode> stableRexNodes = createStableRexNodes(oriRexToTblColRefMap);

        final RexCall rexNode = (RexCall) REX_BUILDER.makeCall(timestampRelDataType, SqlStdOperatorTable.DATETIME_PLUS,
                Lists.newArrayList(stableRexNodes.get(0),
                        REX_BUILDER.makeIntervalLiteral(new BigDecimal(86400000), day)));
        RexToSqlNodeConverter rexNodeToSqlConverter = new RexToTblColRefTranslator(
                Sets.newHashSet(oriRexToTblColRefMap.values()), oriRexToTblColRefMap).new ExtendedRexToSqlNodeConverter(
                        new RexToTblColRefTranslator.OlapRexSqlStdConvertletTable(rexNode, Maps.newHashMap()));
        check("TIMESTAMPADD(DAY, 1, `T_1_CED5FEB`.`TIME0`)", rexNodeToSqlConverter.convertCall(rexNode));
    }

    /**
     * verify timestampadd(week, 1, col2)
     * RexNode is: DATETIME_PLUS($0, 604800000)
     */
    @Test
    public void testTimestampAddWithTimeUnitWeek() {
        Map<RexNode, TblColRef> oriRexToTblColRefMap = Maps.newHashMap();
        prepareRexToTblColRefOfTimestamp(oriRexToTblColRefMap);
        final List<RexNode> stableRexNodes = createStableRexNodes(oriRexToTblColRefMap);

        final RexCall rexNode = (RexCall) REX_BUILDER.makeCall(timestampRelDataType, SqlStdOperatorTable.DATETIME_PLUS,
                Lists.newArrayList(stableRexNodes.get(0),
                        REX_BUILDER.makeIntervalLiteral(new BigDecimal(604800000), week)));
        RexToSqlNodeConverter rexNodeToSqlConverter = new RexToTblColRefTranslator(
                Sets.newHashSet(oriRexToTblColRefMap.values()), oriRexToTblColRefMap).new ExtendedRexToSqlNodeConverter(
                        new RexToTblColRefTranslator.OlapRexSqlStdConvertletTable(rexNode, Maps.newHashMap()));
        check("TIMESTAMPADD(WEEK, 1, `T_1_CED5FEB`.`TIME0`)", rexNodeToSqlConverter.convertCall(rexNode));
    }

    /**
     * verify timestampadd(month, 1, col2)
     * RexNode is: DATETIME_PLUS($0, 1)
     */
    @Test
    public void testTimestampAddWithTimeUnitMonth() {
        Map<RexNode, TblColRef> oriRexToTblColRefMap = Maps.newHashMap();
        prepareRexToTblColRefOfTimestamp(oriRexToTblColRefMap);
        final List<RexNode> stableRexNodes = createStableRexNodes(oriRexToTblColRefMap);

        final RexCall rexNode = (RexCall) REX_BUILDER.makeCall(timestampRelDataType, SqlStdOperatorTable.DATETIME_PLUS,
                Lists.newArrayList(stableRexNodes.get(0), REX_BUILDER.makeIntervalLiteral(new BigDecimal(1), month)));
        RexToSqlNodeConverter rexNodeToSqlConverter = new RexToTblColRefTranslator(
                Sets.newHashSet(oriRexToTblColRefMap.values()), oriRexToTblColRefMap).new ExtendedRexToSqlNodeConverter(
                        new RexToTblColRefTranslator.OlapRexSqlStdConvertletTable(rexNode, Maps.newHashMap()));
        check("TIMESTAMPADD(MONTH, 1, `T_1_CED5FEB`.`TIME0`)", rexNodeToSqlConverter.convertCall(rexNode));
    }

    /**
     * verify timestampadd(quarter, 1, col2)
     * RexNode is: DATETIME_PLUS($0, 3)
     */
    @Test
    public void testTimestampAddWithTimeUnitQuarter() {
        Map<RexNode, TblColRef> oriRexToTblColRefMap = Maps.newHashMap();
        prepareRexToTblColRefOfTimestamp(oriRexToTblColRefMap);
        final List<RexNode> stableRexNodes = createStableRexNodes(oriRexToTblColRefMap);

        final RexCall rexNode = (RexCall) REX_BUILDER.makeCall(timestampRelDataType, SqlStdOperatorTable.DATETIME_PLUS,
                Lists.newArrayList(stableRexNodes.get(0), REX_BUILDER.makeIntervalLiteral(new BigDecimal(3), quarter)));
        RexToSqlNodeConverter rexNodeToSqlConverter = new RexToTblColRefTranslator(
                Sets.newHashSet(oriRexToTblColRefMap.values()), oriRexToTblColRefMap).new ExtendedRexToSqlNodeConverter(
                        new RexToTblColRefTranslator.OlapRexSqlStdConvertletTable(rexNode, Maps.newHashMap()));
        check("TIMESTAMPADD(QUARTER, 1, `T_1_CED5FEB`.`TIME0`)", rexNodeToSqlConverter.convertCall(rexNode));
    }

    /**
     * verify timestampadd(year, 1, col2)
     * RexNode is: DATETIME_PLUS($0, 12)
     */
    @Test
    public void testTimestampAddWithTimeUnitYear() {
        Map<RexNode, TblColRef> oriRexToTblColRefMap = Maps.newHashMap();
        prepareRexToTblColRefOfTimestamp(oriRexToTblColRefMap);
        final List<RexNode> stableRexNodes = createStableRexNodes(oriRexToTblColRefMap);

        final RexCall rexNode = (RexCall) REX_BUILDER.makeCall(timestampRelDataType, SqlStdOperatorTable.DATETIME_PLUS,
                Lists.newArrayList(stableRexNodes.get(0), REX_BUILDER.makeIntervalLiteral(new BigDecimal(12), year)));
        RexToSqlNodeConverter rexNodeToSqlConverter = new RexToTblColRefTranslator(
                Sets.newHashSet(oriRexToTblColRefMap.values()), oriRexToTblColRefMap).new ExtendedRexToSqlNodeConverter(
                        new RexToTblColRefTranslator.OlapRexSqlStdConvertletTable(rexNode, Maps.newHashMap()));
        check("TIMESTAMPADD(YEAR, 1, `T_1_CED5FEB`.`TIME0`)", rexNodeToSqlConverter.convertCall(rexNode));
    }

    /**
     * verify function: year(col1) or extract(year from col1)
     * RexNode is: EXTRACT(FLAG(YEAR), $0)
     */
    @Test
    public void testFunctionYear() {
        Map<RexNode, TblColRef> oriRexToTblColRefMap = Maps.newHashMap();
        prepareRexToTblColRefOfTimestamp(oriRexToTblColRefMap);
        final List<RexNode> stableRexNodes = createStableRexNodes(oriRexToTblColRefMap);

        final RexCall rexNode = (RexCall) REX_BUILDER.makeCall(bingIntRelDataType, SqlStdOperatorTable.EXTRACT,
                Lists.newArrayList(REX_BUILDER.makeFlag(TimeUnitRange.YEAR), stableRexNodes.get(0)));
        RexToSqlNodeConverter rexNodeToSqlConverter = new RexToTblColRefTranslator(
                Sets.newHashSet(oriRexToTblColRefMap.values()), oriRexToTblColRefMap).new ExtendedRexToSqlNodeConverter(
                        new RexToTblColRefTranslator.OlapRexSqlStdConvertletTable(rexNode, Maps.newHashMap()));
        check("YEAR(`T_1_CED5FEB`.`TIME0`)", rexNodeToSqlConverter.convertCall(rexNode));
    }

    /**
     * verify function: quarter(col1) or extract(quarter from col1)
     * RexNode is: EXTRACT(FLAG(QUARTER), $0)
     */
    @Test
    public void testFunctionQuarter() {
        Map<RexNode, TblColRef> oriRexToTblColRefMap = Maps.newHashMap();
        prepareRexToTblColRefOfTimestamp(oriRexToTblColRefMap);
        final List<RexNode> stableRexNodes = createStableRexNodes(oriRexToTblColRefMap);

        final RexCall rexNode = (RexCall) REX_BUILDER.makeCall(bingIntRelDataType, SqlStdOperatorTable.EXTRACT,
                Lists.newArrayList(REX_BUILDER.makeFlag(TimeUnitRange.QUARTER), stableRexNodes.get(0)));
        RexToSqlNodeConverter rexNodeToSqlConverter = new RexToTblColRefTranslator(
                Sets.newHashSet(oriRexToTblColRefMap.values()), oriRexToTblColRefMap).new ExtendedRexToSqlNodeConverter(
                        new RexToTblColRefTranslator.OlapRexSqlStdConvertletTable(rexNode, Maps.newHashMap()));
        check("QUARTER(`T_1_CED5FEB`.`TIME0`)", rexNodeToSqlConverter.convertCall(rexNode));
    }

    /**
     * verify function: month(col1) or extract(month from col1)
     * RexNode is: EXTRACT(FLAG(MONTH), $0)
     */
    @Test
    public void testFunctionMonth() {
        Map<RexNode, TblColRef> oriRexToTblColRefMap = Maps.newHashMap();
        prepareRexToTblColRefOfTimestamp(oriRexToTblColRefMap);
        final List<RexNode> stableRexNodes = createStableRexNodes(oriRexToTblColRefMap);

        final RexCall rexNode = (RexCall) REX_BUILDER.makeCall(bingIntRelDataType, SqlStdOperatorTable.EXTRACT,
                Lists.newArrayList(REX_BUILDER.makeFlag(TimeUnitRange.MONTH), stableRexNodes.get(0)));
        RexToSqlNodeConverter rexNodeToSqlConverter = new RexToTblColRefTranslator(
                Sets.newHashSet(oriRexToTblColRefMap.values()), oriRexToTblColRefMap).new ExtendedRexToSqlNodeConverter(
                        new RexToTblColRefTranslator.OlapRexSqlStdConvertletTable(rexNode, Maps.newHashMap()));
        check("MONTH(`T_1_CED5FEB`.`TIME0`)", rexNodeToSqlConverter.convertCall(rexNode));
    }

    /**
     * verify function: week(col1) or extract(week from col1)
     * RexNode is: EXTRACT(FLAG(WEEK), $0)
     */
    @Test
    public void testFunctionWeek() {
        Map<RexNode, TblColRef> oriRexToTblColRefMap = Maps.newHashMap();
        prepareRexToTblColRefOfTimestamp(oriRexToTblColRefMap);
        final List<RexNode> stableRexNodes = createStableRexNodes(oriRexToTblColRefMap);

        final RexCall rexNode = (RexCall) REX_BUILDER.makeCall(bingIntRelDataType, SqlStdOperatorTable.EXTRACT,
                Lists.newArrayList(REX_BUILDER.makeFlag(TimeUnitRange.WEEK), stableRexNodes.get(0)));
        RexToSqlNodeConverter rexNodeToSqlConverter = new RexToTblColRefTranslator(
                Sets.newHashSet(oriRexToTblColRefMap.values()), oriRexToTblColRefMap).new ExtendedRexToSqlNodeConverter(
                        new RexToTblColRefTranslator.OlapRexSqlStdConvertletTable(rexNode, Maps.newHashMap()));
        check("WEEK(`T_1_CED5FEB`.`TIME0`)", rexNodeToSqlConverter.convertCall(rexNode));
    }

    /**
     * verify function: dayofmonth(col1) or extract(day from col1)
     * RexNode is: EXTRACT(FLAG(DAY), $0)
     */
    @Test
    public void testFunctionDayOfMonth() {
        Map<RexNode, TblColRef> oriRexToTblColRefMap = Maps.newHashMap();
        prepareRexToTblColRefOfTimestamp(oriRexToTblColRefMap);
        final List<RexNode> stableRexNodes = createStableRexNodes(oriRexToTblColRefMap);

        final RexCall rexNode = (RexCall) REX_BUILDER.makeCall(bingIntRelDataType, SqlStdOperatorTable.EXTRACT,
                Lists.newArrayList(REX_BUILDER.makeFlag(TimeUnitRange.DAY), stableRexNodes.get(0)));
        RexToSqlNodeConverter rexNodeToSqlConverter = new RexToTblColRefTranslator(
                Sets.newHashSet(oriRexToTblColRefMap.values()), oriRexToTblColRefMap).new ExtendedRexToSqlNodeConverter(
                        new RexToTblColRefTranslator.OlapRexSqlStdConvertletTable(rexNode, Maps.newHashMap()));
        check("DAYOFMONTH(`T_1_CED5FEB`.`TIME0`)", rexNodeToSqlConverter.convertCall(rexNode));
    }

    /**
     * verify function: hour(col1) or extract(hour from col1)
     * RexNode is: EXTRACT(FLAG(HOUR), $0)
     */
    @Test
    public void testFunctionHour() {
        Map<RexNode, TblColRef> oriRexToTblColRefMap = Maps.newHashMap();
        prepareRexToTblColRefOfTimestamp(oriRexToTblColRefMap);
        final List<RexNode> stableRexNodes = createStableRexNodes(oriRexToTblColRefMap);

        final RexCall rexNode = (RexCall) REX_BUILDER.makeCall(bingIntRelDataType, SqlStdOperatorTable.EXTRACT,
                Lists.newArrayList(REX_BUILDER.makeFlag(TimeUnitRange.HOUR), stableRexNodes.get(0)));
        RexToSqlNodeConverter rexNodeToSqlConverter = new RexToTblColRefTranslator(
                Sets.newHashSet(oriRexToTblColRefMap.values()), oriRexToTblColRefMap).new ExtendedRexToSqlNodeConverter(
                        new RexToTblColRefTranslator.OlapRexSqlStdConvertletTable(rexNode, Maps.newHashMap()));
        check("HOUR(`T_1_CED5FEB`.`TIME0`)", rexNodeToSqlConverter.convertCall(rexNode));
    }

    /**
     * verify function: minute(col1) or extract(minute from col1)
     * RexNode is: EXTRACT(FLAG(MINUTE), $0)
     */
    @Test
    public void testFunctionMinute() {
        Map<RexNode, TblColRef> oriRexToTblColRefMap = Maps.newHashMap();
        prepareRexToTblColRefOfTimestamp(oriRexToTblColRefMap);
        final List<RexNode> stableRexNodes = createStableRexNodes(oriRexToTblColRefMap);

        final RexCall rexNode = (RexCall) REX_BUILDER.makeCall(bingIntRelDataType, SqlStdOperatorTable.EXTRACT,
                Lists.newArrayList(REX_BUILDER.makeFlag(TimeUnitRange.MINUTE), stableRexNodes.get(0)));
        RexToSqlNodeConverter rexNodeToSqlConverter = new RexToTblColRefTranslator(
                Sets.newHashSet(oriRexToTblColRefMap.values()), oriRexToTblColRefMap).new ExtendedRexToSqlNodeConverter(
                        new RexToTblColRefTranslator.OlapRexSqlStdConvertletTable(rexNode, Maps.newHashMap()));
        check("MINUTE(`T_1_CED5FEB`.`TIME0`)", rexNodeToSqlConverter.convertCall(rexNode));
    }

    /**
     * verify function: second(col1) or extract(second from col1)
     * RexNode is: EXTRACT(FLAG(SECOND), $0)
     */
    @Test
    public void testFunctionSecond() {
        Map<RexNode, TblColRef> oriRexToTblColRefMap = Maps.newHashMap();
        prepareRexToTblColRefOfTimestamp(oriRexToTblColRefMap);
        final List<RexNode> stableRexNodes = createStableRexNodes(oriRexToTblColRefMap);

        final RexCall rexNode = (RexCall) REX_BUILDER.makeCall(bingIntRelDataType, SqlStdOperatorTable.EXTRACT,
                Lists.newArrayList(REX_BUILDER.makeFlag(TimeUnitRange.SECOND), stableRexNodes.get(0)));
        RexToSqlNodeConverter rexNodeToSqlConverter = new RexToTblColRefTranslator(
                Sets.newHashSet(oriRexToTblColRefMap.values()), oriRexToTblColRefMap).new ExtendedRexToSqlNodeConverter(
                        new RexToTblColRefTranslator.OlapRexSqlStdConvertletTable(rexNode, Maps.newHashMap()));
        check("SECOND(`T_1_CED5FEB`.`TIME0`)", rexNodeToSqlConverter.convertCall(rexNode));
    }

    /**
     * verify function: dayofyear(col1) only, for spark doesn't support extract(doy from col1)
     * RexNode is: EXTRACT(FLAG(DOY), $0)
     */
    @Test
    public void testFunctionOfDayOfYear() {
        Map<RexNode, TblColRef> oriRexToTblColRefMap = Maps.newHashMap();
        prepareRexToTblColRefOfTimestamp(oriRexToTblColRefMap);
        final List<RexNode> stableRexNodes = createStableRexNodes(oriRexToTblColRefMap);

        final RexCall rexNode = (RexCall) REX_BUILDER.makeCall(bingIntRelDataType, SqlStdOperatorTable.EXTRACT,
                Lists.newArrayList(REX_BUILDER.makeFlag(TimeUnitRange.DOY), stableRexNodes.get(0)));
        RexToSqlNodeConverter rexNodeToSqlConverter = new RexToTblColRefTranslator(
                Sets.newHashSet(oriRexToTblColRefMap.values()), oriRexToTblColRefMap).new ExtendedRexToSqlNodeConverter(
                        new RexToTblColRefTranslator.OlapRexSqlStdConvertletTable(rexNode, Maps.newHashMap()));
        check("DAYOFYEAR(`T_1_CED5FEB`.`TIME0`)", rexNodeToSqlConverter.convertCall(rexNode));
    }

    /**
     * verify function: dayofweek(col1) only, for spark doesn't support extract(dow from col1)
     * RexNode is: EXTRACT(FLAG(DOW), $0)
     */
    @Test
    public void testFunctionDayOfWeek() {
        Map<RexNode, TblColRef> oriRexToTblColRefMap = Maps.newHashMap();
        prepareRexToTblColRefOfTimestamp(oriRexToTblColRefMap);
        final List<RexNode> stableRexNodes = createStableRexNodes(oriRexToTblColRefMap);

        final RexCall rexNode = (RexCall) REX_BUILDER.makeCall(bingIntRelDataType, SqlStdOperatorTable.EXTRACT,
                Lists.newArrayList(REX_BUILDER.makeFlag(TimeUnitRange.DOW), stableRexNodes.get(0)));
        RexToSqlNodeConverter rexNodeToSqlConverter = new RexToTblColRefTranslator(
                Sets.newHashSet(oriRexToTblColRefMap.values()), oriRexToTblColRefMap).new ExtendedRexToSqlNodeConverter(
                        new RexToTblColRefTranslator.OlapRexSqlStdConvertletTable(rexNode, Maps.newHashMap()));
        check("DAYOFWEEK(`T_1_CED5FEB`.`TIME0`)", rexNodeToSqlConverter.convertCall(rexNode));
    }

    private void check(String expectedStr, SqlNode sqlNode) {
        Assert.assertEquals(expectedStr, sqlNode.toString());
    }

    private List<RexNode> createStableRexNodes(Map<RexNode, TblColRef> oriRexToTblColRefMap) {
        final List<RexNode> rexNodes = Lists.newArrayList(oriRexToTblColRefMap.keySet());
        rexNodes.sort(Comparator.comparing(RexNode::toString));
        return rexNodes;
    }

    private void prepareRexToTblColRefOfTimestamp(Map<RexNode, TblColRef> rexNodeTblColRefMap) {
        ColumnRowType columnRowType = ColumnRowTypeMockUtil.mock("CALCS", "T_1_CED5FEB",
                ImmutableList.of(Pair.newPair("TIME0", "timestamp"), //
                        Pair.newPair("TIME1", "timestamp")));
        IntStream.range(0, columnRowType.getAllColumns().size()).forEach(i -> {
            RexNode key = new RexInputRef(i, TYPE_FACTORY.createTypeWithNullability(timestampRelDataType, true));
            rexNodeTblColRefMap.put(key, columnRowType.getAllColumns().get(i));
        });
    }
}
