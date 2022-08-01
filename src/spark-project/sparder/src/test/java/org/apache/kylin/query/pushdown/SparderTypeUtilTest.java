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

package org.apache.kylin.query.pushdown;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.spark.sql.util.SparderTypeUtil;
import org.junit.Assert;
import org.junit.Test;

import scala.Tuple2;
import scala.collection.immutable.HashMap;
import scala.collection.mutable.WrappedArray;

public class SparderTypeUtilTest {

    private RelDataType baiscSqlType(SqlTypeName typeName) {
        return new BasicSqlType(RelDataTypeSystem.DEFAULT, typeName);
    }

    private String convertToStringWithCalciteType(Object value, SqlTypeName typeName) {
        return SparderTypeUtil.convertToStringWithCalciteType(value, baiscSqlType(typeName), false);
    }

    private String convertToStringWithDecimalType(Object value, int precision, int scale) {
        return SparderTypeUtil.convertToStringWithCalciteType(value,
                new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DECIMAL, precision, scale), false);
    }

    @Test
    public void testConvertToStringWithCalciteType() {
        // matched types
        Assert.assertEquals("9", convertToStringWithCalciteType(Integer.parseInt("9"), SqlTypeName.INTEGER));
        Assert.assertEquals("9", convertToStringWithCalciteType(Short.parseShort("9"), SqlTypeName.SMALLINT));
        Assert.assertEquals("9", convertToStringWithCalciteType(Byte.parseByte("9"), SqlTypeName.TINYINT));
        Assert.assertEquals("9", convertToStringWithCalciteType(Long.parseLong("9"), SqlTypeName.BIGINT));
        Assert.assertEquals("123.345", convertToStringWithDecimalType(new BigDecimal("123.345"), 29, 3));
        Assert.assertEquals("2012-01-01",
                convertToStringWithCalciteType(java.sql.Date.valueOf("2012-01-01"), SqlTypeName.DATE));
        Assert.assertEquals("2012-01-01 12:34:56", convertToStringWithCalciteType(
                java.sql.Timestamp.valueOf("2012-01-01 12:34:56"), SqlTypeName.TIMESTAMP));
        Assert.assertEquals("2012-01-01 12:34:45.01", convertToStringWithCalciteType(
                java.sql.Timestamp.valueOf("2012-01-01 12:34:45.01"), SqlTypeName.TIMESTAMP));
        Assert.assertEquals("2012-01-01 12:34:45.01", convertToStringWithCalciteType(
                java.sql.Timestamp.valueOf("2012-01-01 12:34:45.010"), SqlTypeName.TIMESTAMP));
        Assert.assertEquals("2012-01-01 12:34:45.01", convertToStringWithCalciteType(
                java.sql.Timestamp.valueOf("2012-01-01 12:34:45.010100"), SqlTypeName.TIMESTAMP));
        Assert.assertEquals("2012-01-01 12:34:45.1", convertToStringWithCalciteType(
                java.sql.Timestamp.valueOf("2012-01-01 12:34:45.1"), SqlTypeName.TIMESTAMP));
        Assert.assertEquals("2012-01-01 12:34:45", convertToStringWithCalciteType(
                java.sql.Timestamp.valueOf("2012-01-01 12:34:45.000101"), SqlTypeName.TIMESTAMP));
        Assert.assertEquals("2012-01-01 12:34:45", convertToStringWithCalciteType(
                java.sql.Timestamp.valueOf("2012-01-01 12:34:45.0"), SqlTypeName.TIMESTAMP));
        // cast to char/varchar
        Assert.assertEquals("foo", convertToStringWithCalciteType("foo", SqlTypeName.VARCHAR));
        Assert.assertEquals("foo", convertToStringWithCalciteType("foo", SqlTypeName.CHAR));
        Assert.assertEquals("123.345", convertToStringWithCalciteType(new BigDecimal("123.345"), SqlTypeName.VARCHAR));
        Assert.assertEquals("2012-01-01",
                convertToStringWithCalciteType(java.sql.Date.valueOf("2012-01-01"), SqlTypeName.VARCHAR));
        Assert.assertEquals("2012-01-01 12:34:56",
                convertToStringWithCalciteType(java.sql.Timestamp.valueOf("2012-01-01 12:34:56"), SqlTypeName.VARCHAR));
        // type conversion
        Assert.assertEquals("9", convertToStringWithCalciteType(Float.parseFloat("9.1"), SqlTypeName.INTEGER));
        Assert.assertEquals("9", convertToStringWithCalciteType(Double.parseDouble("9.1"), SqlTypeName.TINYINT));
        Assert.assertEquals("9", convertToStringWithCalciteType("9.1", SqlTypeName.SMALLINT));
        Assert.assertEquals("9", convertToStringWithCalciteType(new BigDecimal("9.1"), SqlTypeName.BIGINT));
        Assert.assertEquals("9", convertToStringWithDecimalType(Long.parseLong("9"), 18, 0));
        Assert.assertEquals("9.123", convertToStringWithDecimalType(Double.parseDouble("9.123"), 18, 3));
        Assert.assertEquals("9.1", convertToStringWithDecimalType(Double.parseDouble("9.123"), 18, 1));
        Assert.assertEquals("2012-01-01 00:00:00",
                convertToStringWithCalciteType(java.sql.Date.valueOf("2012-01-01"), SqlTypeName.TIMESTAMP));
        Assert.assertEquals("2012-01-01",
                convertToStringWithCalciteType(java.sql.Timestamp.valueOf("2012-01-01 12:34:56"), SqlTypeName.DATE));
        // in case type is not set
        Assert.assertEquals("9.1", convertToStringWithCalciteType(Float.parseFloat("9.1"), SqlTypeName.ANY));
        Assert.assertEquals("2012-01-01 12:34:56",
                convertToStringWithCalciteType(java.sql.Timestamp.valueOf("2012-01-01 12:34:56"), SqlTypeName.ANY));
        Assert.assertEquals("2012-01-01",
                convertToStringWithCalciteType(java.sql.Date.valueOf("2012-01-01"), SqlTypeName.ANY));
        // array, map
        Assert.assertEquals("[\"a\",\"b\"]",
                convertToStringWithCalciteType(WrappedArray.make(new String[] { "a", "b" }), SqlTypeName.ANY));
        Assert.assertEquals("[1,null,2]",
                convertToStringWithCalciteType(WrappedArray.make(new Integer[] { 1, null, 2 }), SqlTypeName.ANY));
        HashMap<String, Integer> map = new HashMap<>();
        map = map.$plus(new Tuple2<>("foo", 123));
        map = map.$plus(new Tuple2<>("bar", null));
        Assert.assertEquals("{\"bar\":null,\"foo\":123}", convertToStringWithCalciteType(map, SqlTypeName.ANY));
    }

    @Test
    public void testEmpty() {
        JavaTypeFactoryImpl tp = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        assert (SparderTypeUtil.convertStringToValue("", tp.createType(BigDecimal.class), false)
                .equals(new BigDecimal(0)));
        assert (SparderTypeUtil.convertStringToValue("", tp.createType(String.class), false).equals(""));
        assert (SparderTypeUtil.convertStringToValue("", tp.createType(char.class), false).equals(""));
        assert (SparderTypeUtil.convertStringToValue("", tp.createType(short.class), false).equals((short) 0));
        assert (SparderTypeUtil.convertStringToValue("", tp.createType(int.class), false).equals(0));
        assert (SparderTypeUtil.convertStringToValue("", tp.createType(long.class), false).equals(0L));
        assert (SparderTypeUtil.convertStringToValue("", tp.createType(double.class), false).equals(0D));
        assert (SparderTypeUtil.convertStringToValue("", tp.createType(float.class), false).equals(0F));
        assert (SparderTypeUtil.convertStringToValue("", tp.createType(boolean.class), false) == null);
        assert (SparderTypeUtil.convertStringToValue("", tp.createType(Date.class), false).equals(0));
        assert (SparderTypeUtil.convertStringToValue("", tp.createType(Time.class), false).equals(0L));
        assert (SparderTypeUtil.convertStringToValue("", tp.createType(Timestamp.class), false).equals(0L));
    }

    @Test
    public void testUnMatchType() {
        JavaTypeFactoryImpl tp = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        assert (SparderTypeUtil.convertStringToValue("", tp.createType(BigDecimal.class), false)
                .equals(new BigDecimal(0)));
        Double value = 0.604951272091475354;
        Object convert_value = SparderTypeUtil.convertStringToValue(value, tp.createType(long.class), false);
        Assert.assertEquals(Long.class, convert_value.getClass());
        Assert.assertEquals(0L, convert_value);
    }
}
