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

package org.apache.kylin.query.udf;

import java.util.List;

import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.linq4j.function.Parameter;
import org.apache.calcite.sql.type.NotConstant;
import org.apache.kylin.common.exception.CalciteNotSupportException;

public class SparkStringUDF implements NotConstant {

    public Integer ASCII(@Parameter(name = "str") String str) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String BASE64(@Parameter(name = "str1") String exp1) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String BTRIM(@Parameter(name = "str1") String exp1, @Parameter(name = "str2") String exp2)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String CHAR(@Parameter(name = "str") Long str) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String CHR(@Parameter(name = "str") Long str) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Integer CHAR_LENGTH(@Parameter(name = "str") String str) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Integer CHAR_LENGTH(@Parameter(name = "str") byte[] str) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Integer CHARACTER_LENGTH(@Parameter(name = "str") String str) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Integer CHARACTER_LENGTH(@Parameter(name = "str") byte[] str) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String CONCAT_WS(@Parameter(name = "str") String sep, @Parameter(name = "arr") ColumnMetaData.ArrayType arr)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String DECODE(@Parameter(name = "str1") byte[] exp1, @Parameter(name = "str2") Object exp2)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public byte[] ENCODE(@Parameter(name = "str1") String exp1, @Parameter(name = "str2") String exp2)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Integer FIND_IN_SET(@Parameter(name = "str1") String exp1, @Parameter(name = "str2") String exp2)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String INITCAPB(@Parameter(name = "exp") Object exp) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String LCASE(@Parameter(name = "str1") String exp1) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String LEFT(@Parameter(name = "str") String s, @Parameter(name = "length") int len)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Integer LEVENSHTEIN(@Parameter(name = "str1") String exp1, @Parameter(name = "str2") String exp2)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Integer LOCATE(@Parameter(name = "str1") String exp1, @Parameter(name = "str2") String exp2,
            @Parameter(name = "num3") Integer exp3) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Integer LOCATE(@Parameter(name = "str1") String exp1, @Parameter(name = "str2") String exp2)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String LOWER(@Parameter(name = "str1") String exp1) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String LPAD(@Parameter(name = "str1") String exp1, @Parameter(name = "num2") Object exp2,
            @Parameter(name = "str3") String exp3) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String LPAD(@Parameter(name = "str1") String exp1, @Parameter(name = "num2") Object exp2)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Integer LENGTH(@Parameter(name = "str") String str) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Integer LENGTH(@Parameter(name = "str") byte[] str) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String LTRIM(@Parameter(name = "str") String str) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String LTRIM(@Parameter(name = "str1") String exp1, @Parameter(name = "str2") String exp2)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String REPEAT(@Parameter(name = "str") String str, @Parameter(name = "n") Integer n)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String REPLACE(@Parameter(name = "str1") String exp1, @Parameter(name = "str2") String exp2,
            @Parameter(name = "str3") String exp3) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String RIGHT(@Parameter(name = "str") String s, @Parameter(name = "length") int len)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Boolean RLIKE(@Parameter(name = "str") String str, @Parameter(name = "regexp") String regexp)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public Boolean REGEXP_LIKE(@Parameter(name = "str") String str, @Parameter(name = "regexp") String regexp)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String REGEXP_EXTRACT(@Parameter(name = "str") String str, @Parameter(name = "regexp") String regexp,
            @Parameter(name = "idx") Integer idx) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String RPAD(@Parameter(name = "str1") String exp1, @Parameter(name = "num2") Object exp2,
            @Parameter(name = "str3") String exp3) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String RPAD(@Parameter(name = "str1") String exp1, @Parameter(name = "num2") Object exp2)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String RTRIM(@Parameter(name = "str1") String exp1) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String RTRIM(@Parameter(name = "str1") String exp1, @Parameter(name = "str2") String exp2)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String[] SENTENCES(@Parameter(name = "str1") String exp1) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String SPACE(@Parameter(name = "n") Object n) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public List<String> SPLIT(@Parameter(name = "str") Object str, @Parameter(name = "regex") Object regex)
            throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public List<String> SPLIT(@Parameter(name = "str") Object str, @Parameter(name = "regex") Object regex,
            @Parameter(name = "limit") Object limit) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String SUBSTRING_INDEX(@Parameter(name = "str1") String exp1, @Parameter(name = "str2") String exp2,
            @Parameter(name = "num2") Integer exp3) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String TRANSLATE(@Parameter(name = "str1") String exp1, @Parameter(name = "str2") String exp2,
            @Parameter(name = "str3") String exp3) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String TRIM(@Parameter(name = "str1") String exp1) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String UCASE(@Parameter(name = "str1") String exp1) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public String UPPER(@Parameter(name = "str1") String exp1) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }

    public byte[] UNBASE64(@Parameter(name = "str1") String exp1) throws CalciteNotSupportException {
        throw new CalciteNotSupportException();
    }
}
