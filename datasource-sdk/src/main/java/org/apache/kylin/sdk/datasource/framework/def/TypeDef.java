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
package org.apache.kylin.sdk.datasource.framework.def;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

public class TypeDef {
    private static final Pattern P = Pattern
            .compile("\\s*([^\\(\\)]+)\\s*(?:\\(\\s*([^\\s,]+)(?:\\s*,\\s*([^\\s,]+)\\s*)?\\))?\\s*");

    @JacksonXmlProperty(localName = "ID", isAttribute = true)
    private String id;

    @JacksonXmlProperty(localName = "EXPRESSION", isAttribute = true)
    private String expression;

    @JacksonXmlProperty(localName = "MAXPRECISION", isAttribute = true)
    private String maxPrecision;

    // computed fields
    private String name;
    private int defaultPrecision;
    private int defaultScale;

    void init() {
        id = id.toUpperCase(Locale.ROOT);

        Matcher m = P.matcher(expression);
        if (m.matches()) {
            name = m.group(1).toUpperCase(Locale.ROOT);
            Integer p = m.group(2) != null ? Ints.tryParse(m.group(2)) : null;
            Integer s = m.group(3) != null ? Ints.tryParse(m.group(3)) : null;
            defaultPrecision = p != null ? p : -1;
            defaultScale = s != null ? s : -1;
        }
    }

    @VisibleForTesting
    void setExpression(String expression) {
        this.expression = expression;
    }

    @VisibleForTesting
    void setId(String id) {
        this.id = id;
    }

    TypeDef() {
    }

    public String getId() {
        return id;
    }

    public String getExpression() {
        return expression;
    }

    public int getMaxPrecision() {
        try {
            return Integer.parseInt(maxPrecision);
        } catch (Exception e) {
            return Integer.MAX_VALUE;
        }
    }

    public String getName() {
        return name;
    }

    public int getDefaultPrecision() {
        return defaultPrecision;
    }

    public int getDefaultScale() {
        return defaultScale;
    }

    public String buildString(int p, int s) {
        return expression.replaceAll("\\$p", Integer.toString(p)).replaceAll("\\$s", Integer.toString(s));
    }

    public static TypeDef fromString(String kylinTypeName) {
        TypeDef t = new TypeDef();
        t.setId("UNKNOWN");
        t.setExpression(kylinTypeName);
        t.init();
        return t;
    }
}
