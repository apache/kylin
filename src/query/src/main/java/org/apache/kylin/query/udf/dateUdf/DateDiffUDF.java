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

package org.apache.kylin.query.udf.dateUdf;

import java.sql.Date;

import org.apache.calcite.linq4j.function.Parameter;

public class DateDiffUDF {

    public Long dateDiff(@Parameter(name = "date") Date date1, @Parameter(name = "date") Date date2) {
        return (date1.getTime() - date2.getTime()) / (1000 * 60 * 60 * 24);
    }

    public Long dateDiff(@Parameter(name = "date") String date1, @Parameter(name = "date") String date2){
        return (Date.valueOf(date1).getTime() - Date.valueOf(date2).getTime()) / (1000 * 60 * 60 * 24);
    }
}
