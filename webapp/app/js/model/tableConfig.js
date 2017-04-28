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

KylinApp.constant('tableConfig', {
  theaditems: [
    {attr: 'id', name: 'ID'},
    {attr: 'name', name: 'Name'},
    {attr: 'datatype', name: 'Data Type'},
    {attr: 'cardinality', name: 'Cardinality'},
    {attr: 'comment', name: 'Comment'}
  ],
  dataTypes:["tinyint","smallint","int","bigint","float","double","decimal","timestamp","date","string","varchar(256)","char","boolean","binary"],
  columnTypeEncodingMap:{
    "numeric": [
      "dict"
    ],
    "bigint": [
      "boolean",
      "date",
      "time",
      "dict",
      "integer"
    ],
    "char": [
      "boolean",
      "date",
      "time",
      "dict",
      "fixed_length",
      "fixed_length_hex",
      "integer"
    ],
    "integer": [
      "boolean",
      "date",
      "time",
      "dict",
      "integer"
    ],
    "int4": [
      "boolean",
      "date",
      "time",
      "dict",
      "integer"
    ],
    "tinyint": [
      "boolean",
      "date",
      "time",
      "dict",
      "integer"
    ],
    "double": [
      "dict"
    ],
    "date": [
      "date",
      "time",
      "dict"
    ],
    "float": [
      "dict"
    ],
    "decimal": [
      "dict"
    ],
    "timestamp": [
      "date",
      "time",
      "dict"
    ],
    "real": [
      "dict"
    ],
    "time": [
      "date",
      "time",
      "dict"
    ],
    "long8": [
      "boolean",
      "date",
      "time",
      "dict",
      "integer"
    ],
    "datetime": [
      "date",
      "time",
      "dict"
    ],
    "smallint": [
      "boolean",
      "date",
      "time",
      "dict",
      "integer"
    ],
    "varchar": [
      "boolean",
      "date",
      "time",
      "dict",
      "fixed_length",
      "fixed_length_hex",
      "integer"
    ]
  }


});
