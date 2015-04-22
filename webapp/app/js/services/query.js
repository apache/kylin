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

KylinApp.factory('QueryService', ['$resource', function ($resource, config) {
    return $resource(Config.service.url + ':subject/:subject_id/:propName/:propValue/:action', {}, {
        query: {method: 'POST', params: {action: 'query'},timeout:300000, isArray: false},
        save: {method: 'POST', params: {subject: 'saved_queries'}, isArray: false},
        delete: {method: 'DELETE', params: {subject: 'saved_queries'}, isArray: false},
        list: {method: 'GET', params: {subject: 'saved_queries'}, isArray: true},
        export: {method: 'GET', params: {subject: 'query', propName: 'format', propValue: 'csv'}, isArray: false},
        getTables: {method: 'GET', params: {subject: 'tables_and_columns'}, isArray: true}
    });
}])
;
