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
import { editTypes } from '../handler'

export const fieldVisiableMaps = {
  [editTypes.CONFIG_SOURCE]: ['name', 'creator', 'description', 'host', 'port', 'isAuthentication', 'username', 'password'],
  [editTypes.VIEW_SOURCE]: ['type', 'name', 'host', 'port']
}

export const validate = {
  name (rule, value, callback) {
    if (!value) {
      callback(new Error(this.$t('kylinLang.common.pleaseInput')))
    } else if (!/^\w+$/.test(value)) {
      callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
    } else {
      callback()
    }
  },
  host (rule, value, callback) {
    if (!value) {
      callback(new Error(this.$t('kylinLang.common.pleaseInput')))
    } else if (!/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/.test(value)) {
      callback(new Error(this.$t('hostValidTip')))
    } else {
      callback()
    }
  },
  port (rule, value, callback) {
    if (!value) {
      callback(new Error(this.$t('kylinLang.common.pleaseInput')))
    } else if (!/^[0-9]+$/.test(value)) {
      callback(new Error(this.$t('portValidTip')))
    } else {
      callback()
    }
  },
  username (rule, value, callback) {
    if (!this.form.isAuthentication) {
      callback()
    } else if (!value) {
      callback(new Error(this.$t('kylinLang.common.pleaseInput')))
    } else if (!/^\w+$/.test(value)) {
      callback(new Error(this.$t('kylinLang.common.nameFormatValidTip')))
    } else {
      callback()
    }
  },
  password (rule, value, callback) {
    if (!this.form.isAuthentication) {
      callback()
    } else if (!value) {
      callback(new Error(this.$t('kylinLang.common.pleaseInput')))
    } else {
      callback()
    }
  }
}
