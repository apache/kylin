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

import { createLocalVue } from '@vue/test-utils'
import VueI18n from 'vue-i18n'
import enKylinLocale from '../../src/locale/en'
import Vuex from 'vuex'
import KyligenceUI from 'kyligence-kylin-ui'
import VueClipboard from 'vue-clipboard2'
import { createDirectives } from './directive'
import kylinPager from 'components/common/pager'
import emptyData from 'components/common/EmptyData/EmptyData.vue'
import kylinNodata from 'components/common/nodata.vue'
import kylinEditor from 'components/common/editor'
import kylinLoading from 'components/common/loading'
import commonTip from 'components/common/common_tip'
import editor from 'vue2-ace-editor'
jest.mock('components/common/editor', () => {
  return {
    setOption: jest.fn()
  }
})
jest.mock('vue2-ace-editor', () => {
  return {
    editor: {setOptions: jest.fn()}
  }
})

const mockEchartsEvent = {
  setOption: jest.fn(),
  dispose: jest.fn(),
  resize: jest.fn()
}
jest.mock('echarts', () => {
  return {
    init: () => {
      return {
        setOption: mockEchartsEvent.setOption,
        dispose: mockEchartsEvent.dispose,
        resize: mockEchartsEvent.resize
      }
    }
  }
})

const localVue = createLocalVue()
localVue.use(Vuex)
localVue.use(VueI18n)
localVue.use(KyligenceUI)
localVue.use(VueClipboard)
localVue.locale('en', {kylinLang: enKylinLocale.default})
localVue.component('kylin-pager', kylinPager)
localVue.component('kylin-empty-data', emptyData)
localVue.component('kylin-nodata', kylinNodata)
localVue.component('editor', editor)
localVue.component('kylin-editor', kylinEditor)
localVue.component('kylin-loading', kylinLoading)
localVue.component('common-tip', commonTip)
createDirectives(localVue)

export {
  localVue,
  mockEchartsEvent
}
