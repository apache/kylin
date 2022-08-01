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
