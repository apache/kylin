// The Vue build version to load with the `import` command
// (runtime-only or standalone) has been set in webpack.base.conf with an alias.
import '@babel/polyfill'
import './util/polyfill'
import Vue from 'vue'
import ElementUI from 'kyligence-kylin-ui'
import store from './store'
import fullLayout from 'components/layout/layout_full'
import router from './router'
import filters from './filter'
import directive from './directive'
import { sync } from './util/vuex-router-sync'
// common module register
import commonTip from 'components/common/common_tip'
import tab from 'components/common/tab'
import kylinPager from 'components/common/pager'
import slider from 'components/common/slider'
import nodata from 'components/common/nodata'
import emptyData from 'components/common/EmptyData/EmptyData.vue'
import progressbar from 'components/common/progress'
import kylinEditor from 'components/common/editor'
import kylinLoading from 'components/common/loading'
import editor from 'vue2-ace-editor'
import VueClipboard from 'vue-clipboard2'
import VueKonva from 'vue-konva'
import nprogress from 'nprogress'
import 'brace/mode/json'
import 'brace/mode/sql'
import 'brace/snippets/sql'
import 'brace/theme/chrome'
import 'brace/theme/monokai'
import 'brace/ext/language_tools'
import './assets/styles/index.less'
import { ListenMessage } from './service/message.js'

Vue.component('common-tip', commonTip)
Vue.component('kylin-pager', kylinPager)
Vue.component('kylin-nodata', nodata)
Vue.component('slider', slider)
Vue.component('kylin-progress', progressbar)
Vue.component('editor', editor)
Vue.component('kylin-editor', kylinEditor)
Vue.component('kylin-tab', tab)
Vue.component('kylin-loading', kylinLoading)
Vue.component('kylin-empty-data', emptyData)
import { pageRefTags } from 'config'

Vue.use(ElementUI, {
  closeOtherMessages: true,
  errorMessageDuration: 10000,
  errorMessageShowClose: true
})
Vue.use(VueClipboard)
Vue.use(VueKonva)
Vue.http.headers.common['Accept-Language'] = localStorage.getItem('kystudio_lang') === 'en' ? 'en' : 'cn'
Vue.http.options.xhr = { withCredentials: true }
const skipUpdateApiList = [
  'kylin/api/jobs'
]
Vue.http.interceptors.push(function (request, next) {
  const isProgressVisiable = !request.headers.get('X-Progress-Invisiable')
  request.headers['Cache-Control'] = 'no-cache'
  request.headers['If-Modified-Since'] = '0'
  if (request.url.indexOf('kylin/api/j_spring_security_logout') >= 0) {
    request.headers.set('Accept', 'text/html')
  } else if (request.url.indexOf('acl/') >= 0 && request.method === 'PUT') {
    request.headers.set('Accept', 'application/vnd.apache.kylin-v4-public+json')
  } else {
    request.headers.set('Accept', 'application/vnd.apache.kylin-v4+json')
  }
  skipUpdateApiList.forEach((url) => {
    if (request.url.indexOf(url)) {
      request.headers.set('Auto', request.params.isAuto ? 'true' : 'false')
    }
  })
  isProgressVisiable && nprogress.start()
  next(function (response) {
    isProgressVisiable && nprogress.done()
    if (response.status === 401 && router.history.current.name !== 'login') {
      for (let p in pageRefTags) {
        const pager = pageRefTags[p]
        if (localStorage.getItem(pager)) {
          localStorage.removeItem(pager)
        }
      }
      localStorage.setItem('loginIn', false)
      router.replace({name: 'Login', params: { ignoreIntercept: true }})
    }
  })
})

Vue.prototype.$_bus = new Vue()
const EventsBus = Vue.prototype.$_bus

sync(store, router)
/* eslint-disable no-new */
window.kylinVm = new Vue({
  el: '#app',
  router,
  store,
  directive,
  filters,
  template: '<fullLayout/>',
  components: { fullLayout }
})

// 消息通知
ListenMessage(EventsBus)
