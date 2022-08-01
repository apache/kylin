
import Vuex from 'vuex'
import Vue from 'vue'
Vue.use(Vuex)
import model from './model'
import project from './project'
import config from './config'
import kafka from './kafka'
import user from './user'
import datasource from './datasource'
import system from './system'
import monitor from './monitor'
import * as actionTypes from './types'

export default new Vuex.Store({
  modules: {
    model: model,
    project: project,
    config: config,
    kafka: kafka,
    user: user,
    datasource: datasource,
    system: system,
    monitor: monitor,
    modals: {}
  }
})

export {
  actionTypes
}
