import * as types from './types'
import api from '../service/api'

export default {
  state: {},
  actions: {
    [types.GET_MODEL_STATISTICS]: function ({commit, state}, param) {
      return api.dashboard.getModelStatistics(param)
    },
    [types.GET_QUERY_STATISTICS]: function ({commit, state}, param) {
      return api.dashboard.getQueryStatistics(param)
    },
    [types.GET_CHART_DATA]: function ({commit, state}, param) {
      return api.dashboard.getChartData(param.category, param.metric, param.dimension, param.projectName, param.cubeName, param.startTime, param.endTime)
    },
    [types.GET_JOB_STATISTICS]: function ({commit, state}, param) {
      return api.dashboard.getJobStatistics(param)
    }
  }
}
