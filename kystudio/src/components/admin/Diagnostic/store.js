import api from '../../../service/api'
import { apiUrl } from '../../../config/index'
import { handleError } from '../../../util/business'

const pollingTime = 1000
let timer = {}

export const types = {
  GET_DUMP_REMOTE: 'GET_DUMP_REMOTE',
  GET_SERVERS: 'GET_SERVERS',
  GET_STATUS_REMOTE: 'GET_STATUS_REMOTE',
  UPDATE_DUMP_IDS: 'UPDATE_DUMP_IDS',
  UPDATE_SERVERS: 'UPDATE_SERVERS',
  SET_DUMP_PROGRESS: 'SET_DUMP_PROGRESS',
  UPDATE_CHECK_TYPE: 'UPDATE_CHECK_TYPE',
  RESET_DUMP_DATA: 'RESET_DUMP_DATA',
  DEL_DUMP_ID_LIST: 'DEL_DUMP_ID_LIST',
  POLLING_STATUS_MSG: 'POLLING_STATUS_MSG',
  DOWNLOAD_DUMP_DIAG: 'DOWNLOAD_DUMP_DIAG',
  STOP_INTERFACE_CALL: 'STOP_INTERFACE_CALL',
  REMOVE_DIAGNOSTIC_TASK: 'REMOVE_DIAGNOSTIC_TASK',
  GET_QUERY_DIAGNOSTIC: 'GET_QUERY_DIAGNOSTIC'
}

export default {
  state: {
    diagDumpIds: {},
    servers: [],
    isReset: false
  },
  mutations: {
    [types.UPDATE_DUMP_IDS] (state, { host, start, end, id, tm }) {
      let idList = state.diagDumpIds
      if (typeof id === 'string') {
        Object.keys(idList).map((item) => {
          if (idList[item].host === host && idList[item].start === start && idList[item].end === end) {
            delete idList[item]
          }
        })
        idList = {
          ...idList,
          ...{[id]: {
            id,
            host,
            start,
            end,
            status: '000',
            stage: '',
            progress: 0,
            error: '',
            showErrorDetail: false,
            isCheck: false,
            running: true,
            tm
          }}
        }
      }
      // 按时间顺序倒序排列
      let obj = {}
      let timeList = Object.keys(idList).map(it => {
        return { tm: idList[it].tm, id: idList[it].id }
      }).sort((a, b) => b.tm - a.tm)
      timeList.forEach(item => {
        obj[item.id] = idList[item.id]
      })
      state.diagDumpIds = {...obj}
    },
    [types.UPDATE_SERVERS] (state, data) {
      if (!Array.isArray(data)) console.error('[API Servers]: response data is not array')
      state.servers = data
    },
    // 更改当前诊断包生成的进度
    [types.SET_DUMP_PROGRESS] (state, data) {
      const { status, id, duration } = data
      if (!(id in state.diagDumpIds)) return
      if (status === '000') {
        const { stage, progress } = data
        // state.diagDumpIds[id] = {...state.diagDumpIds[id], ...{status, stage, progress}}
        state.diagDumpIds = {...state.diagDumpIds, ...{[id]: {...state.diagDumpIds[id], ...{status, stage, progress, duration, running: progress !== 1}}}}
      } else if (['001', '002', '999'].includes(status)) {
        const { stage, error } = data
        state.diagDumpIds = {...state.diagDumpIds, ...{[id]: {...state.diagDumpIds[id], ...{status, stage, error, duration, running: false}}}}
      } else {
        state.diagDumpIds = {...state.diagDumpIds, ...{[id]: {...state.diagDumpIds[id], ...{status, duration, running: false}}}}
      }
    },
    // 更改诊断包选中状态
    [types.UPDATE_CHECK_TYPE] (state, type) {
      let dumps = state.diagDumpIds
      if (type) {
        Object.keys(dumps).forEach(item => {
          dumps[item].status === '000' && !dumps[item].running && (dumps[item].isCheck = true)
        })
      } else {
        Object.keys(dumps).forEach(item => {
          dumps[item].status === '000' && (dumps[item].isCheck = false)
        })
      }
      state.diagDumpIds = {...state.diagDumpIds, ...dumps}
    },
    [types.DEL_DUMP_ID_LIST] (state, id) {
      let list = state.diagDumpIds
      delete list[id]
      state.diagDumpIds = {...list}
    },
    // 重置生成数据
    [types.RESET_DUMP_DATA] (state, type) {
      if (type) {
        state.diagDumpIds = {}
      }
      Object.keys(timer).forEach(item => {
        clearTimeout(timer[item])
      })
      timer = {}
    },
    [types.STOP_INTERFACE_CALL] (state, value) {
      state.isReset = value
    }
  },
  actions: {
    [types.GET_QUERY_DIAGNOSTIC] ({ state, commit, dispatch }, { host = '', start = '', end = '', query_id = '', project, tm }) {
      if (!host) return
      return new Promise((resolve, reject) => {
        api.system.getQueryDiagnostic({ host, project, query_id }).then(async (res) => {
          if (state.isReset) return
          const { data } = res.data
          await commit(types.UPDATE_DUMP_IDS, { host, start, end, id: data, tm })
          dispatch(types.POLLING_STATUS_MSG, { host, id: data })
          resolve(data)
        }).catch((err) => {
          handleError(err)
          reject(err)
        })
      })
    },
    // 生成诊断包
    [types.GET_DUMP_REMOTE] ({ state, commit, dispatch }, { host = '', start = '', end = '', job_id = '', tm }) {
      if (!host) return
      return new Promise((resolve, reject) => {
        api.system.getDumpRemote({ host, start, end, job_id }).then(async (res) => {
          if (state.isReset) return
          const { data } = res.data
          await commit(types.UPDATE_DUMP_IDS, { host, start, end, id: data, tm })
          dispatch(types.POLLING_STATUS_MSG, { host, id: data })
          resolve(data)
        }).catch((err) => {
          handleError(err)
          reject(err)
        })
      })
    },
    // 获取all或query节点信息
    [types.GET_SERVERS] ({ commit }, self) {
      return new Promise((resolve, reject) => {
        api.datasource.loadOnlineQueryNodes({ext: true}).then((res) => {
          let { data } = res.data
          // 所有节点都能打任务诊断包
          // if (self.$route.name === 'Job') {
          //   data = data.filter(item => item.mode === 'all')
          // }
          commit(types.UPDATE_SERVERS, data)
          resolve(data)
        }).catch((err) => {
          handleError(err)
          reject(err)
        })
      })
    },
    // 获取诊断报生成进度
    [types.GET_STATUS_REMOTE] ({ commit }, { host, id }) {
      return new Promise((resolve, reject) => {
        api.system.getStatusRemote({ host, id }).then(res => {
          const { data } = res.data
          commit(types.SET_DUMP_PROGRESS, {...data, id})
          resolve(res)
        }).catch((err) => {
          reject(err)
        })
      })
    },
    // 轮询接口获取信息
    [types.POLLING_STATUS_MSG] ({ state, commit, dispatch }, { host, id }) {
      if (state.isReset) return
      dispatch(types.GET_STATUS_REMOTE, { host, id }).then((res) => {
        timer[id] = setTimeout(() => {
          dispatch(types.POLLING_STATUS_MSG, { host, id })
        }, pollingTime)
        const { data } = res.data
        if (data.status === '000' && data.stage === 'DONE') {
          clearTimeout(timer[id])
          dispatch(types.DOWNLOAD_DUMP_DIAG, {host, id})
        } else if (['001', '002', '999'].includes(data.status)) {
          clearTimeout(timer[id])
        }
      }).catch((err) => {
        handleError(err)
        clearTimeout(timer[id])
        commit(types.SET_DUMP_PROGRESS, {status: 'error', id})
      })
    },
    // 下载诊断包
    [types.DOWNLOAD_DUMP_DIAG] (_, { host, id }) {
      let dom = document.createElement('a')
      dom.download = true
      // 兼容IE 10以下 无origin属性问题，此处用protocol和host拼接
      dom.href = `${location.protocol}//${location.host}${apiUrl}system/diag?host=${host}&id=${id}`
      document.body.appendChild(dom)
      dom.click()
      document.body.removeChild(dom)
    },
    // 关闭弹窗后通知后端终止正在进行的任务
    [types.REMOVE_DIAGNOSTIC_TASK] ({ state }, message) {
      const { diagDumpIds } = state
      let removeList = Object.keys(diagDumpIds).filter(item => diagDumpIds[item].running)
      removeList.length && removeList.forEach(it => {
        let { host, id } = diagDumpIds[it]
        api.system.stopDumpTask({ host, id }).then(() => {
          window.kylinVm.$message({
            message: message,
            type: 'success'
          })
        })
      })
    }
  },
  namespaced: true
}
