import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  getEnv: () => {
    return Vue.resource(apiUrl + 'admin/env').get()
  },
  getConfig: () => {
    return Vue.resource(apiUrl + 'admin/config').get()
  },
  // 获取节点的一些固定配置信息
  getInstanceConfig: () => {
    return Vue.resource(apiUrl + 'admin/instance_info').get()
  },
  getPublicConfig: () => {
    return Vue.resource(apiUrl + 'admin/public_config').get()
  },
  getAuthentication: () => {
    return Vue.resource(apiUrl + 'user/authentication').get()
  },
  reloadMetadata: () => {
    return Vue.resource(apiUrl + 'cache/announce/all/all/update').update()
  },
  backupMetadata: () => {
    return Vue.resource(apiUrl + 'metastore/backup').save()
  },
  updateConfig: (config) => {
    return Vue.resource(apiUrl + 'admin/config').update(config)
  },
  getServiceState: () => {
    return Vue.resource(apiUrl + 'service_discovery/state/all').get()
  },
  getCanaryReport: (para) => {
    return Vue.resource(apiUrl + 'canary/report').get(para)
  },
  // 生成诊断包相关api接口
  getDumpRemote: (para) => {
    const { host, start, end, job_id } = para
    return Vue.http.post(apiUrl + `system/diag?host=${host}`, { start, end, job_id })
  },
  // 获取诊断包生成进度
  getStatusRemote: (para) => {
    return Vue.resource(apiUrl + 'system/diag/status').get(para)
  },
  // 终止后台诊断包生成
  stopDumpTask: (para) => {
    const { host, id } = para
    return Vue.resource(apiUrl + `system/diag?host=${host}`).delete({id})
  },
  downloadJobLogs: (para) => {
    const { jobId, stepId, project } = para
    return Vue.resource(apiUrl + `jobs/${jobId}/steps/${stepId}/log?project=${project}`).get()
  },
  loadOnlineNodes: (para) => {
    return Vue.resource(apiUrl + 'system/servers').get(para)
  },
  getQueryDiagnostic: (para) => {
    const { host, project, query_id } = para
    return Vue.http.post(apiUrl + `system/diag/query?host=${host}`, {project, query_id})
  }
}
