import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  getProjectList: (params) => {
    return Vue.resource(apiUrl + 'projects').get(params)
  },
  deleteProject: (projectName) => {
    return Vue.resource(apiUrl + 'projects/' + projectName).delete()
  },
  updateProject: (project) => {
    return Vue.resource(apiUrl + 'projects').update({ former_project_name: project.name, project_desc_data: project.desc })
  },
  saveProject: (projectDesc) => {
    return Vue.resource(apiUrl + 'projects').save(projectDesc)
  },
  addProjectAccess: (accessData, projectId) => {
    return Vue.resource(apiUrl + 'access/batch/ProjectInstance/' + projectId).save(accessData)
  },
  editProjectAccess: (accessData, projectId) => {
    return Vue.resource(apiUrl + 'access/ProjectInstance/' + projectId).update(accessData)
  },
  getProjectAccess: (projectId, data) => {
    return Vue.resource(apiUrl + 'access/ProjectInstance/' + projectId).get(data)
  },
  getProjectEndAccess: (projectId) => {
    return Vue.resource(apiUrl + 'access/all/ProjectInstance/' + projectId).get()
  },
  delProjectAccess: (projectId, aid, userName, principal) => {
    return Vue.resource(apiUrl + 'access/ProjectInstance/' + projectId).delete({
      access_entry_id: aid,
      sid: userName,
      principal: principal
    })
  },
  submitAccessData: (projectName, userType, roleOrName, accessData) => {
    return Vue.resource(apiUrl + `acl/${userType}/${roleOrName}?project=${projectName}`).update(accessData)
  },
  saveProjectFilter: (filterData) => {
    return Vue.resource(apiUrl + 'ext_filter/save_ext_filter').save(filterData)
  },
  getProjectFilter: (project) => {
    return Vue.resource(apiUrl + 'ext_filter').get({
      project: project
    })
  },
  delProjectFilter: (project, filterName) => {
    return Vue.resource(apiUrl + 'ext_filter/' + filterName + '/' + project).delete()
  },
  updateProjectFilter: (filterData) => {
    return Vue.resource(apiUrl + 'ext_filter/update_ext_filter').update(filterData)
  },
  backupProject: (project) => {
    return Vue.resource(apiUrl + 'projects/' + project.name + '/backup').save()
  },
  accessAvailableUserOrGroup: (sidType, uuid, data) => {
    return Vue.resource(apiUrl + 'access/available/' + sidType + '/' + uuid).get(data)
  },
  getQuotaInfo: (para) => {
    return Vue.resource(apiUrl + 'projects/' + para.project + '/storage_volume_info').get()
  },
  clearTrash: (para) => {
    return Vue.resource(apiUrl + 'projects/' + para.project + '/storage').update()
  },
  fetchProjectSettings: (project) => {
    return Vue.resource(apiUrl + 'projects/' + project + '/project_config').get()
  },
  updateProjectGeneralInfo (body) {
    return Vue.resource(apiUrl + 'projects/' + body.project + '/project_general_info').update(body)
  },
  updateSegmentConfig (body) {
    return Vue.resource(apiUrl + 'projects/' + body.project + '/segment_config').update(body)
  },
  updatePushdownConfig (body) {
    return Vue.resource(apiUrl + 'projects/' + body.project + '/push_down_config').update(body)
  },
  updateStorageQuota (body) {
    return Vue.resource(apiUrl + 'projects/' + body.project + '/storage_quota').update(body)
  },
  updateJobAlertSettings (body) {
    return Vue.resource(apiUrl + 'projects/' + body.project + '/job_notification_config').update(body)
  },
  updateProjectDatasource (body) {
    return Vue.resource(apiUrl + 'projects/' + body.project + '/source_type').update(body)
  },
  resetConfig (para) {
    return Vue.resource(apiUrl + 'projects/' + para.project + '/project_config').update({reset_item: para.reset_item})
  },
  updateDefaultDBSettings (body) {
    return Vue.resource(apiUrl + 'projects/' + body.project + '/default_database').update({default_database: body.default_database})
  },
  updateYarnQueue (body) {
    return Vue.resource(apiUrl + 'projects/' + body.project + '/yarn_queue').update(body)
  },
  updateSnapshotConfig (body) {
    return Vue.resource(apiUrl + 'projects/' + body.project + '/snapshot_config').update(body)
  },
  updateExposeCCConfig (body) {
    return Vue.resource(apiUrl + 'projects/' + body.project + '/computed_column_config').update(body)
  },
  getAclPermission (data) {
    return Vue.resource(apiUrl + 'acl/updatable').get(data)
  },
  updateKerberosConfig (data) {
    return Vue.resource(apiUrl + 'projects/' + data.project + '/project_kerberos_info').update(data.body)
  },
  getAvailableProjectOwners (data) {
    return Vue.resource(apiUrl + 'access/available/ProjectInstance').get(data)
  },
  updateProjectOwner (data) {
    return Vue.resource(apiUrl + `projects/${data.project}/owner`).update({owner: data.owner})
  },
  updateIndexOptimization (data) {
    return Vue.resource(apiUrl + 'projects/' + data.project + '/garbage_cleanup_config').update(data)
  },
  toggleEnableSCD (data) {
    return Vue.resource(apiUrl + 'projects/' + data.project + '/scd2_config').update(data)
  },
  getSCDModel (data) {
    return Vue.resource(apiUrl + 'models/name/scd2').get(data)
  },
  updateSecStorageSettings (data) {
    return Vue.resource(apiUrl + 'storage/project/state').save(data)
  },
  getSecStorageModels (para) {
    return Vue.resource(apiUrl + 'storage/project/state/validation').save(para)
  },
  fetchAvailableNodes (para) {
    return Vue.resource(apiUrl + 'storage/nodes').get(para)
  },
  loadStatistics (para) {
    return Vue.resource(apiUrl + `projects/statistics`).get(para)
  },
  toggleMultiPartition (data) {
    return Vue.resource(apiUrl + 'projects/' + data.project + '/multi_partition_config').update(data)
  },
  getMultiPartitionModels (data) {
    return Vue.resource(apiUrl + 'models/name/multi_partition').get(data)
  },
  updateConfig ({project, data}) {
    return Vue.resource(apiUrl + 'projects/' + project + '/config').update(data)
  },
  deleteConfig (params) {
    return Vue.resource(apiUrl + 'projects/config/deletion').save(params)
  },
  getDefaultConfig () {
    return Vue.resource(apiUrl + 'projects/default_configs').get()
  }
}
