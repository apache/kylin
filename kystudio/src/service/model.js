import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'
import { download } from '../util/domHelper'

Vue.use(VueResource)

export default {
  ignoreSpeedModelInfo: (para) => {
    return Vue.resource(apiUrl + 'query/favorite_queries/ignore?project=' + para.project + '&ignore_size=' + para.ignoreSize).update()
  },
  // purge
  purgeModel: (project, modelId) => {
    return Vue.resource(apiUrl + 'models/' + modelId + '/segments?project=' + project + '&purge=' + true).delete()
  },
  getModelList: (params) => {
    return Vue.resource(apiUrl + 'models{?status}{&model_attributes}').get(params)
  },
  renameModel: (params) => {
    return Vue.resource(apiUrl + 'models/' + params.model + '/name').update(params)
  },
  disableModel: (params) => {
    params.status = 'OFFLINE'
    return Vue.resource(apiUrl + 'models/' + params.modelId + '/status').update({model: params.modelId, project: params.project, status: params.status})
  },
  enableModel: (params) => {
    params.status = 'ONLINE'
    return Vue.resource(apiUrl + 'models/' + params.modelId + '/status').update({model: params.modelId, project: params.project, status: params.status})
  },
  invalidIndexes: (params) => {
    return window.kylinVm.$http.post(apiUrl + 'models/invalid_indexes', params)
  },
  measureDimensionSuggestion: (params) => {
    return Vue.resource(apiUrl + 'models/table_suggestions').get(params)
  },
  getModelByModelName: (para) => {
    return Vue.resource(apiUrl + 'models').get(para)
  },
  deleteModel: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.modelId + '?project=' + para.project).delete()
  },
  collectStats: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.modelname + '/stats?project=' + para.project).save(para.data)
  },
  updateModel: (data) => {
    return Vue.resource(apiUrl + 'models/semantic').update(data)
  },
  saveModel: (data) => {
    return Vue.resource(apiUrl + 'models').save(data)
  },
  saveModelDraft: (data) => {
    return Vue.resource(apiUrl + 'models/draft').update(data)
  },
  cloneModel: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.model + '/clone').save(para)
  },
  diagnose: (project, modelName) => {
    return Vue.resource(apiUrl + 'models/' + modelName + '/diagnose?project=' + project).get()
  },
  diagnoseList: (para) => {
    return Vue.resource(apiUrl + 'models/get_all_stats').get(para)
  },
  checkModelName: (para) => {
    return Vue.resource(apiUrl + 'models/validate/' + para.modelName).get()
  },
  checkUsedCols: (modelName) => {
    return Vue.resource(apiUrl + 'models/' + modelName + '/usedCols').get()
  },
  modelProgress: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.modelName + '/progress?project=' + para.project).get()
  },
  modelCheckable: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.modelName + '/checkable?project=' + para.project).get()
  },
  getModelAccess: (modelId) => {
    return Vue.resource(apiUrl + 'access/data_model_desc/' + modelId).get()
  },
  getModelEndAccess: (modelId) => {
    return Vue.resource(apiUrl + 'access/all/data_model_desc/' + modelId).get()
  },
  validModelPartitionColumnFormat: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.table + '/' + para.column + '/validate?project=' + para.project).get({format: para.format})
  },
  getColumnSampleData: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.table + '/' + para.column + '?project=' + para.project).get()
  },
  checkComputedExpression: (para) => {
    return Vue.resource(apiUrl + 'models/computed_columns/check').save(para)
  },
  getComputedColumns: (para) => {
    return Vue.resource(apiUrl + 'models/computed_columns/usage').get(para)
  },
  sqlValidate: (para) => {
    return Vue.resource(apiUrl + 'sql_validate/model').save(para)
  },
  fetchSegments: (model, paraData) => {
    return Vue.resource(`${apiUrl}models/${model}/segments`).get(paraData)
  },
  fetchAggregates: (para) => {
    return Vue.resource(`${apiUrl}models/${para.model}/agg_indices`).get(para)
  },
  fetchCuboid: (model, project, id) => {
    return Vue.resource(`${apiUrl}models/cuboids`).get({model, project, id})
  },
  fetchCuboids: (model, project) => {
    return Vue.resource(`${apiUrl}models/${model}/relations`).get({model, project})
  },
  getTableIndex: (para) => {
    return Vue.resource(apiUrl + 'index_plans/table_index').get(para)
  },
  editTableIndex: (para) => {
    return Vue.resource(apiUrl + 'index_plans/table_index').update(para)
  },
  delTableIndex: (para) => {
    return Vue.resource(apiUrl + 'index_plans/table_index/' + para.model + '/' + para.tableIndexId + '?project=' + para.project).delete()
  },
  addTableIndex: (para) => {
    return Vue.resource(apiUrl + 'index_plans/table_index').save(para)
  },
  refreshTableIndex: (para) => {
    return Vue.resource(apiUrl + 'index_plans/table_index').save(para)
  },
  refreshSegments: (modelId, project, ids, refresh_all_indexes) => {
    return Vue.resource(apiUrl + 'models/' + modelId + '/segments').update({ project, ids, type: 'REFRESH', refresh_all_indexes })
  },
  fetchSubPartitionValues: (para) => {
    return Vue.resource(apiUrl + `models/${para.model_id}/multi_partition/sub_partition_values`).get(para)
  },
  addPartitionValues: (para) => {
    return Vue.resource(apiUrl + `models/${para.model_id}/multi_partition/sub_partition_values`).save(para)
  },
  deletePartitionValues: (para) => {
    return Vue.resource(apiUrl + `models/${para.model_id}/multi_partition/sub_partition_values`).delete(para)
  },
  buildSubPartitions: (para) => {
    return Vue.resource(apiUrl + `models/${para.model_id}/model_segments/multi_partition`).save(para)
  },
  fetchSubPartitions: (para) => {
    return Vue.resource(apiUrl + `models/${para.model_id}/model_segments/multi_partition`).get(para)
  },
  refreshSubPartition: (para) => {
    return Vue.resource(apiUrl + `models/${para.model_id}/model_segments/multi_partition`).update(para)
  },
  deleteSubPartition: (para) => {
    return Vue.resource(apiUrl + `models/model_segments/multi_partition`).delete(para)
  },
  deleteSegments: (model, project, ids) => {
    return Vue.resource(`${apiUrl}models/${model}/segments/?project=${project}&purge=false`).delete({ ids })
  },
  // merge segment
  mergeSegments: (modelId, project, ids) => {
    return Vue.resource(apiUrl + 'models/' + modelId + '/segments').update({ project, ids, type: 'MERGE' })
  },
  mergeSegmentCheck: (para) => {
    return Vue.resource(apiUrl + `models/${para.modelId}/segments/merge_check`).save(para)
  },
  // 弃用
  modelDataCheck: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.modelId + '/data_check').update(para.data)
  },
  buildModel: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.model_id + '/model_segments').update(para.data)
  },
  buildFullLoadModel: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.model_id + '/segments').save(para)
  },
  checkDataRange: (para) => {
    return Vue.resource(apiUrl + `models/${para.modelId}/segment/validation`).save({project: para.project, start: para.start, end: para.end})
  },
  checkSegments: (para) => {
    return Vue.resource(apiUrl + `models/${para.modelId}/segment/validation{?ids}`).get({project: para.projectName, ids: para.ids})
  },
  setPartition: (para) => {
    return Vue.resource(apiUrl + `models/${para.modelId}/partition`).update({project: para.project, partition_desc: para.partition_desc, multi_partition_desc: para.multi_partition_desc})
  },
  fetchAggregateGroups: (project, model) => {
    return Vue.resource(apiUrl + 'index_plans/rule').get({ project, model })
  },
  updateAggregateGroups: (project, modelId, dimensions, aggregationGroups, isCatchUp, globalDimCap, restoreDeletedIndex) => {
    return Vue.resource(apiUrl + 'index_plans/rule').update({ project, model_id: modelId, dimensions, aggregation_groups: aggregationGroups, load_data: isCatchUp, global_dim_cap: globalDimCap, scheduler_version: 2, restore_deleted_index: restoreDeletedIndex })
  },
  getCalcCuboids: (project, modelId, dimensions, aggregationGroups, globalDimCap) => {
    return Vue.resource(apiUrl + 'index_plans/agg_index_count').update({ project, model_id: modelId, dimensions, aggregation_groups: aggregationGroups, global_dim_cap: globalDimCap, scheduler_version: 2 })
  },
  fetchRelatedModelStatus: (project, uuids) => {
    const body = { project, uuids }
    const headers = { 'X-Progress-Invisiable': 'true' }
    return window.kylinVm.$http.post(apiUrl + 'models/job_error_status', body, { headers })
  },
  getModelDataNewestRange: (para) => {
    return Vue.resource(apiUrl + `models/${para.model}/data_range/latest_data`).save(para)
  },
  loadModelConfigList: (para) => {
    return Vue.resource(apiUrl + 'models/config').get(para)
  },
  updateModelConfig: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.model + '/config').update(para)
  },
  getModelJSON: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.model + '/json').get(para)
  },
  getModelSql: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.model + '/sql').get(para)
  },
  buildIndex: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.model_id + '/indices').save({project: para.project})
  },
  complementAllIndex: (para) => {
    return Vue.resource(apiUrl + `models/${para.modelId}/model_segments/all_indexes`).save(para.data)
  },
  complementBatchIndex: (para) => {
    return Vue.resource(apiUrl + `models/${para.modelId}/model_segments/indexes`).save(para.data)
  },
  getModelRecommendations: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.model + '/recommendations?project=' + para.project).get()
  },
  adoptModelRecommendations: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.model + '/recommendations').update(para)
  },
  clearModelRecommendations: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.model + '/recommendations').delete(para)
  },
  getAggIndexContentList: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.model + '/recommendations/agg_index').get(para)
  },
  getTableIndexContentList: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.model + '/recommendations/table_index').get(para)
  },
  getIndexContentList: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.model + '/recommendations/index').get(para)
  },
  deleteBatchIndex: (para) => {
    return Vue.resource(apiUrl + `models/${para.modelId}/model_segments/indexes/deletion`).save(para.data)
  },
  suggestModel: (para) => {
    return Vue.resource(apiUrl + 'models/suggest_model').save(para)
  },
  saveSuggestModels: (para) => {
    return Vue.resource(apiUrl + `models/model_recommendation?project=${para.project}`).save(para)
  },
  validateModelName: (para) => {
    return Vue.resource(apiUrl + 'models/validate_model').save(para)
  },
  newModelNameValidate: (para) => {
    return Vue.resource(apiUrl + 'models/name/validation').save(para)
  },
  addAggIndexAdvanced: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.model_id + '/agg_indices/shard_columns').save(para)
  },
  getAggIndexAdvanced: (para) => {
    return Vue.resource(apiUrl + 'models/' + para.model + '/agg_indices/shard_columns').get(para)
  },
  loadAllIndex: (para) => {
    return Vue.resource(apiUrl + 'index_plans/index{?sources}' + '{&status}' + '{&range}').get(para)
  },
  loadBaseIndex: (paras) => {
    return window.kylinVm.$http.post(apiUrl + 'index_plans/base_index', paras)
  },
  updateBaseIndex: (paras) => {
    return Vue.http.put(apiUrl + 'index_plans/base_index', paras)
  },
  deleteIndex: (para) => {
    const params = {
      ...para
    }
    delete params.id
    return Vue.resource(apiUrl + `index_plans/index/${para.id}`).delete(params)
    // return Vue.resource(apiUrl + `index_plans/index/${para.id}?project=${para.project}&model=${para.model}`).delete()
  },
  deleteIndexes: (para) => {
    return Vue.resource(apiUrl + 'index_plans/index').delete(para)
  },
  fetchIndexGraph: (para) => {
    return Vue.resource(apiUrl + 'index_plans/index_graph').get(para)
  },
  // 获取索引自定义的特征信息
  fetchIndexStat: (para) => {
    return Vue.resource(apiUrl + 'index_plans/index_stat').get(para)
  },
  suggestIsByAnswered: (para) => {
    return Vue.resource(apiUrl + 'models/can_answered_by_existed_model').save(para)
  },
  checkFilterConditon: (para) => {
    return Vue.resource(apiUrl + 'models/model_save/check').save(para)
  },
  getIndexDiff: (project, modelId, dimensions, aggregationGroups, isCatchUp, globalDimCap) => {
    return Vue.resource(apiUrl + 'index_plans/rule_based_index_diff').update({ project, model_id: modelId, dimensions, aggregation_groups: aggregationGroups, load_data: isCatchUp, global_dim_cap: globalDimCap })
  },
  autoFixSegmentHoles: (para) => {
    return Vue.resource(apiUrl + `models/${para.model_id}/segment_holes`).save(para)
  },
  getMetadataStructure (para) {
    return Vue.resource(apiUrl + `metastore/previews/models`).get(para)
  },
  downloadModelsMetadata (para) {
    return download.post(apiUrl + `metastore/backup/models?project=${para.project}`, para.form)
  },
  uploadModelsMetadata (para) {
    return Vue.resource(apiUrl + `metastore/validation/models?project=${para.project}`).save(para.form)
  },
  importModelsMetadata (para) {
    return Vue.resource(apiUrl + `metastore/models?project=${para.project}`).save(para.form)
  },
  getAvailableModelOwners (para) {
    return Vue.resource(apiUrl + 'access/available/NDataModel').get(para)
  },
  updateModelOwner (para) {
    return Vue.resource(apiUrl + `models/${para.model}/owner`).update({project: para.project, owner: para.owner})
  },
  downloadModelsMetadataBlob (para) {
    const body = para.params
    const headers = { 'Content-Type': 'application/x-www-form-urlencoded' }
    const responseType = 'blob' || 'arraybuffer'
    return window.kylinVm.$http.post(`/kylin/api/metastore/backup/models?project=${para.project}`, body, { emulateJSON: true }, { headers }, { responseType })
  },
  // 获取所有优化建议
  getRecommendations (para) {
    return Vue.resource(apiUrl + `recommendations/${para.modelId}`).get(para)
  },
  // 刷新优化建议
  recommendationCountRefresh (para) {
    return Vue.http.put(apiUrl + 'recommendations/count', para)
  },
  // 删除优化建议
  deleteRecommendations (para) {
    return Vue.resource(apiUrl + `recommendations/${para.modelId}`).delete(para)
  },
  // 通过优化建议
  acceptRecommendations (para) {
    return Vue.resource(apiUrl + `recommendations/${para.modelId}`).save(para)
  },
  // 获取优化建议详情
  getRecommendDetails (para) {
    return Vue.resource(apiUrl + `recommendations/${para.modelId}/${para.id}`).get(para)
  },
  validateRecommend (para) {
    return Vue.resource(apiUrl + `recommendations/${para.modelId}/validation`).save(para)
  },
  fetchHitModelsList (para) {
    return Vue.resource(apiUrl + 'query/query_history_models').get(para)
  },
  fetchSubmitterList (para) {
    return Vue.resource(apiUrl + 'query/query_history_submitters').get(para)
  },
  // 获取任务状态
  getStreamingJobs (para) {
    return Vue.resource(apiUrl + 'streaming_jobs').get(para)
  },
  // 更改任务状态
  changeStreamingJobStatus (para) {
    return Vue.resource(apiUrl + 'streaming_jobs/status').update(para)
  },
  updateStreamingConfigurations (para) {
    return Vue.resource(apiUrl + 'streaming_jobs/params').update(para)
  },
  updateModelSecStorage (para) {
    return Vue.resource(apiUrl + 'storage/model/state').save(para)
  },
  syncSegmentsSecStorage (para) {
    return Vue.resource(apiUrl + 'storage/segments').save(para)
  },
  deleteSyncSegments (para) {
    return Vue.resource(apiUrl + 'storage/segments{?segment_ids}').delete(para)
  },
  validateDateFormat (para) {
    return Vue.resource(apiUrl + 'models/check_partition_desc').save(para)
  },
  checkInternalMeasure (para) {
    return Vue.resource(apiUrl + 'models/model_save/check').save(para)
  },
  loadSecondStorageScanRows (para) {
    return Vue.resource(apiUrl + 'query/query_history/tired_storage_metrics').get(para)
  },
  validateExportTds: (para) => {
    return Vue.resource(apiUrl + 'models/validate_export?project=' + para.project + '&model=' + para.modelId + '&element=' + para.element).get()
  },
  saveSecondaryStorageIndexes (para) {
    return Vue.resource(apiUrl + 'storage/index').save(para)
  },
  getSecondaryStorageIndexes (para) {
    return Vue.resource(apiUrl + 'storage/index').get(para)
  },
  materializeSecondaryStorageIndexes (para) {
    return Vue.resource(apiUrl + 'storage/index/secondary/materialize').save(para)
  },
  deleteSecondaryOrderByIndex (para) {
    return Vue.resource(apiUrl + 'storage/index/primary').delete(para)
  },
  deleteSecondarySkippingIndex (para) {
    return Vue.resource(apiUrl + 'storage/index/secondary').delete(para)
  },
  updateModelStorageType (para) {
    return Vue.resource(apiUrl + `models/${para.model_id}/storage_type`).update(para.data)
  },
  saveOptimizeLayoutData (para) {
    return Vue.resource(apiUrl + `models/${para.model_id}/optimize_layout_data`).save(para)
  },
  getIndexDetail (para) {
    return Vue.resource(apiUrl + `models/${para.project}/${para.model_id}/${para.index_id}/layout_detail`).get()
  },
  downloadOptimizeLayoutTemplate () {
    return Vue.resource(apiUrl + 'models/optimize_layout_data_template').get()
  }
}
