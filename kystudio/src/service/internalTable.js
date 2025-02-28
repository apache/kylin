import Vue from 'vue'
import VueResource from 'vue-resource'
import { apiUrl } from '../config'

Vue.use(VueResource)

export default {
  getInternalTableList: (params) => {
    return Vue.resource(apiUrl + 'internal_tables').get(params)
  },

  updateInternalTable: (params) => {
    return Vue.resource(apiUrl + `internal_tables/${params.database}/${params.table}?project=${params.project}`).update(params)
  },

  createInternalTable: (params) => {
    return Vue.resource(apiUrl + `internal_tables/${params.database}/${params.table}?project=${params.project}`).save(params)
  },

  getInternalTableDetails: (params) => {
    return Vue.resource(apiUrl + 'internal_tables{/database}{/table}').get(params)
  },

  deleteInternalTable: (params) => {
    return Vue.resource(apiUrl + `internal_tables/${params.database}/${params.table}?project=${params.project}`).delete()
  },

  loadSingleInternalTable: (params) => {
    return Vue.resource(apiUrl + `internal_tables/${params.project}/${params.database}/${params.table}`).save(params)
  },

  batchLoadInternalTables: (params) => {
    return Vue.resource(apiUrl + 'internal_tables/data/batch').save(params)
  },

  deletePartitions: (params) => {
    return Vue.resource(apiUrl + 'internal_tables/partitions').delete(params)
  },

  deleteAllData: (params) => {
    return Vue.resource(apiUrl + 'internal_tables/truncate_internal_table').delete(params)
  }
}
