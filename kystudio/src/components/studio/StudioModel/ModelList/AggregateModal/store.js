import { handleSuccessAsync, getFullMapping } from 'util'

const types = {
  SHOW_MODAL: 'SHOW_MODAL',
  HIDE_MODAL: 'HIDE_MODAL',
  SET_MODAL: 'SET_MODAL',
  SET_MODAL_FORM: 'SET_MODAL_FORM',
  RESET_MODAL_FORM: 'RESET_MODAL_FORM',
  CALL_MODAL: 'CALL_MODAL',
  INIT_FORM: 'INIT_FORM',
  SHOW_LOADING: 'SHOW_LOADING',
  HIDE_LOADING: 'HIDE_LOADING',
  SET_MODEL_DATA_LOADED: 'SET_MODEL_DATA_LOADED',
  GET_STREAMING_INFO: 'GET_STREAMING_INFO'
}
export const initialAggregateData = JSON.stringify({
  id: 0,
  index_range: '',
  curAggIsEdit: false,
  includes: [],
  measures: [],
  mandatory: [],
  jointArray: [{
    id: 0,
    items: []
  }],
  hierarchyArray: [{
    id: 0,
    items: []
  }],
  activeTab: 'dimension',
  open: true,
  dimCap: null,
  isEditDim: false
})
const initialState = JSON.stringify({
  isShow: false,
  isLoading: false,
  editType: 'edit',
  callback: null,
  model: null,
  projectName: null,
  aggregateIdx: -1,
  formDataLoaded: false,
  indexUpdateEnabled: true,
  indexType: '',
  form: {
    isCatchUp: false,
    globalDimCap: null,
    isDimClearable: false,
    aggregateArray: [
      JSON.parse(initialAggregateData)
    ]
  }
})

export default {
  state: JSON.parse(initialState),
  getters: {
    dimensions: state => () => {
      return getDimensions(state.model)
    },
    dimensionIdMapping (state, getters) {
      return getMapping(getters.dimensions())
    },
    measures (state) {
      return getMeasures(state.model)
    },
    measureIdMapping (_, getters) {
      return getMapping(getters.measures)
    }
  },
  mutations: {
    [types.SET_MODAL_FORM]: (state, payload) => {
      state.form = { ...state.form, ...payload }
    },
    [types.SHOW_MODAL]: (state) => {
      state.isShow = true
    },
    [types.HIDE_MODAL]: (state) => {
      state.isShow = false
    },
    [types.SHOW_LOADING]: (state) => {
      state.isLoading = true
    },
    [types.HIDE_LOADING]: (state) => {
      state.isLoading = false
    },
    [types.RESET_MODAL_FORM]: (state) => {
      state.form = JSON.parse(initialState).form
    },
    [types.SET_MODAL]: (state, payload) => {
      for (const key of Object.keys(state)) {
        key in payload && (state[key] = payload[key])
      }
    },
    [types.SET_MODEL_DATA_LOADED]: (state, result) => {
      state.formDataLoaded = result
    },
    [types.INIT_FORM]: (state, payload) => {
      if (payload.aggregateGroupRule && payload.aggregateGroupRule.aggregation_groups.length) {
        const dimensions = getDimensions(state.model)
        const measuresList = getMeasures(state.model)
        const nameMapping = getMapping(dimensions)
        const measuresMapping = getMapping(measuresList)
        state.form.indexUpdateEnabled = payload.indexUpdateEnabled
        state.form.globalDimCap = payload.aggregateGroupRule.global_dim_cap
        state.form.isDimClearable = !!payload.aggregateGroupRule.global_dim_cap
        const aggGroups = payload.aggregateGroupRule.aggregation_groups.map((aggregationGroup, aggregateIdx) => {
          const index_range = aggregationGroup.index_range
          const id = payload.aggregateGroupRule.aggregation_groups.length - aggregateIdx
          const includes = aggregationGroup.includes.map(include => nameMapping[include])
          const curAggIsEdit = includes.length > 0 // 为了区分是否是编辑的聚合组
          let measures = aggregationGroup.measures.map(measures => measuresMapping[measures])
          const selectRules = aggregationGroup.select_rule
          const mandatory = selectRules.mandatory_dims.map(mandatory => nameMapping[mandatory])
          const jointArray = selectRules.joint_dims.map((jointGroup, groupIdx) => {
            const items = jointGroup.map(joint => nameMapping[joint])
            return { id: groupIdx, items }
          })
          const hierarchyArray = selectRules.hierarchy_dims.map((hierarchyGroup, groupIdx) => {
            const items = hierarchyGroup.map(hierarchy => nameMapping[hierarchy])
            return { id: groupIdx, items }
          })
          const activeTab = 'dimension'
          const open = true
          const dimCap = selectRules.dim_cap
          const isEditDim = false
          if (dimCap) {
            state.form.isDimClearable = true
          }
          if (!hierarchyArray.length) {
            hierarchyArray.push({ id: 0, items: [] })
          }
          if (!jointArray.length) {
            jointArray.push({ id: 0, items: [] })
          }
          measures.includes('COUNT_ALL') && (measures = ['COUNT_ALL', ...measures.filter(label => label !== 'COUNT_ALL')])
          return { id, includes, measures, mandatory, jointArray, hierarchyArray, activeTab, open, dimCap, isEditDim, index_range, curAggIsEdit }
        })
        state.form.aggregateArray = aggGroups

        if (payload.editType === 'new') {
          const initAggregate = JSON.parse(initialAggregateData)
          let measuresList = []
          for (let item of getMeasures(state.model)) {
            item.label && (item.label === 'COUNT_ALL' ? measuresList.unshift(item.label) : measuresList.push(item.label))
          }
          initAggregate.measures = measuresList
          state.form.aggregateArray.push(initAggregate)
        }
      }
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit, state, dispatch }, { editType, projectName, model, aggregateIdx, indexUpdateEnabled = true, indexType = '' }) {
      const { dispatch: rootDispatch } = this

      return new Promise(async (resolve, reject) => {
        const modelId = model && model.uuid
        commit(types.SET_MODEL_DATA_LOADED, false)
        commit(types.SET_MODAL, { editType, model, projectName, aggregateIdx, indexUpdateEnabled, indexType, callback: resolve })
        commit(types.SHOW_LOADING)
        commit(types.SHOW_MODAL)
        indexType && indexType === 'BATCH' && dispatch(types.GET_STREAMING_INFO, { projectName, modelId })
        const response = await rootDispatch('FETCH_AGGREGATE_GROUPS', { projectName, modelId })
        const aggregateGroupRule = await handleSuccessAsync(response)
        commit(types.HIDE_LOADING)
        if (!aggregateGroupRule || !aggregateGroupRule.aggregation_groups.length) {
          let measuresList = []
          for (let item of getMeasures(state.model)) {
            item.label && (item.label === 'COUNT_ALL' ? measuresList.unshift(item.label) : measuresList.push(item.label))
          }
          state.form.aggregateArray[0].measures = measuresList
          state.form.aggregateArray[0].index_range = ''
          return
        }
        setTimeout(() => {
          commit(types.SET_MODEL_DATA_LOADED, true)
          commit(types.INIT_FORM, {aggregateGroupRule, editType})
        }, 0)
      })
    },
    async [types.GET_STREAMING_INFO] ({commit, state, dispatch}, { projectName, modelId }) {
      const { dispatch: rootDispatch } = this
      const res = await rootDispatch('LOAD_ALL_INDEX', {
        sources: '',
        status: '',
        range: 'HYBRID,STREAMING',
        project: projectName,
        model: modelId,
        ids: '',
        page_offset: 0,
        page_size: 1,
        key: ''
      })
      const result = await handleSuccessAsync(res)
      commit(types.SET_MODAL, { indexUpdateEnabled: result.index_update_enabled })
    }
  },
  namespaced: true
}

function getMapping (data) {
  const mapping = data.reduce((mapping, item) => {
    mapping[item.value] = item.id
    return mapping
  }, {})
  return getFullMapping(mapping)
}

function getDimensions (model) {
  if (model) {
    return model.simplified_dimensions
      .filter(column => column.status === 'DIMENSION')
      .map(dimension => ({
        ...dimension,
        label: dimension.column,
        value: dimension.column,
        isCheck: false
      }))
  } else {
    return []
  }
}

function getMeasures (model) {
  return model ? model.simplified_measures.map(measure => ({...measure, label: measure.name, value: measure.name})) : []
}

export { types }
