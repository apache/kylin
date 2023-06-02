import { AGGREGATE_TYPE } from '../../../config'

const types = {
  SHOW_MODAL: 'SHOW_MODAL',
  HIDE_MODAL: 'HIDE_MODAL',
  SET_MODAL: 'SET_MODAL',
  RESET_MODAL: 'RESET_MODAL',
  SET_MODAL_FORM: 'SET_MODAL_FORM',
  CALL_MODAL: 'CALL_MODAL'
}

export const ALERT_STATUS = {
  INIT: 'INIT',
  SUCCESS: 'SUCCESS',
  WARNING: 'WARNING',
  ERROR: 'ERROR'
}

export function getInitialErrors () {
  return {
    notInModel: [],
    notInIncludes: [],
    duplicate: [],
    usedInOthers: []
  }
}

function getInitialState () {
  return {
    isShow: false,
    model: null,
    aggregate: null,
    type: AGGREGATE_TYPE.INCLUDE,
    status: ALERT_STATUS.INIT,
    groupIdx: null,
    errors: getInitialErrors(),
    errorLines: [],
    errorInEditor: [],
    errorCursor: 0,
    form: {
      text: '',
      // { label, value, isChecked, type }
      dimensions: []
    },
    callback: null
  }
}

export default {
  state: getInitialState(),
  mutations: {
    [types.SHOW_MODAL] (state) {
      state.isShow = true
    },
    [types.HIDE_MODAL] (state) {
      state.isShow = false
    },
    [types.SET_MODAL] (state, payload) {
      for (const [key, value] of Object.entries(payload)) {
        state[key] = value
      }
    },
    [types.RESET_MODAL] (state) {
      for (const [key, value] of Object.entries(getInitialState())) {
        state[key] = value
      }
    },
    [types.SET_MODAL_FORM] (state, payload) {
      for (const [key, value] of Object.entries(payload)) {
        state.form[key] = value
      }
    }
  },
  getters: {
    includes (state) {
      const { aggregate } = state
      return aggregate?.includes ?? []
    },
    mandatories (state) {
      const { aggregate } = state
      return aggregate?.mandatory ?? []
    },
    hierarchies (state) {
      const { aggregate } = state
      return aggregate?.hierarchyArray ?? []
    },
    joints (state) {
      const { aggregate } = state
      return aggregate?.jointArray ?? []
    },
    tableIndexCols (state) {
      const { allColumns } = state
      return allColumns.filter(c => c.isUsed).map(c => c.fullName)
    },
    hierarchyItems (state) {
      const { aggregate, groupIdx } = state
      return aggregate?.hierarchyArray[groupIdx]?.items ?? []
    },
    jointItems (state) {
      const { aggregate, groupIdx } = state
      return aggregate?.jointArray[groupIdx]?.items ?? []
    },
    modelDimensions (state) {
      const { model } = state
      return model?.simplified_dimensions?.filter(c => c.status === 'DIMENSION') ?? []
    }
  },
  actions: {
    [types.CALL_MODAL] ({ commit }, args) {
      const { aggregate, type, model, allColumns = [], groupIdx = null } = args
      return new Promise(resolve => {
        commit(types.SET_MODAL, { aggregate, model, type, groupIdx, allColumns, callback: resolve })
        commit(types.SHOW_MODAL)
      })
    }
  },
  namespaced: true
}

export { types }
