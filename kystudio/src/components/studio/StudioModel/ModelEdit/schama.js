import store from '../../../../store'
import { jsPlumbTool } from '../../../../util/plumb'
import { sampleGuid, indexOfObjWithSomeKeys, objectClone } from '../../../../util'
import { modelRenderConfig } from './config'

export default class SchemaModels {
  constructor (options, _mount, _) {
    Object.assign(this, options)
    this.mode = options.uuid ? 'edit' : 'new' // 当前模式
    this.name = options.name
    this.alias = options.alias || options.name
    this.fact_table = options.fact_table
    this.description = options.description
    this.project = options.project
    this.uuid = options.uuid || null
    this.tables = {}
    this.owner = options.owner || ''
    this.storage = options.storage
    this.canvas = options.canvas // 模型布局坐标
    this.broken_reason = options.broken_reason // 破损原因
    this.filter_condition = options.filter_condition || ''
    this.column_correlations = options.column_correlations || []
    this.computed_columns = options.computed_columns || []
    this.last_modified = options.last_modified || 0
    this.linkUsedColumns = {}
    this.partition_desc = options.partition_desc || {
      partition_date_column: null,
      partition_time_column: null,
      partition_date_start: 0,
      partition_type: 'APPEND'
    }
    this.anti_flatten_lookups = options.join_tables ? options.join_tables.filter(item => item.flattenable === 'normalized').map(it => it.alias) : []
    this.anti_flatten_cc = []
    this.multi_partition_desc = options.multi_partition_desc || null
    this.his_partition_desc = Object.assign({}, options.partition_desc)
    this.has_base_agg_index = options.has_base_agg_index || false
    this.has_base_table_index = options.has_base_table_index || false
    this.simplified_dimensions = options.simplified_dimensions || []
    this.simplified_dimensions.forEach((col) => {
      col.guid = sampleGuid()
    })
    this.computed_columns.forEach((col) => {
      col.guid = sampleGuid()
    })
    this.dimensions = this.simplified_dimensions.filter((x) => {
      if (x.status === 'DIMENSION') {
        let columnNamed = x.column.split('.')
        let k = indexOfObjWithSomeKeys(this.computed_columns, 'tableAlias', columnNamed[0], 'columnName', columnNamed[1])
        if (k >= 0) {
          x.isCC = true
          x.cc = this.computed_columns[k]
        }
        return x
      }
    })
    // 用在tableIndex上的列
    this.tableIndexColumns = this.simplified_dimensions.filter((x) => {
      if (x.status !== 'DIMENSION') {
        return x
      }
    })
    // 这里要将后端的结构数据解析为前端需要的
    let tempLooks = options.join_tables || options.lookups || []
    tempLooks.forEach((table) => {
      let itemJoin = table.join
      let temp_foreign_key = []
      let temp_primary_key = []
      let temp_op = []
      for (let i = 0; i < itemJoin.foreign_key.length; i++) {
        temp_foreign_key.push(itemJoin.foreign_key[i])
        temp_op.push('EQUAL')
      }
      for (let i = 0; i < itemJoin.primary_key.length; i++) {
        temp_primary_key.push(itemJoin.primary_key[i])
      }
      if (itemJoin.simplified_non_equi_join_conditions) {
        for (let i = 0; i < itemJoin.simplified_non_equi_join_conditions.length; i++) {
          temp_foreign_key.push(itemJoin.simplified_non_equi_join_conditions[i].foreign_key)
          temp_op.push(itemJoin.simplified_non_equi_join_conditions[i].op)
          temp_primary_key.push(itemJoin.simplified_non_equi_join_conditions[i].primary_key)
        }
      }
      table.join.foreign_key = objectClone(temp_foreign_key)
      table.join.primary_key = objectClone(temp_primary_key)
      table.join.op = objectClone(temp_op)
      table.flattenable = typeof table.flattenable !== 'undefined' ? table.flattenable !== 'normalized' ? 'flatten' : 'normalized' : 'flatten'
      delete table.join.non_equi_join_condition
      delete table.join.simplified_non_equi_join_conditions
    })
    this.lookups = objectClone(tempLooks)
    // this.lookups = options.lookups || options.join_tables || []
    this.all_measures = options.simplified_measures || []
    this.project = options.project
    this.maintain_model_type = options.maintain_model_type
    this.management_type = options.management_type || 'MODEL_BASED'
    this.globalDataSource = store.state.datasource.dataSource // 全局数据源表数据，新拖入时，需要从这里这个数据中取遍历
    // 能从模型详情接口里取到 simplified_tables 字段，就取这个字段，取不到的时候，取编辑模型时，模型使用到的 table 的信息这个接口里的返回
    this.datasource = []
    if (options.simplified_tables && options.simplified_tables.length) {
      this.datasource = options.simplified_tables
    } else {
      if (options.global_datasource && options.global_datasource[options.project] && options.global_datasource[options.project].length) {
        this.datasource = options.global_datasource[options.project]
      }
    }
    if (_) {
      this.vm = _
      this._mount = _mount // 挂载对象
      this.$set = _.$set
      this.$delete = _.$delete
      this.plumbTool = jsPlumbTool()
    } else {
      this.$set = function (obj, key, val) {
        obj[key] = val
      }
    }
    if (_mount) {
      this.$set(this._mount, 'computed_columns', this.computed_columns)
      this.$set(this._mount, 'tables', this.tables)
      this.$set(this._mount, 'simplified_dimensions', this.simplified_dimensions)
      this.$set(this._mount, 'all_measures', this.all_measures)
      this.$set(this._mount, 'dimensions', this.dimensions)
      this.$set(this._mount, 'zoom', this.canvas && this.canvas.zoom || modelRenderConfig.zoom)
      this.$set(this._mount, 'marginClient', this.canvas && this.canvas.marginClient || modelRenderConfig.marginClient)
      this.$set(this._mount, 'zoomXSpace', 0)
      this.$set(this._mount, 'zoomYSpace', 0)
      this.$set(this._mount, 'tableIndexColumns', this.tableIndexColumns)
      this.$set(this._mount, 'maintain_model_type', this.maintain_model_type)
      this.$set(this._mount, 'management_type', this.management_type)
      this.$set(this._mount, 'linkUsedColumns', this.linkUsedColumns)
      this.$set(this._mount, 'hasBrokenLinkedTable', false)
      this.$set(this._mount, 'broken_reason', options.broken_reason)
      this.$set(this._mount, 'all_named_columns', options.all_named_columns || [])
      this.$set(this._mount, 'anti_flatten_lookups', this.anti_flatten_lookups)
      this.$set(this._mount, 'anti_flatten_cc', this.anti_flatten_cc)
      this.$set(this._mount, 'has_base_agg_index', this.has_base_agg_index)
      this.$set(this._mount, 'has_base_table_index', this.has_base_table_index)
    }
    if (options.renderDom) {
      this.renderDom = this.vm.$el.querySelector(options.renderDom)
      this.plumbInstance = this.plumbTool.init(this.renderDom, this._mount.zoom / 10)
      this.plumbInstance.registerConnectionTypes({
        'broken': {
          paintStyle: { stroke: '#e73371', strokeWidth: 2 },
          hoverPaintStyle: { stroke: '#e73371', strokeWidth: 2 }
        },
        'normal': {
          paintStyle: { stroke: '#0988de', strokeWidth: 1 },
          hoverPaintStyle: { stroke: '#0988de', strokeWidth: 1 }
        }
      })
    }
    this.allConnInfo = {}
  }
}
