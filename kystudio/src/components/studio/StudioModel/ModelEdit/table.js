import { objectClone, sampleGuid } from 'util/index'
import { modelRenderConfig } from './config'
let zIndex = 10
// table 对象
class NTable {
  constructor (options) {
    // this.database = options.database // 数据库名
    // this.tablename = options.tablename // 表名
    this.name = options.table // 全称
    this.columns = objectClone(options.columns).filter((col) => {
      return !col.is_computed_column
    }) // 所有列
    this.columnPerSize = 50 // 每批显示条数
    this.hasMoreColumns = this.columns.length > 50 // 是否显示加载更多
    this.columnCurrentPage = 1 // 当前页数
    this.filterColumnChar = '' // 搜索字符串
    this.showColumns = this.columns.slice(0, this.columnPerSize)
    this.getOtherTableByGuid = options.getTableByGuid
    this.kind = options.kind ? options.kind : options.fact ? modelRenderConfig.tableKind.fact : modelRenderConfig.tableKind.lookup // table 类型
    this.joinInfo = {} // 链接对象
    this.guid = options.guid || sampleGuid() // identify id
    this.alias = options.alias || options.table // 别名
    this.source_type = options.source_type
    this.batch_table_identity = options.batch_table_identity // kafka关联的hive表名
    this._cache_search_columns = this.columns // 搜索结果缓存
    // this._parent = options._parent
    this.ST = null
    this.drawSize = Object.assign({}, { // 绘制信息
      left: 100,
      top: 20,
      width: modelRenderConfig.tableBoxWidth,
      height: modelRenderConfig.tableBoxHeight,
      limit: {
        height: [144],
        width: [modelRenderConfig.tableBoxWidth]
      },
      box: modelRenderConfig.rootBox,
      allowOutOfView: true,
      needCheckOutOfView: true,
      needCheckEdge: true,
      edgeOffset: 200, // 边缘检测提前缓冲距离
      isInLeftEdge: false, // 是否已经拖到边缘左侧
      isInRightEdge: false, // 是否已经拖到边缘右侧
      isOutOfView: false, // 是否出界
      zIndex: zIndex++,
      sizeChangeCb: (x, y, sw, sh, dragInfo) => {
        let _parent = options._parent
        _parent.windowWidth = sw
        _parent.windowHeight = sh
        if (!dragInfo.needCheckEdge && !dragInfo.needCheckOutOfView) { return }
        let left = dragInfo.left * (_parent.zoom / 10) + _parent.zoomXSpace
        // 判断是否到达边缘
        if (dragInfo.needCheckEdge) {
          if (left + dragInfo.width > sw - dragInfo.edgeOffset) {
            dragInfo.isInRightEdge = true
          } else if (left < dragInfo.edgeOffset) {
            dragInfo.isInLeftEdge = true
          } else {
            dragInfo.isInLeftEdge = false
            dragInfo.isInRightEdge = false
          }
        }
        // 判断是否超过边界
        this.checkIsOutOfView(_parent, dragInfo, sw, sh)
        options.plumbTool.lazyRender(() => {
          options.plumbTool.refreshPlumbInstance()
        })
      }
    }, options.drawSize)
  }
  loadMoreColumns () {
    this.columnCurrentPage++
    this.getPagerColumns()
  }
  getPagerColumns () {
    this.showColumns = this._cache_search_columns.slice(0, this.columnCurrentPage * this.columnPerSize)
    this.hasMoreColumns = this.showColumns.length < this._cache_search_columns.length
  }
  filterColumns () {
    let reg = new RegExp(this.filterColumnChar, 'i')
    this.showColumns = this.columns.filter((col) => {
      return this.filterColumnChar ? reg.test(col.name) : true
    })
    this._cache_search_columns = this.showColumns
    this.columnCurrentPage = 1
    this.getPagerColumns()
  }
  checkIsOutOfView (_parent, dragInfo, sw, sh) {
    clearTimeout(this.ST)
    this.ST = setTimeout(() => {
      const { left: mL, top: mT } = _parent.marginClient ?? { left: 0, top: 0 }
      let left = (dragInfo.left + mL) * (_parent.zoom / 10) + _parent.zoomXSpace
      let top = (dragInfo.top + mT) * (_parent.zoom / 10) + _parent.zoomYSpace
      let offset = 20
      if (left - offset > sw || left + dragInfo.width + offset < 0 || top + dragInfo.height + offset < 0 || top - offset > sh) {
        dragInfo.isOutOfView = true
      } else {
        dragInfo.isOutOfView = false
      }
    }, 200)
  }
  // 链接关系处理 (链接数据都存储在主键表上)
  addLinkData (fTable, linkColumnF, linkColumnP, type, opArr, precompute, selectedTRelation) {
    // this.addFreeLinkData(fTable, linkColumnF, linkColumnP, type)
    let fguid = fTable.guid
    let key = fguid + '$' + this.guid
    this.joinInfo[key] = {
      table: {
        guid: this.guid,
        columns: this.columns,
        name: this.name,
        alias: this.alias,
        kind: this.kind
      },
      join: {
        type: type,
        primary_key: [...linkColumnP],
        foreign_key: [...linkColumnF],
        op: [...opArr]
      },
      foreignTable: {
        guid: fTable.guid,
        name: fTable.name,
        alias: this.alias,
        kind: this.kind
      },
      kind: this.kind,
      join_relation_type: selectedTRelation,
      flattenable: precompute
    }
  }
  removeJoinInfo (fid) {
    delete this.joinInfo[fid + '$' + this.guid]
  }
  getColumnType (columnName) {
    let len = this.columns && this.columns.length || 0
    for (let i = len - 1; i >= 0; i--) {
      if (this.columns[i].name === columnName) {
        return this.columns[i].datatype
      }
    }
  }
  getJoinInfoByFGuid (fguid) {
    return this.joinInfo[fguid + '$' + this.guid]
  }
  getJoinInfo () {
    for (let k in this.joinInfo) {
      return this.joinInfo[k]
    }
  }
  getTableInViewOffset () {
    return {
      x: modelRenderConfig.beestViewPos[0] - this.drawSize.left,
      y: modelRenderConfig.beestViewPos[1] - this.drawSize.top
    }
  }
  _replaceAlias (alias, fullName) {
    return alias + '.' + fullName.split('.')[1]
  }
  // 获取符合元数据格式的JoinInfo
  getMetaJoinInfo (modelInstance) {
    let obj = {}
    for (let key in this.joinInfo) {
      let joinInfo = objectClone(this.joinInfo[key])
      if (joinInfo && joinInfo.table && joinInfo.join) {
        obj.table = joinInfo.table.name
        obj.alias = joinInfo.table.alias
        obj.join_relation_type = joinInfo.join_relation_type
        obj.flattenable = joinInfo.flattenable
        // obj.join = joinInfo.join
        // 这里要处理成后端要的结构
        let item = joinInfo.join.op
        let foreign_key = []
        let primary_key = []
        let opArr = []
        for (let i = 0; i < item.length; i++) {
          if (item[i] === 'EQUAL') {
            foreign_key.push(joinInfo.join.foreign_key[i])
            primary_key.push(joinInfo.join.primary_key[i])
          } else {
            opArr.push({foreign_key: joinInfo.join.foreign_key[i], primary_key: joinInfo.join.primary_key[i], op: item[i]})
          }
        }
        obj.join = {
          foreign_key: foreign_key,
          primary_key: primary_key,
          simplified_non_equi_join_conditions: opArr,
          type: joinInfo.join.type
        }
      } else {
        return null
      }
      return obj
    }
  }
  changeJoinAlias (modelInstance) {
    for (let j in this.joinInfo) {
      let joinInfo = this.joinInfo[j]
      if (joinInfo && joinInfo.table && joinInfo.join) {
        let fguid = joinInfo.foreignTable.guid
        let fntable = modelInstance.getTableByGuid(fguid)
        joinInfo.join.foreign_key = joinInfo.join.foreign_key.map((x) => {
          return this._replaceAlias(fntable.alias, x)
        })
        let pguid = joinInfo.table.guid
        let pntable = modelInstance.getTableByGuid(pguid)
        joinInfo.join.primary_key = joinInfo.join.primary_key.map((x) => {
          return this._replaceAlias(pntable.alias, x)
        })
        joinInfo.table.alias = pntable.alias
      }
    }
  }
  // 获取符合元数据格式的模型坐标位置信息
  getMetaCanvasInfo () {
    return {
      x: this.drawSize.left,
      y: this.drawSize.top,
      width: this.drawSize.width,
      height: this.drawSize.height
    }
  }
  // 改变连接关系
  changeLinkType (pid, type) {
    if (this.joinInfo[pid]) {
      this.joinInfo[pid].join = type
    }
  }
  // 获取所有的连接关系
  get links () {
    let _links = []
    for (var i in this.joinInfo) {
      _links.push(this.joinInfo[i])
    }
    return _links
  }
  // getLinkByFriGuid (fguid) {
  //   return this.freeJoinInfo[fguid + '$' + this.guid]
  // }
  // 获取某个主键表相关的连接
  getLinks () {
    return this.joinInfo[this.guid] || {}
  }
  getColumnObj (columnName) {
    for (let i = 0; i < this.columns.length; i++) {
      const col = this.columns[i]
      if (col.name === columnName) {
        return col
      }
    }
  }
  changeColumnProperty (columnName, key, val, _) {
    let col = this.getColumnObj(columnName)
    if (col) {
      if (_) {
        _.$set(col, key, val)
      } else {
        col[key] = val
      }
    }
  }
  changeColumnsProperty (key, val, _) {
    this.columns.forEach((col) => {
      if (_) {
        _.$set(col, key, val)
      } else {
        col[key] = val
      }
    })
  }
  // 可计算列处理
  // 维度处理
  // dimension处理
  // 展示信息处理
  renderLink () {
  }
  setPosition (x, y) {
    this.drawSize.x = x
    this.drawSize.y = y
  }
  setSize (w, h) {
    this.drawSize.width = w
    this.drawSize.height = h
  }
}

export default NTable
