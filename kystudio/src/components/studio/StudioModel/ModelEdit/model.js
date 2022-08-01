import Schama from './schama'
import NTable from './table.js'
import { parsePath, sampleGuid, indexOfObjWithSomeKey, indexOfObjWithSomeKeys, objectClone } from '../../../../util'
import { modelRenderConfig } from './config'
import { kylinConfirm } from 'util/business'
import ModelTree from './layout'
import $ from 'jquery'
// model 对象
class NModel extends Schama {
  constructor (options, _mount, _) {
    if (!options) {
      console.log('model init failed')
      return null
    }
    super(options, _mount, _)
    this.getSysInfo()
    this.render()
    this.getZoomSpace()
  }
  // 初始化数据和渲染数据
  render () {
    this._renderTable()
    this.vm && this.vm.$nextTick(() => {
      this._renderLinks()
      // 如果没有布局信息，就走自动布局程序
      if (!this.canvas) {
        this.renderPosition()
      }
      setTimeout(() => {
        this.getBrokenLinkedTable()
        // this._renderLabels()
      }, 1)
    })
    // renderDimension
    this.dimensions = this.dimensions.filter((x) => {
      x.datatype = x.isCC ? this.getCCColumnType(x.column) : this.getColumnType(x.column)
      let alias = x.column.split('.')[0]
      let guid = this._cacheAliasAndGuid(alias)
      x.table_guid = guid
      return x
    })
    // renderMeasure
    this.all_measures.forEach((x) => {
      x.guid = sampleGuid()
      if (x.parameter_value.length > 0) {
        x.parameter_value.forEach((y) => {
          if (y.type === 'column') {
            const convertedAlias = y.value.split('.')[0]
            const convertenTable = this.getTableByAlias(convertedAlias)
            const convertedGuid = convertenTable && convertenTable.guid
            y.table_guid = convertedGuid
          }
        })
      }
    })
    // render table index Columns
    this.tableIndexColumns = this.tableIndexColumns.filter((x) => {
      let alias = x.column.split('.')[0]
      let guid = this._cacheAliasAndGuid(alias)
      x.table_guid = guid
      return x
    })
    // render partition desc
    if (this.partition_desc.table) {
      let guid = this._cacheAliasAndGuid(this.partition_desc.table)
      this.partition_desc.table_guid = guid
    }
  }
  // 自动布局
  renderPosition () {
    // 自动布局前先理顺链表方向
    // this._arrangeLinks()
    const layers = this.autoCalcLayer()
    if (layers && layers.length > 0) {
      const baseL = modelRenderConfig.baseLeft
      const baseT = modelRenderConfig.baseTop
      const centerL = $(this.renderDom).width() / 2 - modelRenderConfig.tableBoxWidth / 2
      const moveL = layers[0].X - centerL
      this.renderDom.style.cssText += `margin-left: 0; margin-top: 0;`
      this._mount.marginClient.top = 0
      this._mount.marginClient.left = 0
      for (let k = 0; k < layers.length; k++) {
        var currentTable = this.getTableByGuid(layers[k].guid)
        currentTable.drawSize.left = baseL - moveL + layers[k].X
        currentTable.drawSize.top = baseT + layers[k].Y
        currentTable.drawSize.width = modelRenderConfig.tableBoxWidth
        currentTable.drawSize.height = modelRenderConfig.tableBoxHeight
        currentTable.checkIsOutOfView(this._mount, currentTable.drawSize, this._mount.windowWidth, this._mount.windowHeight)
      }
      this.vm.$nextTick(() => {
        this.plumbTool.refreshPlumbInstance()
      })
    }
  }
  getSysInfo () {
    if (this.renderDom && this._mount) {
      let boxDom = $(this.renderDom).parents(modelRenderConfig.rootBox).eq(0)
      this._mount.windowWidth = boxDom.width()
      this._mount.windowHeight = boxDom.height()
    }
  }
  getConn (pid, fid) {
    return this.allConnInfo[pid + '$' + fid]
  }
  collectLinkedColumn (pid, fid, pks, fks) {
    pks = pks || []
    fks = fks || []
    this.clearPFMark() // 清除之前的标识
    // 删除连线的情况
    if (pks.length === fks.length === 0) {
      delete this.linkUsedColumns[pid + fid]
    } else {
      this.linkUsedColumns[pid + fid] = [...pks, ...fks]
    }
    this.renderPFMark() // 重新标记主外键标识
  }
  clearPFMark () {
    for (let i in this.linkUsedColumns) {
      this.linkUsedColumns[i].forEach((col) => {
        let nameList = col.split('.')
        let alias = nameList[0]
        let columnName = nameList[1]
        let ntable = this.getTableByAlias(alias)
        if (ntable) {
          ntable.changeColumnProperty(columnName, 'isPFK', false, this)
        }
      })
    }
  }
  renderPFMark () {
    for (let i in this.linkUsedColumns) {
      this.linkUsedColumns[i].forEach((col) => {
        let nameList = col.split('.')
        let alias = nameList[0]
        let columnName = nameList[1]
        let ntable = this.getTableByAlias(alias)
        if (ntable) {
          ntable.changeColumnProperty(columnName, 'isPFK', true, this)
        }
      })
    }
  }
  // 连线
  renderLink (pid, fid) {
    var hasConn = this.getConn(pid, fid)
    let joinInfo = this.tables[pid].getJoinInfoByFGuid(fid)
    var primaryKeys = joinInfo && joinInfo.join.primary_key
    var foreignKeys = joinInfo && joinInfo.join.foreign_key
    let isBrokenLine = this.checkIsBrokenModelLink(pid, fid, primaryKeys, foreignKeys)
    if (hasConn) {
      // 如果渲染的时候发现连接关系都没有了，直接删除
      if (!primaryKeys || primaryKeys && primaryKeys.length === 1 && primaryKeys[0] === '') {
        this.removeRenderLink(hasConn)
      } else {
        this.setOverLayLabel(hasConn, isBrokenLine)
        this.plumbTool.refreshPlumbInstance()
      }
    } else {
      this.addPlumbPoints(pid, '', '', isBrokenLine)
      this.addPlumbPoints(fid, '', '', isBrokenLine)
      var conn = this.plumbTool.connect(pid, fid, (pid, fid, e) => {
        if (e.target && /close/.test(e.target.className)) {
          // 调用删除
          kylinConfirm(this.vm.$t('delConnTip'), null, this.vm.$t('delConnTitle')).then(() => {
            this.removeRenderLink(conn)
            if (this.vm.modelData.available_indexes_count > 0 && !this.vm.isIgnore) {
              this.vm.showChangeTips()
            }
          })
        } else {
          this.connClick(pid, fid)
        }
      }, {})
      this.setOverLayLabel(conn, isBrokenLine)
      this.plumbTool.refreshPlumbInstance()
      this.allConnInfo[pid + '$' + fid] = conn
    }
    this.collectLinkedColumn(pid, fid, primaryKeys, foreignKeys)
  }
  // 检测是否连接关系的列已经不在table里
  checkIsBrokenModelLink (pid, fid, primaryKeys, foreignKeys) {
    let ptable = this.getTableByGuid(pid)
    let primaryKeysLen = primaryKeys.length
    for (let i = 0; i < primaryKeysLen; i++) {
      let column = primaryKeys[i] && primaryKeys[i].replace(/^.*?\./, '')
      if (indexOfObjWithSomeKey(ptable.columns, 'name', column) < 0) {
        return true
      }
    }
    let ftable = this.getTableByGuid(fid)
    let foreignKeysLen = foreignKeys.length
    for (let i = 0; i < foreignKeysLen; i++) {
      let column = foreignKeys[i] && foreignKeys[i].replace(/^.*?\./, '')
      if (indexOfObjWithSomeKey(ftable.columns, 'name', column) < 0) {
        return true
      }
    }
    return false
  }
  getBrokenModelLinksKeys (guid, keys) {
    let table = this.getTableByGuid(guid)
    let keysLen = keys.length
    let result = []
    if (table) {
      for (let i = 0; i < keysLen; i++) {
        let column = keys[i] && keys[i].replace(/^.*?\./, '')
        if (indexOfObjWithSomeKey(table.columns, 'name', column) < 0) {
          result.push(keys[i])
        }
      }
    }
    return result
  }
  // 删除conn相关的主键的连接信息
  removeRenderLink (conn) {
    var fid = conn.sourceId
    var pid = conn.targetId
    delete this.allConnInfo[pid + '$' + fid]
    delete this.linkUsedColumns[pid]
    this.plumbTool.deleteConnect(conn)
    this.tables[pid].removeJoinInfo(fid)
    this.collectLinkedColumn(pid, fid, [], [])
    // delete this.tables[pid].joinInfo[fid + '$' + [pid]]
  }
  // 生成供后台使用的数据结构
  generateMetadata (ignoreAloneTableCheck) {
    // scd2 no-equal 禁止自动整理连接关系
    // this._arrangeLinks()
    return new Promise((resolve, reject) => {
      try {
        let metaData = {
          uuid: this.uuid,
          name: this.name,
          owner: this.owner,
          project: this.project,
          description: this.description,
          alias: this.alias,
          broken_reason: this.broken_reason
        }
        let factTable = this.getFactTable()
        if (factTable) {
          metaData.fact_table = factTable.name
        } else {
          return reject({errorKey: 'noFact'})
        }
        // 检查是否有脱离组织的table
        if (!ignoreAloneTableCheck) {
          let aloneCount = this._getAloneTableCount()
          if (aloneCount) {
            return reject({errorKey: 'hasAloneTable', aloneCount: aloneCount})
          }
        }
        metaData.join_tables = this._generateLookups()
        metaData.simplified_dimensions = this._generateAllColumns()
        metaData.simplified_measures = this._generateAllMeasureColumns()
        metaData.computed_columns = objectClone(this.computed_columns)
        metaData.last_modified = this.last_modified
        metaData.filter_condition = this.filter_condition
        metaData.partition_desc = this.partition_desc
        metaData.batch_partition_desc = this.batch_partition_desc
        metaData.multi_partition_desc = this.multi_partition_desc
        metaData.maintain_model_type = this._mount.maintain_model_type
        metaData.management_type = this.management_type
        metaData.with_second_storage = this.second_storage_enabled
        metaData.second_storage_size = this.second_storage_size
        metaData.canvas = this._generateTableRectData()
        // metaData = _filterData(metaData)
        resolve(metaData)
      } catch (e) {
        reject({errorKey: e})
      }
    })
  }
  _renderTable () {
    if (this.fact_table) {
      let factTableInfo = this._getTableOriginInfo(this.fact_table)
      let initTableOptions = {
        alias: this.fact_table.split('.')[1],
        columns: factTableInfo.columns,
        fact: factTableInfo.fact,
        kind: 'FACT',
        table: this.fact_table
      }
      initTableOptions.drawSize = this.getTableCoordinate(initTableOptions.alias) // 获取坐标信息
      this.addTable(initTableOptions)
      this.lookups.forEach((tableObj) => {
        let tableInfo = this._getTableOriginInfo(tableObj.table)
        let initTableInfo = {
          alias: tableObj.alias,
          columns: tableInfo.columns,
          fact: tableInfo.fact,
          kind: tableObj.kind,
          table: tableObj.table
        }
        initTableInfo.drawSize = this.getTableCoordinate(tableObj.alias) // 获取坐标信息
        let ntable = this.addTable(initTableInfo)
        // 获取外键表对象
        if (this.renderDom) {
          var ftable = this.getTableByAlias(tableObj.join.foreign_key[0].split('.')[0])
          ntable.addLinkData(ftable, tableObj.join.foreign_key, tableObj.join.primary_key, tableObj.join.type, tableObj.join.op, tableObj.flattenable, tableObj.join_relation_type)
        }
      })
    }
  }
  // 批量连线
  _renderLinks () {
    this.plumbTool.lazyRender(() => {
      for (var guid in this.tables) {
        var curNT = this.tables[guid]
        for (var i in curNT.joinInfo) {
          var primaryGuid = guid
          var foreignGuid = curNT.joinInfo[i].foreignTable.guid
          this.renderLink(primaryGuid, foreignGuid)
        }
      }
    })
  }
  /*
    按照传入的节点向下改变连线方向 eg: a -> b <- c <- d  改成  a -> b -> c -> d （递归执行）
    @guid 起点节点的guid
    @prevGuid 上次执行到的的guid
    @tartgetGuid 指定要改变顺序的对方节点
  */
  changeLinkDirect (guid, prevGuid, targetGuid) {
    let conns = this.getAllConnectsByGuid(guid)
    let curTable = this.getTableByGuid(guid)
    conns.forEach((conn) => {
      if (conn.sourceId !== prevGuid || targetGuid && conn.sourceId === targetGuid) {
        let newPrimaryTable = null
        if (conn.sourceId !== guid) { // 连线方向不正确
          let hisConnInfo = curTable.getJoinInfoByFGuid(conn.sourceId)
          newPrimaryTable = this.getTableByGuid(conn.sourceId)
          let newFrieignTable = this.getTableByGuid(conn.targetId)
          let hisTargetConnInfo = newPrimaryTable.getJoinInfoByFGuid(conn.targetId)
          // 将两个连接点上的相同关系连接信息归到一个节点
          if (hisTargetConnInfo && hisTargetConnInfo.join) {
            hisConnInfo.join.primary_key.push(...hisTargetConnInfo.join.foreign_key)
            hisConnInfo.join.foreign_key.push(...hisTargetConnInfo.join.primary_key)
            hisConnInfo.join.op.push(...hisTargetConnInfo.join.op)
          }
          // 删除
          this.removeRenderLink(conn)
          // 产生新的连接数据
          if (hisConnInfo) {
            newPrimaryTable.addLinkData(newFrieignTable, hisConnInfo.join.primary_key, hisConnInfo.join.foreign_key, hisConnInfo.join.type, hisConnInfo.join.op, hisConnInfo.flattenable, hisConnInfo.join_relation_type)
          }
          // 重新连接
          this.renderLink(newPrimaryTable.guid, newFrieignTable.guid)
        } else { // 连线方向正确
          newPrimaryTable = this.getTableByGuid(conn.targetId)
        }
        if (!targetGuid) {
          this.changeLinkDirect(newPrimaryTable.guid, guid)
        }
      }
    })
  }
  /*
    提前检测连接是否形成闭环 递归执行
    @startNode 连接的起始点
    @endNode 连接的结束点
    @sourceId 检测点
    @prevSourceId 上一次的监测点
  */
  checkLinkCircle (startNode, endNode, sourceId, prevSourceId) {
    sourceId = sourceId || startNode
    prevSourceId = prevSourceId || startNode
    let conns = this.getAllConnectsByGuid(sourceId)
    for (let k = 0; k < conns.length; k++) {
      let conn = conns[k]
      let nextNode = conn.sourceId === sourceId ? conn.targetId : conn.sourceId
      if (prevSourceId && prevSourceId === nextNode) {
        continue
      }
      if (nextNode === endNode && startNode !== sourceId) { // 第一层检测跳过，直接连接的不算做闭环
        return true
      }
      if (this.checkLinkCircle(startNode, endNode, nextNode, sourceId)) {
        return true
      }
    }
  }
  // 整理用户随意连线的表
  _arrangeLinks () {
    if (!this.fact_table) {
      return
    }
    let factTable = this.getTableByAlias(this.fact_table.split('.')[1])
    let factGuid = factTable.guid
    this.changeLinkDirect(factGuid)
  }
  _guidCache = {}
  _cacheAliasAndGuid (alias) {
    let guid = ''
    if (this._guidCache[alias]) {
      guid = this._guidCache[alias]
    } else {
      let ntable = this.getTableByAlias(alias)
      if (ntable) {
        this._guidCache[alias] = ntable.guid
        guid = this._guidCache[alias]
      }
    }
    return guid
  }
  // 获取非fact中未作为主键的表（据此可判断该表未最终连接到主树上）
  _getAloneTableCount () {
    let wholeConnect = this._generateLookups()
    let wholeConnCount = wholeConnect.length
    let tableCounts = Object.keys(this.tables).length
    let aloneTablesCount = tableCounts - wholeConnCount - 1
    return aloneTablesCount || 0
  }
  _generateLookups () {
    let result = []
    let factTable = this.getFactTable()
    let _recursionLookup = (guid) => {
      if (guid) {
        let conns = this.getConnByFKTableGuid(guid)
        conns && conns.forEach((conn) => {
          let pguid = conn.targetId
          let t = this.getTableByGuid(pguid)
          var joinInfo = t && t.getMetaJoinInfo(this)
          if (joinInfo) {
            result.push(joinInfo)
          }
          _recursionLookup(pguid)
        })
      }
    }
    _recursionLookup(factTable && factTable.guid)
    return result
  }
  _generateAllColumns () {
    let allNamedColumns = objectClone([...this._mount.dimensions, ...this.tableIndexColumns])
    // 移除前端业务字断
    allNamedColumns.forEach((col) => {
      delete col.guid
      delete col.table_guid
      delete col.isCC
      delete col.cc
    })
    return allNamedColumns
  }
  _generateAllMeasureColumns () {
    let allMeasures = objectClone(this._mount.all_measures)
    // 移除前端业务字断
    allMeasures.forEach((col) => {
      delete col.guid
      if (col.parameter_value && col.parameter_value.length) {
        col.parameter_value.forEach((k) => {
          if (k.table_guid) {
            delete k.table_guid
          }
        })
      }
    })
    return allMeasures
  }
  _generateTableRectData () {
    let canvasInfo = {
      coordinate: {}
    }
    for (let t in this.tables) {
      let ntable = this.tables[t]
      canvasInfo.coordinate[ntable.alias] = ntable.getMetaCanvasInfo()
    }
    canvasInfo.zoom = this._mount.zoom
    canvasInfo.marginClient = this._mount.marginClient
    return canvasInfo
  }
  // end
  // 判断是否table有关联的链接
  getAllConnectsByGuid (guid) {
    let result = []
    var reg = new RegExp('^' + guid + '\\$|\\$' + guid + '$')
    for (let i in this.allConnInfo) {
      if (reg.test(i)) {
        result.push(this.allConnInfo[i])
      }
    }
    return result
  }
  getConnByFKTableGuid (guid) {
    let result = []
    var reg = new RegExp('\\$' + guid + '$')
    for (let i in this.allConnInfo) {
      if (reg.test(i)) {
        result.push(this.allConnInfo[i])
      }
    }
    return result
  }
  getConnByPKTableGuid (guid) {
    let result = []
    var reg = new RegExp('^' + guid + '\\$')
    for (let i in this.allConnInfo) {
      if (reg.test(i)) {
        result.push(this.allConnInfo[i])
      }
    }
    return result
  }
  _replaceAlias (alias, fullName) {
    return alias + '.' + fullName.split('.')[1]
  }
  // private 更新所有measure里的alias
  _updateAllMeasuresAlias () {
    this.all_measures.forEach((x) => {
      if (x.parameter_value.length > 0) {
        x.parameter_value.forEach((y) => {
          if (y.table_guid) {
            const guid = y.table_guid
            const nTable = guid && this.getTableByGuid(guid)
            if (nTable) {
              const alias = nTable.alias
              y.value = alias + '.' + y.value.split('.')[1]
            }
          }
        })
      }
    })
  }
  // 更新连接关系里的别名
  _updateLinkColumnAliasInfo () {
    for (let key in this.tables) {
      let t = this.tables[key]
      if (t.alias !== this.fact_table) {
        var joinInfo = t.getJoinInfo()
        if (joinInfo) {
          let pid = joinInfo.table.guid
          let fid = joinInfo.foreignTable.guid
          let nptable = this.getTableByGuid(pid)
          let nftable = this.getTableByGuid(fid)
          joinInfo.table.alias = nptable.alias
          joinInfo.foreignTable.alias = nftable.alias
          joinInfo.join.primary_key = joinInfo.join.primary_key.map((x) => {
            return x.replace(/^.*?\./, nptable.alias + '.')
          })
          joinInfo.join.foreign_key = joinInfo.join.foreign_key.map((x) => {
            return x.replace(/^.*?\./, nftable.alias + '.')
          })
          this.linkUsedColumns[pid + fid] = [...joinInfo.join.primary_key, ...joinInfo.join.foreign_key]
        }
      }
    }
  }
  // 重新调整alias导致数据改变
  _changeAliasRelation () {
    // 更新join信息
    // 跟 _updateLinkColumnAliasInfo 重复
    // Object.values(this.tables).forEach((t) => {
    //   t.changeJoinAlias(this)
    // })
    let replaceFuc = (x, key) => {
      let guid = x.table_guid
      let ntable = this.getTableByGuid(guid)
      x.column = this._replaceAlias(ntable.alias, x.column)
    }
    // 改变dimension列的alias
    this._mount.dimensions.forEach(replaceFuc)
    // 改变tableindex列的alias
    this.tableIndexColumns.forEach(replaceFuc)
    // 改变可计算列的alias
    this._mount.computed_columns.forEach((x) => {
      let guid = x.table_guid
      let ntable = this.getTableByGuid(guid)
      if (ntable) {
        x.tableAlias = ntable.alias
      }
    })
    this._updateAllMeasuresAlias()
    this._updateLinkColumnAliasInfo()
  }
  // 别名修改
  changeAlias () {
    this._changeAliasRelation()
  }
  _renderLabels () {
    for (var i in this.allConnInfo) {
      this.setOverLayLabel(this.allConnInfo[i])
    }
  }
  search (keywords) {
    var stables = this.searchTable(keywords)
    var smeasures = this.searchMeasure(keywords)
    var sdimensions = this.searchDimension(keywords)
    var sjoins = this.searchJoin(keywords)
    var scolumns = this.searchColumn(keywords)
    return [].concat(stables, smeasures, sdimensions, sjoins, scolumns)
  }
  // search
  searchTable (keywords) {
    let filterResult = Object.values(this.tables).filter((x) => {
      return this.searchRule(x.alias, keywords)
    })
    return this.mixResult(filterResult, 'table', 'alias')
  }
  searchMeasure (keywords) {
    let filterResult = this._mount.all_measures.filter((x) => {
      return this.searchRule(x.name, keywords)
    })
    return this.mixResult(filterResult, 'measure', 'name')
  }
  searchDimension (keywords) {
    let filterResult = this._mount.dimensions.filter((x) => {
      return this.searchRule(x.name, keywords)
    })
    return this.mixResult(filterResult, 'dimension', 'name')
  }
  searchJoin (keywords) {
    let joinReg = /^(join|left\s*join|inner\s*join)$/i
    let leftJoinReg = /^left\s*join$/i
    let innerJoinReg = /^inner\s*join$/i
    let filterResult = []
    if (joinReg.test(keywords)) {
      Object.values(this.allConnInfo).forEach((conn) => {
        let pguid = conn.targetId
        let ptable = this.getTableByGuid(pguid)
        let joinInfo = ptable.getJoinInfo()
        if (leftJoinReg.test(keywords)) {
          if (joinInfo.join.type === 'LEFT') {
            filterResult.push(ptable)
          }
        } else if (innerJoinReg.test(keywords)) {
          if (joinInfo.join.type === 'INNER') {
            filterResult.push(ptable)
          }
        } else {
          filterResult.push(ptable)
        }
      })
    }
    return this.mixResult(filterResult, 'join', 'alias')
  }
  searchColumn (keywords) {
    var columnsResult = []
    for (var i in this.tables) {
      let columns = this.tables[i].columns
      columns && columns.forEach((co) => {
        co.full_colname = this.tables[i].alias + '.' + co.name
        co.table_guid = this.tables[i].guid
      })
      columnsResult.push(...columns)
    }
    columnsResult = columnsResult.filter((col) => {
      return this.searchRule(col.full_colname, keywords)
    })
    return this.mixResult(columnsResult, 'column', 'full_colname', keywords)
  }
  searchRule (content, keywords) {
    const key = keywords.replace(/[?()]/g, (v) => `\\${v}`)
    var reg = new RegExp(key, 'i')
    return reg.test(content)
  }
  // 混合结果信息
  mixResult (data, kind, key) {
    let result = []
    let actionsConfig = modelRenderConfig.searchAction[kind]
    actionsConfig.forEach((a) => {
      let i = 0
      data && data.forEach((t) => {
        if (i++ < modelRenderConfig.searchCountLimit) {
          let item = this.renderSearchResult(t, key, kind, a)
          if (item) {
            result.push(item)
          }
        }
      })
    })
    return result
  }
  // 数据结构定制化
  renderSearchResult (t, key, kind, a) {
    let item = {name: t[key], kind: kind, action: a.action, i18n: a.i18n, more: t}
    if (kind === 'table' && a.action === 'tableeditjoin' || kind === 'join' && a.action === 'editjoin') {
      let joinInfo = t.getJoinInfo()
      if (joinInfo) {
        item.extraInfo = ' <span class="jtk-overlay">' + joinInfo.join.type + '</span> ' + joinInfo.foreignTable.name
      } else {
        return ''
      }
    }
    return item
  }
  checkTableCanSwitchFact (guid) {
    let factTable = this.getFactTable()
    if (factTable) {
      return false
    }
    return true
  }
  checkTableCanDel (guid) {
    if (this._checkTableUseInConn(guid)) {
      return false
    }
    if (this._checkTableUseInDimension(guid)) {
      return false
    }
    if (this._checkTableUseInMeasure(guid)) {
      return false
    }
    if (this._checkTableUseInCC(guid)) {
      return false
    }
    if (this._checkTableUseInPartition(guid)) {
      return false
    }
    return true
  }
  _checkTableUseInConn (guid) {
    let conns = this.getAllConnectsByGuid(guid)
    if (conns.length) {
      return true
    }
  }
  _checkTableUseInDimension (guid) {
    return indexOfObjWithSomeKey(this.dimensions, 'table_guid', guid) >= 0
  }
  _checkTableUseInMeasure (guid) {
    let isUseInMeasure = false
    for (let i = 0; i < this._mount.all_measures.length; i++) {
      if (indexOfObjWithSomeKey(this._mount.all_measures[i].parameter_value, 'table_guid', guid) >= 0) {
        isUseInMeasure = true
        break
      }
    }
    return isUseInMeasure
  }
  _checkTableUseInCC (guid) {
    return indexOfObjWithSomeKey(this._mount.computed_columns, 'table_guid', guid) >= 0
  }
  _checkTableUseInPartition (guid) {
    return this.partition_desc.table_guid === guid
  }
  delTable (guid) {
    return new Promise((resolve, reject) => {
      let conns = this.getAllConnectsByGuid(guid)
      conns && conns.forEach((conn) => {
        this.removeRenderLink(conn)
      })
      this._delTableRelated(guid)
      this.$delete(this.tables, guid)
      return resolve()
    })
  }
  _delTableRelated (guid) {
    let ntable = this.getTableByGuid(guid)
    if (ntable) {
      // 如何删除的是事实表，清空this.fact_table的值
      if (this.fact_table && ntable.name === this.fact_table && ntable.alias === this.fact_table.split('.')[1]) {
        this.fact_table = ''
      }
      let alias = ntable.alias
      // 删除对应的 dimension
      this._delDimensionByAlias(alias)
      // 删除对应的 measure
      this._delMeasureByAlias(alias)
      // 删除对应的 tableindex
      this._delTableIndexByAlias(alias)
      // 删除对应的 cc
      // this._delCCByAlias(alias)
      // 删除对应的partition
      this._delTableRelatedPartitionInfo(ntable)
    }
  }
  getTable (key, val) {
    for (var i in this.tables) {
      if (this.tables[i][key] === val) {
        return this.tables[i]
      }
    }
  }
  getTables (key, val) {
    let result = []
    for (var i in this.tables) {
      if (this.tables[i][key] === val) {
        result.push(this.tables[i])
      }
    }
    return result
  }
  getTableColumns () {
    let result = []
    for (var i in this.tables) {
      let columns = this.tables[i].columns
      columns && columns.forEach((col) => {
        col.guid = i // 永久指纹
        col.table_alias = this.tables[i].alias // 临时
        col.full_colname = col.table_alias + '.' + col.name
        result.push(col)
      })
    }
    return result
  }
  getTableCoordinate (alias) {
    if (this.canvas) {
      for (let i in this.canvas.coordinate) {
        if (i === alias) {
          let _info = this.canvas.coordinate[i]
          return {
            left: _info.x,
            top: _info.y,
            width: _info.width,
            height: _info.height
          }
        }
      }
    }
  }
  getComputedColumns () {
    return this._mount ? this._mount.computed_columns : this.computed_columns
  }
  _updateAllMeasuresCCToNewFactTable () {
    let factTable = this.getFactTable()
    this.all_measures.forEach((x) => {
      if (x.parameter_value.length > 0) {
        x.parameter_value.forEach((y) => {
          if (y.type === 'column') {
            let cc = this.getCCObj(y.value)
            if (cc && factTable) {
              y.table_guid = factTable.guid
              y.value = factTable.alias + '.' + y.value.split('.')[1]
            }
          }
        })
      }
    })
  }
  _updateAllNamedColumnsCCToNewFactTable () {
    let factTable = this.getFactTable()
    let replaceFuc = (x, key) => {
      let cc = this.getCCObj(x.column)
      if (cc && factTable) {
        x.column = this._replaceAlias(factTable.alias, x.column)
        x.table_guid = factTable.guid
      }
    }
    this._mount.dimensions.forEach(replaceFuc)
    // 改变tableindex列的alias
    this.tableIndexColumns.forEach(replaceFuc)
  }
  _updateCCToNewFactTable () {
    let factTable = this.getFactTable()
    if (factTable) {
      this._mount.computed_columns.forEach((x) => {
        x.table_guid = factTable.guid
        x.tableIdentity = factTable.name
        x.tableAlias = factTable.tableAlias
      })
    }
  }
  // 移除和某个表相关的partition信息
  _delTableRelatedPartitionInfo (t, column) {
    if (this.partition_desc.partition_date_column && t && this.partition_desc.partition_date_column.split('.')[0] === t.alias && (!column || column && this.partition_desc.partition_date_column.split('.')[1] === column)) {
      this.partition_desc.table_guid = null
      this.partition_desc.partition_date_column = null
      this.partition_desc.partition_time_column = null
    }
  }
  changeTableType (t) {
    t.kind = t.kind === modelRenderConfig.tableKind.fact ? modelRenderConfig.tableKind.lookup : modelRenderConfig.tableKind.fact
    this.setUniqueAlias(t)
    // 如果切换的是fact
    if (t.kind === modelRenderConfig.tableKind.fact) {
      // 将所有和fact相关的ccdimension，ccmeasure，cctableindex,cclist 换上新的fact 指纹
      this.fact_table = t.name
      this._updateAllMeasuresCCToNewFactTable()
      this._updateAllNamedColumnsCCToNewFactTable()
      this._updateCCToNewFactTable()
      // this._arrangeLinks()
    } else if (t.name === this.fact_table) {
      this.fact_table = '' // fact 改为 lookup 需要将它设置为空
    }
    // 删除对应的partition
    this._delTableRelatedPartitionInfo(t)
    // 改变别名且替换掉所有关联的别名信息
    this.changeAlias()
  }
  _checkSameAlias (guid, newAlias) {
    var hasAlias = 0
    Object.values(this.tables).forEach(function (table) {
      if (table.guid !== guid) {
        if (table.alias.toUpperCase() === newAlias.toUpperCase()) {
          hasAlias++
        }
      }
    })
    return hasAlias
  }
  _createUniqueName (guid, alias) {
    if (alias && guid) {
      var sameCount = this._checkSameAlias(guid, alias)
      var finalAlias = alias.toUpperCase().replace(/[^a-zA-Z_0-9]/g, '')
      if (sameCount === 0) {
        return finalAlias
      } else {
        while (this._checkSameAlias(guid, finalAlias + '_' + sameCount)) {
          sameCount++
        }
        return finalAlias + '_' + sameCount
      }
    }
  }
  setUniqueAlias (table) {
    // fact情况的特殊处理
    // fact的别名只能使用默认的自己的 table 名
    let pureTableName = table.name.split('.')[1]
    if (table.kind === modelRenderConfig.tableKind.fact && pureTableName !== table.alias) {
      let sameTable = this.getTables('alias', pureTableName)
      for (let i = 0; i < sameTable.length; i++) {
        const t = sameTable[i]
        if (t.guid !== table.guid) {
          t.alias = table.alias
          break
        }
      }
      table.alias = pureTableName
    } else {
      var uniqueName = this._createUniqueName(table.guid, table.alias)
      this.$set(table, 'alias', uniqueName)
    }
  }
  // 设置当前最上层的table（zindex）
  setIndexTop (data, t, path) {
    let maxZindex = -1
    var pathObj = parsePath(path)
    data.forEach((x) => {
      if (pathObj(x).zIndex > maxZindex) {
        maxZindex = pathObj(x).zIndex
      }
      if (pathObj(x).zIndex > pathObj(t).zIndex) {
        pathObj(x).zIndex--
      }
    })
    pathObj(t).zIndex = maxZindex
  }
  setZoom (zoom) {
    this.plumbTool.setZoom(zoom / 10)
    this.getZoomSpace()
  }
  // 放大视图
  addZoom () {
    var nextZoom = this._mount.zoom + 1 > 10 ? 10 : this._mount.zoom += 1
    this.plumbTool.setZoom(nextZoom / 10)
    this.getZoomSpace()
  }
  // 缩小视图
  reduceZoom () {
    var nextZoom = this._mount.zoom - 1 < 4 ? 4 : this._mount.zoom -= 1
    this.plumbTool.setZoom(nextZoom / 10)
    this.getZoomSpace()
  }
  getZoomSpace () {
    if (this.renderDom) {
      this._mount.zoomXSpace = $(this.renderDom).width() * (1 - this._mount.zoom / 10) / 2
      this._mount.zoomYSpace = $(this.renderDom).height() * (1 - this._mount.zoom / 10) / 2
    }
  }
  bindConnClickEvent (cb) {
    this.connClick = (pid, fid) => {
      var pntable = this.getTableByGuid(pid)
      var fntable = this.getTableByGuid(fid)
      cb && cb(pntable, fntable)
    }
  }
  moveModelPosition (x, y) {
    if (x !== +x || y !== +y) {
      return
    }
    this._mount.marginClient.left += x
    this._mount.marginClient.top += y
    for (var i in this.tables) {
      var curTable = this.tables[i]
      // curTable.drawSize.left += x
      // curTable.drawSize.top += y
      curTable.checkIsOutOfView(this._mount, curTable.drawSize, this._mount.windowWidth, this._mount.windowHeight)
    }
    this.vm.$nextTick(() => {
      this.plumbTool.refreshPlumbInstance()
    })
  }
  // 将某个table移动到可视区域
  setTableInView (guid) {
    let nTable = this.getTableByGuid(guid)
    if (nTable) {
      let offset = nTable.getTableInViewOffset()
      this.moveModelPosition(offset.x, offset.y)
    }
  }
  // 将连接移动到可视区域
  setLinkInView (pid, fid) {
    let ptable = this.getTableByGuid(pid)
    let ftable = this.getTableByGuid(fid)
    let offsetLeft = ptable.drawSize.left - ftable.drawSize.left
    let offsetTop = ptable.drawSize.top - ftable.drawSize.top
    this.setTableInView(pid)
    this.moveModelPosition(offsetLeft / 2, offsetTop / 2)
  }
  addTable (options) {
    if (!this.tables[options.alias]) {
      let tableInfo = this._getTableOriginInfo(options.table)
      options.columns = tableInfo.columns
      options.plumbTool = this.plumbTool
      options.source_type = tableInfo.source_type
      options.fact = tableInfo.fact
      options.batch_table_identity = tableInfo.batch_table_identity
      if (tableInfo.source_type === 1 && !options.isSecStorageEnabled) {
        if (!this.getFactTable()) {
          options.kind = modelRenderConfig.tableKind.fact
          this.fact_table = options.table // kafka的table直接为fact表
        } else {
          this.vm.$message({ type: 'info', message: this.vm.$t('kafakFactTips'), showClose: true, duration: 0 })
          return
        }
      } else if (tableInfo.source_type === 1 && options.isSecStorageEnabled) {
        this.vm.$message({ type: 'info', message: this.vm.$t('kafakDisableSecStorageTips'), showClose: true, duration: 0 })
        return
      } else {
        if (options.fact) {
          if (this.getFactTable()) {
            options.kind = modelRenderConfig.tableKind.lookup // 如果已经存在fact表了，那再拖入一个fact默认设置为lookup
          } else {
            this.fact_table = options.table
          }
        }
      }
      options._parent = this._mount
      let table = new NTable(options)
      // this.tables[options.alias] = table
      if (this.vm) {
        this.vm.$set(this._mount.tables, table.guid, table)
      } else {
        this.tables[table.guid] = table
      }
      if (this.renderDom) {
        this.plumbTool.draggable([table.guid])
      }
      this.setUniqueAlias(table)
      return table
    }
    return this.tables[options.alias]
  }
  _getTableOriginInfo (tableFullName) {
    let getTableBySource = () => {
      if (this.datasource) {
        let i = indexOfObjWithSomeKey(this.datasource, 'table', tableFullName)
        if (i >= 0) {
          return {...this.datasource[i], columns: this.datasource[i].columns.map(it => ({...it, column: it.name}))}
        }
      }
      return []
    }
    let tableNamed = tableFullName.split('.')
    // 先看当前模型中使用的tables 字段是否能匹配上，如果匹配上了
    const currentDatasource = this.globalDataSource[this.project]
    let result = null
    if (currentDatasource) {
      let i = indexOfObjWithSomeKeys(currentDatasource, 'database', tableNamed[0], 'name', tableNamed[1])
      if (i >= 0) {
        let globalTableInfo = currentDatasource[i]
        globalTableInfo.table = globalTableInfo.database + '.' + globalTableInfo.name
        globalTableInfo.columns.forEach(item => {
          item['column'] = item.name
        })
        let k = indexOfObjWithSomeKey(this.datasource, 'table', tableFullName)
        if (k < 0) {
          this.datasource.push(globalTableInfo)
        }
        result = globalTableInfo
      }
    }
    result = result || getTableBySource()
    if (!result) {
      throw new Error('badModel')
    }
    return result
  }
  getTableByGuid (guid) {
    for (var i in this.tables) {
      if (i === guid) {
        return this.tables[i]
      }
    }
  }
  getTableByAlias (alias) {
    for (var i in this.tables) {
      if (this.tables[i].alias === alias) {
        return this.tables[i]
      }
    }
  }
  getColumnType (fullName) {
    let named = fullName.split('.')
    if (named && named.length) {
      let alias = named[0]
      let tableName = named[1]
      let ntable = this.getTableByAlias(alias)
      return ntable && ntable.getColumnType(tableName)
    }
  }
  getCCColumnType (fullName) {
    let cc = this.getCCObj(fullName)
    if (cc) {
      return cc.datatype
    }
  }
  getCCObj () {
    let column = ''
    let alias = ''
    if (arguments.length === 1) {
      let named = arguments[0].split('.')
      alias = named[0]
      column = named[1]
    } else if (arguments.length === 2) {
      alias = arguments[0]
      column = arguments[1]
    }
    let i = indexOfObjWithSomeKeys(this.computed_columns, 'columnName', column, 'tableAlias', alias)
    if (i >= 0) {
      return this.computed_columns[i]
    }
  }
  getFactTable () {
    for (var i in this.tables) {
      if (this.tables[i].kind === modelRenderConfig.tableKind.fact && this.tables[i].name === this.fact_table) {
        return this.tables[i]
      }
    }
  }
  // 添加维度
  addDimension (dimension) {
    return new Promise((resolve, reject) => {
      if (indexOfObjWithSomeKey(this._mount.dimensions, 'name', dimension.name) <= 0) {
        dimension.guid = sampleGuid()
        this._mount.dimensions.push(dimension)
        resolve(dimension)
      } else {
        reject()
      }
    })
  }
  // 编辑dimension name重复检测
  checkSameEditDimensionName (dimension) {
    let name = dimension.name
    for (let k = 0; k < this._mount.dimensions.length; k++) {
      if (this._mount.dimensions[k].guid !== dimension.guid && name === this._mount.dimensions[k].name) {
        return false
      }
    }
    return true
  }
  checkSameEditDimensionColumn (dimension) {
    let column = dimension.column
    for (let k = 0; k < this._mount.dimensions.length; k++) {
      if (this._mount.dimensions[k].guid !== dimension.guid && column === this._mount.dimensions[k].column) {
        return false
      }
    }
    return true
  }
  checkSameEditMeasureColumn (measure) {
    let column = measure.parameterValue
    let corrCol = measure.convertedColumns[0]
    let expression = measure.expression
    if (measure.expression.indexOf('SUM') !== -1) {
      expression = 'SUM'
    }
    if (measure.expression.indexOf('COUNT(constant)') !== -1 || measure.expression.indexOf('COUNT(column)') !== -1) {
      expression = 'COUNT'
    }
    for (let k = 0; k < this._mount.all_measures.length; k++) {
      if (this._mount.all_measures[k].guid !== measure.guid && this._mount.all_measures[k].expression === expression) {
        if (expression !== 'CORR' && column.value === this._mount.all_measures[k].parameter_value[0].value) {
          return false
        } else if (indexOfObjWithSomeKey(this._mount.all_measures[k].parameter_value, 'value', column.value) !== -1 && corrCol && indexOfObjWithSomeKey(this._mount.all_measures[k].parameter_value, 'value', corrCol.value) !== -1) {
          return false
        }
      }
    }
    return true
  }
  // 添加度量
  editDimension (dimension) {
    return new Promise((resolve, reject) => {
      if (!this.checkSameEditDimensionName(dimension)) {
        return reject()
      }
      let index = indexOfObjWithSomeKey(this._mount.dimensions, 'guid', dimension.guid)
      Object.assign(this._mount.dimensions[index], dimension)
      resolve()
    })
  }
  delDimension (name) {
    let dimensionIndex = indexOfObjWithSomeKey(this._mount.dimensions, 'name', name)
    this._mount.dimensions.splice(dimensionIndex, 1)
  }
  // 删除某个别名table下的所有 Dimension 用到的列
  _delDimensionByAlias (alias, column) {
    let dimensions = this._mount.dimensions.filter((item) => {
      return !(item.column && item.column.split('.')[0] === alias && (!column || column && item.column.split('.')[1] === column))
    })
    this._mount.dimensions.splice(0, this._mount.dimensions.length)
    this._mount.dimensions.push(...dimensions)
  }
  // 删除某个别名下的table里的所有的table index 用到的列
  _delTableIndexByAlias (alias, column) {
    let tableIndexColumns = this.tableIndexColumns.filter((item) => {
      return !(item.column && item.column.split('.')[0] === alias && (!column || column && item.column.split('.')[1] === column))
    })
    this.tableIndexColumns.splice(0, this.tableIndexColumns.length)
    this.tableIndexColumns.push(...tableIndexColumns)
  }
  // 删除某个别名下的 Measure 里的用到的列
  _delMeasureByAlias (alias, column) {
    let measures = this._mount.all_measures.filter((item) => {
      let isUseAlias = false
      if (item.parameter_value && item.parameter_value.length > 0) {
        for (let i = 0; i < item.parameter_value.length; i++) {
          if (`${item.parameter_value[i].value}`.split('.')[0] === alias && (!column || column && `${item.parameter_value[i].value}`.split('.')[1] === column)) {
            isUseAlias = true
            break
          }
        }
      }
      return !isUseAlias
    })
    this._mount.all_measures.splice(0, this._mount.all_measures.length)
    this._mount.all_measures.push(...measures)
  }
  delMeasure (name) {
    let measureIndex = indexOfObjWithSomeKey(this._mount.all_measures, 'name', name)
    this._mount.all_measures.splice(measureIndex, 1)
  }
  // 添加度量
  addMeasure (measureObj) {
    return new Promise((resolve, reject) => {
      if (indexOfObjWithSomeKey(this._mount.all_measures, 'name', measureObj.name) <= 0) {
        measureObj.guid = sampleGuid()
        this._mount.all_measures.push(measureObj)
        resolve(measureObj)
      } else {
        reject()
      }
    })
  }
  // 编辑度量
  editMeasure (measureObj) {
    return new Promise((resolve, reject) => {
      let index = indexOfObjWithSomeKey(this._mount.all_measures, 'guid', measureObj.guid)
      Object.assign(this._mount.all_measures[index] || {}, measureObj)
      resolve()
    })
  }
  // 检查是否有同名, 通过重名检测返回true
  checkSameCCName (name) {
    return indexOfObjWithSomeKey(this._mount.computed_columns, 'columnName', name) < 0
  }
  generateCCMeta (ccObj) {
    let factTable = this.getFactTable()
    if (factTable) {
      let ccBase = {
        tableIdentity: factTable.name,
        tableAlias: factTable.alias,
        guid: sampleGuid()
      }
      Object.assign(ccBase, ccObj)
      return ccBase
    }
  }
  // 添加CC
  addCC (ccObj) {
    return new Promise((resolve, reject) => {
      if (this.checkSameCCName(ccObj.columnName)) {
        let ccMeta = this.generateCCMeta(ccObj)
        this._mount.computed_columns.push(ccMeta)
        resolve(ccMeta)
      } else {
        reject()
      }
    })
  }
  // 编辑CC
  editCC (ccObj) {
    return new Promise((resolve, reject) => {
      let hasEdit = false
      this._mount.computed_columns.forEach((c) => {
        if (c.guid === ccObj.guid) {
          Object.assign(c, ccObj)
          hasEdit = true
          resolve(c)
        }
      })
      if (!hasEdit) {
        reject()
      }
    })
  }
  _delCCRelated (t, column) {
    let alias = t.alias
    // 删除dimension里的cc
    this._delDimensionByAlias(alias, column)
    // 删除measure里的cc
    this._delMeasureByAlias(alias, column)
    // 删除table index里的cc
    this._delTableIndexByAlias(alias, column)
    // 删除partition里的cc
    this._delTableRelatedPartitionInfo(t, column)
  }
  // 删除可计算列
  delCC (cc) {
    let ccColumnName = typeof cc === 'object' ? cc.columnName : cc
    return new Promise((resolve) => {
      let ccIndex = indexOfObjWithSomeKey(this._mount.computed_columns, 'columnName', ccColumnName)
      let delCC = this._mount.computed_columns.splice(ccIndex, 1)
      this._delCCRelated(this.getFactTable(), ccColumnName)
      resolve(delCC)
    })
  }
  // 自动布局
  autoCalcLayer (root, result, deep) {
    var factTable = this.getFactTable()
    if (!factTable) {
      return
    }
    const rootGuid = factTable.guid
    const tree = new ModelTree({rootGuid: rootGuid, showLinkCons: this.allConnInfo})
    tree.positionTree()
    return tree.nodeDB.db
  }
  // 添加连接点
  addPlumbPoints (guid, columnName, columnType, isBroken) {
    var anchor = modelRenderConfig.jsPlumbAnchor
    var scope = 'showlink'
    if (isBroken) {
      this.plumbTool.setLineStyle({
        color: '#e73371',
        strokeWidth: 2
      })
    } else {
      this.plumbTool.setLineStyle({
        color: '#0988de',
        strokeWidth: 1
      })
    }
    var endPointConfig = Object.assign({}, this.plumbTool.endpointConfig, {
      scope: scope,
      parameters: {
        data: {
          guid: guid,
          column: {
            columnName: columnName,
            columnType: columnType
          }}
      },
      uuid: guid + columnName
    })
    this.plumbTool.addEndpoint(guid, {anchor: anchor}, endPointConfig)
  }
  // 添加连线上的图标（连接类型Left/Inner）
  setOverLayLabel (conn, isBroken) {
    var fid = conn.sourceId
    var pid = conn.targetId
    var labelObj = conn.getOverlay(pid + (fid + 'label'))
    var joinInfo = this.tables[pid].getJoinInfoByFGuid(fid)
    if (!joinInfo) {
      return
    }
    var joinType = joinInfo.join.type
    var labelCanvas = $(labelObj.canvas)
    labelCanvas.removeClass('link-label-broken')
    conn.setType(isBroken ? 'broken' : 'normal')
    conn.isBroken = isBroken
    this.getBrokenLinkedTable()
    labelCanvas.addClass(isBroken ? 'link-label link-label-broken' : 'link-label')
    labelCanvas && labelCanvas.find('.label').eq(0).text(joinType)
  }
  getBrokenLinkedTable () {
    for (let i in this.allConnInfo) {
      if (this.allConnInfo[i].isBroken) {
        this._mount.hasBrokenLinkedTable = true
        return i.split('$')
      }
    }
    this._mount.hasBrokenLinkedTable = false
    return null
  }
  // 判断同一张表join关系是否相同
  compareModelLink (table, joinColumns) {
    let guidTableNameMap = {}
    const {fid, pid} = table
    const linkList = []
    const connInfo = Object.keys(this.allConnInfo).filter(item => item.split('$')[1] === fid && item.split('$')[0] !== pid).map(it => it.split('$')[0])
    connInfo.forEach(item => {
      const tableName = this.tables[item].name
      if (tableName in guidTableNameMap) {
        guidTableNameMap[tableName].push(item)
      } else {
        guidTableNameMap[tableName] = [item]
      }
    })
    if (this.tables[pid].name in guidTableNameMap && guidTableNameMap[this.tables[pid].name].length) {
      guidTableNameMap[this.tables[pid].name].forEach(item => {
        const tableInfo = this.getTableByGuid(item)
        const tableName = tableInfo.name.split('.')[1]
        const { join } = tableInfo.getJoinInfo()
        let link = join.foreign_key.map((it, index) => `${it}/${join.op[index]}/${join.primary_key[index].replace(/^(\w+)\./, `${tableName}.`)}/${join.type}`).sort()
        linkList.push(link.join('&'))
      })
    }

    const currentJoin = joinColumns.foreign_key.map((f, index) => `${f}/${joinColumns.op[index]}/${joinColumns.primary_key[index]}/${joinColumns.type}`).sort().join('&')
    return linkList.includes(currentJoin)
  }
}

export default NModel
