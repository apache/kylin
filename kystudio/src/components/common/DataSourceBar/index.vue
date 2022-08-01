<template>
<div style="height: 100%;" class="clearfix">
  <aside class="data-source-bar" :style="dataSourceStyle">
    <section class="header clearfix" v-if="isShowActionGroup && !hideBarTitle">
      <div class="header-text ksd-title-module">
        <span>{{$t('kylinLang.common.dataSource')}}</span>
      </div>
      <div class="icon-btns clearfix">
        <!-- <div :class="['header-icons', 'clearfix', {selected: isSwitchSource}]" v-if="isShowSourceSwitch">
          <el-tooltip :content="$t('sourceManagement')" effect="dark" placement="top">
            <i class="ksd-fs-14 el-icon-ksd-setting" @click="handleSwitchSource"></i>
          </el-tooltip>
        </div> -->
        <div class="add-source-table-icon" v-if="isShowLoadTable">
          <el-tooltip :content="$t('addDatasource')" effect="dark" placement="top">
            <i class="ksd-fs-14 el-icon-ksd-project_add"  @click="importDataSource('selectSource', currentProjectData)"></i>
          </el-tooltip>
        </div>
      </div>
    </section>
    <section class="body">
      <div v-if="isShowLoadTable" class="btn-group">
        <el-button plain size="medium" v-if="!isLoadingTreeData && showAddDatasourceBtn" type="primary" icon="el-ksd-icon-add_data_source_old" @click="importDataSource('selectSource', currentProjectData)">
          {{$t('addDatasource')}}
        </el-button>
      </div>
      <div v-if="showTreeFilter" class="ksd-mb-16 dispaly-flex">
        <el-input v-model="filterText" :placeholder="$t('searchTable')"  prefix-icon="el-ksd-icon-search_22" v-global-key-event.enter.debounce="handleFilter" @clear="handleClear()"></el-input>
        <div class="add-source-table-icon" v-if="isShowLoadTableInnerBtn">
          <el-tooltip :content="$t('addDatasource')" effect="dark" placement="top">
            <i class="ksd-fs-14 el-icon-ksd-project_add"  @click="importDataSource('selectSource', currentProjectData)"></i>
          </el-tooltip>
        </div>
      </div>
      <div v-scroll style="height:calc(100% - 51px)" v-loading="isLoadingTreeData">
        <TreeList
          :tree-key="treeKey"
          ref="treeList"
          :data="datasources"
          :placeholder="$t('searchTable')"
          :default-expanded-keys="defaultExpandedKeys"
          :draggable-node-types="draggableNodeTypes"
          :is-model-have-fact="isModelHaveFact"
          :is-second-storage-enabled="isSecondStorageEnabled"
          :is-expand-all="isExpandAll"
          :is-show-filter="false"
          :is-expand-on-click-node="isExpandOnClickNode"
          :is-resizable="isResizable"
          :on-filter="handleFilter"
          :filter-white-list-types="['column']"
          :ignore-column-tree="ignoreColumnTree"
          @click="handleClick"
          @drag="handleDrag"
          @load-more="handleLoadMore"
          @node-expand="handleNodeExpand"
          @node-collapse="handleNodeCollapse">
          <template>
            <kylin-nodata :content="emptyText" v-if="databaseArray.length <= 0 || tableArray.length <= 0" size="small"></kylin-nodata>
          </template>
        </TreeList>
      </div>
    </section>
  </aside>
  <div class="ky-drag-layout-line" unselectable="on" v-if="isShowDragWidthBar" v-drag:change.width="dataSourceDragData">
    <!-- <div class="ky-drag-layout-bar" unselectable="on" v-if="isShowDragWidthBar" v-drag:change.width="dataSourceDragData">||</div> -->
  </div>
</div>
</template>

<script>
import Vue from 'vue'
import { mapActions, mapGetters, mapMutations } from 'vuex'
import { Component, Watch } from 'vue-property-decorator'
import { sourceTypes, sourceNameMapping, pageSizeMapping } from '../../../config'
import TreeList from '../TreeList/index.vue'
import locales from './locales'
import { getDatasourceObj, getDatabaseObj, getTableObj, getFirstTableData, getWordsData, getTableDBWordsData, freshTreeOrder, getDatabaseTablesObj } from './handler'
import { handleSuccessAsync, handleError, objectClone } from '../../../util'
// import { types } from '../../studio/StudioModel/TableIndexEdit/store'
@Component({
  props: {
    projectName: {
      type: String
    },
    expandNodeTypes: {
      type: Array,
      default: () => []
    },
    searchableNodeTypes: {
      type: Array,
      default: () => []
    },
    draggableNodeTypes: {
      type: Array,
      default: () => []
    },
    clickableNodeTypes: {
      type: Array,
      default: () => ['database', 'table', 'column']
    },
    defaultWidth: {
      type: Number,
      default: 240
    },
    isShowActionGroup: {
      type: Boolean,
      default: true
    },
    isShowLoadSource: {
      type: Boolean,
      default: false
    },
    isShowLoadTable: {
      type: Boolean,
      default: true
    },
    isShowLoadTableInnerBtn: {
      type: Boolean,
      default: false
    },
    isShowSourceSwitch: {
      type: Boolean,
      default: false
    },
    isShowSettings: {
      type: Boolean,
      default: false
    },
    isShowSelected: {
      type: Boolean,
      default: false
    },
    isExpandOnClickNode: {
      type: Boolean,
      default: true
    },
    isShowFilter: {
      type: Boolean,
      default: true
    },
    isExpandAll: {
      type: Boolean,
      default: false
    },
    isResizable: {
      type: Boolean,
      default: false
    },
    ignoreNodeTypes: {
      type: Array,
      default: () => []
    },
    isShowDragWidthBar: {
      type: Boolean,
      default: false
    },
    hideBarTitle: {
      type: Boolean,
      default: false
    },
    hideFactIcon: {
      type: Boolean,
      default: false
    },
    customTreeTitle: {
      type: String,
      default: ''
    },
    isModelHaveFact: {
      type: Boolean,
      default: false
    },
    isSecondStorageEnabled: {
      type: Boolean,
      default: false
    }
  },
  components: {
    TreeList
  },
  computed: {
    ...mapGetters([
      'isAdminRole',
      'isProjectAdmin',
      'currentProjectData'
    ])
  },
  methods: {
    ...mapActions('DataSourceModal', {
      callDataSourceModal: 'CALL_MODAL'
    }),
    ...mapActions('DetailDialogModal', {
      callGlobalDetailDialog: 'CALL_MODAL'
    }),
    ...mapActions({
      fetchDatabases: 'FETCH_DATABASES',
      fetchTables: 'FETCH_TABLES',
      updateTopTable: 'UPDATE_TOP_TABLE',
      fetchDBandTables: 'FETCH_DB_AND_TABLES'
    }),
    ...mapMutations({
      cacheDatasource: 'CACHE_DATASOURCE'
    })
  },
  locales
})
export default class DataSourceBar extends Vue {
  treeKey = 'pagetree' + Number(new Date())
  filterText = ''
  isSearchIng = false
  isLoadingTreeData = true // 用于处理查询搜索时loading 效果
  datasources = []
  sourceTypes = sourceTypes
  allWords = []
  defaultExpandedKeys = []
  cacheDefaultExpandedKeys = []
  draggableNodeKeys = []
  isSwitchSource = false
  loadedTables = []
  failedTables = []
  dataSourceDragData = {
    width: this.defaultWidth,
    limit: {
      width: [this.defaultWidth, 500]
    }
  }
  databaseSizeObj = {}
  dataSourceSelectedLabel = ''
  currentSourceTypes = [9, 1, 8] // 默认试着获取一下Hive和kafka数据源的数据
  get emptyText () {
    return this.filterText ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }
  get showAddDatasourceBtn () {
    // 4x 本身保持原来逻辑: 有库表后，才隐藏添加数据源按钮（搜索时会有可能导致databaseArray数组为空，所以要判断是否是搜索后的空态）
    return this.filterText === '' && this.databaseArray.length <= 0
  }
  get showTreeFilter () {
    // 配置带搜索
    if (this.isShowFilter) {
      // 有库表 显示搜索框
      if (this.databaseArray.length) {
        return true
      } else {
        // 没库表，但处于筛选状态，也显示搜索框
        return this.isSearchIng
      }
    } else { // 配置为false 就直接隐藏搜索框
      return false
    }
  }
  get dataSourceStyle () {
    return this.isShowDragWidthBar ? {
      width: this.dataSourceDragData.width + 'px'
    } : {}
  }
  get databaseArray () {
    const allData = this.datasources.reduce((databases, datasource) => [...databases, ...datasource.children], [])
    return allData.filter(data => !['isMore', 'isLoading'].includes(data.type))
  }
  get tableArray () {
    const allData = this.databaseArray.reduce((tables, database) => [...tables, ...database.children], [])
    return allData.filter(data => !['isMore', 'isLoading'].includes(data.type))
  }
  get columnArray () {
    return this.ignoreNodeTypes.indexOf('column') >= 0 ? [] : this.tableArray.reduce((columns, table) => [...columns, ...table.childContent], [])
  }
  get ignoreColumnTree () {
    return this.ignoreNodeTypes.indexOf('column') >= 0
  }
  // get currentSourceTypes () {
  //   const { override_kylin_properties: overrideKylinProperties } = this.currentProjectData || {}
  //   return overrideKylinProperties && overrideKylinProperties['kylin.source.default']
  //     ? [+overrideKylinProperties['kylin.source.default']]
  //     : []
  // }
  get foreignKeys () {
    return this.tableArray.reduce((foreignKeys, table) => {
      const currentFK = table.__data.foreign_key.map(foreignKey => `${table.datasource}.${table.database}.${foreignKey}`)
      return [...foreignKeys, ...currentFK]
    }, [])
  }
  get isShowBtnLoad () {
    return (this.isAdminRole || this.isProjectAdmin) && !this.datasources.length
  }
  get primaryKeys () {
    return this.tableArray.reduce((primaryKeys, table) => {
      const currentPK = table.__data.primary_key.map(primaryKey => `${table.datasource}.${table.database}.${primaryKey}`)
      return [...primaryKeys, ...currentPK]
    }, [])
  }
  @Watch('$lang')
  onLanguageChange () {
    this.freshDatasourceTitle()
  }
  // @Watch('tableArray')
  // onTreeDataChange () {
  //   this.freshAutoCompleteWords()
  //   this.defaultExpandedKeys = this.defaultExpandedKeysWords
  //     .filter(word => this.expandNodeTypes.includes(word.meta))
  //     .map(word => word.id)
  // }
  @Watch('projectName')
  @Watch('currentSourceTypes')
  onProjectChange (oldValue, newValue) {
    if (JSON.stringify(oldValue) !== JSON.stringify(newValue)) {
      this.initTree()
    }
  }
  mounted () {
    this.$on('filter', (event) => {
      this.$refs.treeList && this.$refs.treeList.$emit('filter', event)
    })
    this.initTree()
    const tableLayout = document.getElementsByClassName('table-layout')
    if (tableLayout && tableLayout.length) {
      this.dataSourceDragData.limit.width[1] = tableLayout[0].offsetWidth - 60
    }
  }
  addPagination (data) {
    data.pagination.page_offset++
  }
  clearPagination (data) {
    data.pagination.page_offset = 0
  }
  showLoading (data) {
    data.isLoading = true
  }
  hideLoading (data) {
    data.isLoading = false
  }
  async initTree () {
    try {
      this.isSearchIng = false
      // await this.loadDatasources()
      // await this.loadDataBases()
      // await this.loadTables({ isReset: true })
      // 有加载数据源的情况，才去加载db 和 table，否则就处理loading字段
      // if (this.datasources.length > 0) {
      //   await this.loadTreeData()
      //   this.freshAutoCompleteWords()
      // } else {
      //   this.isLoadingTreeData = false
      // }
      // freshTreeOrder(this)
      // this.selectFirstTable()
      this.isLoadingTreeData = false
      await this.loadTreeData()
    } catch (e) {
      handleError(e)
    }
  }
  // async loadDatasources () {
  //   this.datasources = this.currentSourceTypes.map(sourceType => getDatasourceObj(this, sourceType))
  // }
  async loadTreeData (filterText) { // 根据数据源获取dbs 和tables
    this.isLoadingTreeData = true
    this.treeKey = 'pageTree_' + filterText + Number(new Date())
    this.databaseSizeObj = {}
    this.datasources = []
    const datasources = this.currentSourceTypes.map(sourceType => getDatasourceObj(this, sourceType))
    await Promise.all(datasources.map((datasource, index) => {
      return this.loadDateBasesData(datasource, index, filterText)
    }))
    this.freshAutoCompleteWords()
    freshTreeOrder(this)
    this.selectFirstTable()
    this.isLoadingTreeData = false
  }
  async loadDateBasesData (datasource, index, filterText) {
    if (!datasource.projectName) {
      return
    }
    let params = {
      project_name: datasource.projectName,
      source_type: datasource.sourceType,
      page_offset: 0,
      page_size: pageSizeMapping.TABLE_TREE,
      table: filterText || ''
    }
    const response = await this.fetchDBandTables(params)
    const results = await handleSuccessAsync(response)
    if (results.databases.length) {
      // 没有筛选条件时，需要将数据库和 size 做个映射
      if (!filterText && datasource.sourceType === 9) { // 只针对hive数据源的loaded table 计数，kafka的同名数据源会混淆
        results.databases.forEach((item) => {
          this.databaseSizeObj[item.dbname] = item.size
        })
      }
      // 先处理 db 一层的render，以及初值的赋值
      datasource.children = results.databases.map(resultDatabse => getDatabaseTablesObj(this, datasource, resultDatabse))
      // 然后处理 table 一级的render render 后变更页码，并存入store
      datasource.children.forEach((database, index) => {
        database.children = database.originTables.map(resultTable => getTableObj(this, database, resultTable, this.ignoreNodeTypes.indexOf('column') >= 0))
        this.addPagination(database)
        this.cacheDatasourceInStore(index, database.originTables, true)
      })
      // 确保下Hive数据源第一位
      if (datasource && datasource.sourceType === 9) {
        this.datasources.unshift(datasource)
      } else {
        this.datasources.push(datasource)
      }
      this.defaultExpandedKeys.push(datasource.id)
    }
  }
  async loadDataBases () {
    // 分数据源，请求database
    const responses = await Promise.all(this.datasources.map(({ projectName, sourceType }) => {
      return this.fetchDatabases({ projectName, sourceType })
    }))
    const results = await handleSuccessAsync(responses)
    // 组装database进datasource
    this.datasources.forEach((datasource, index) => {
      datasource.children = results[index].map(resultDatabse => getDatabaseObj(this, datasource, resultDatabse))
    })
  }
  async loadTables (params) {
    const { tableName = null, databaseId = null, isReset = false } = params || {}
    const currentDatabases = this.databaseArray.filter(database => {
      return database.id === databaseId || !databaseId
    })
    const responses = await Promise.all(currentDatabases.map((database) => {
      const { projectName, label: databaseName, pagination, datasource } = database
      isReset ? this.clearPagination(database) : null
      return this.fetchTables({ projectName, databaseName, tableName, isExt: true, ...pagination, sourceType: datasource })
    }))
    const results = await handleSuccessAsync(responses)
    currentDatabases.forEach((database, index) => {
      const { size, tables: resultTables } = results[index]
      const tables = resultTables.map(resultTable => getTableObj(this, database, resultTable, this.ignoreNodeTypes.indexOf('column') >= 0))
      if (this.ignoreNodeTypes.indexOf('table') < 0) {
        database.children = !isReset ? [...database.children, ...tables] : tables
        database.isMore = size && size > this.getChildrenCount(database)
        database.isHidden = !this.getChildrenCount(database)
      }
      this.addPagination(database)
      this.hideLoading(database)
      this.cacheDatasourceInStore(index, resultTables, isReset)
    })
    this.recoverySelectedTable()
  }
  // 恢复之前选中的表名
  recoverySelectedTable () {
    for (let item of this.tableArray) {
      if (item.label === this.dataSourceSelectedLabel) {
        item.isSelected = !item.isSelected
        return
      }
    }
  }
  cacheDatasourceInStore (index, tables, isSourceReset) {
    const isReset = isSourceReset && index === 0
    const project = this.projectName
    this.cacheDatasource({ data: { tables }, project, isReset })
  }
  getChildrenCount (data) {
    return data.children.filter(data => !['isMore', 'isLoading'].includes(data.type)).length
  }
  handleDrag (data, node) {
    this.$emit('drag', data, node)
  }
  async handleClear () {
    await this.handleFilter('', true)
    this.defaultExpandedKeys = this.cacheDefaultExpandedKeys
  }
  async handleFilter (filterText, isNotResetDefaultExpandedKeys) {
    this.isSearchIng = true
    const scrollDom = this.$el.querySelector('.scroll-content')
    const scrollBarY = this.$el.querySelector('.scrollbar-thumb.scrollbar-thumb-y')
    scrollDom && (scrollDom.style.transform = null)
    scrollBarY && (scrollBarY.style.transform = null)
    return new Promise(async resolve => {
      await this.loadTreeData(filterText)
      !isNotResetDefaultExpandedKeys && this.resetDefaultExpandedKeys()
      this.filterText = filterText
      freshTreeOrder(this)
      this.selectFirstTable()
      resolve()
    })
  }
  // 过滤搜索重置为展开搜索结果的树结构，缓存一下以便清除搜索时回复树结构展开情况
  resetDefaultExpandedKeys () {
    this.cacheDefaultExpandedKeys = this.defaultExpandedKeys
    this.freshAutoCompleteWords()
  }
  async reloadTables (isNotResetDefaultExpandedKeys) {
    const res = await this.handleFilter(this.filterText, isNotResetDefaultExpandedKeys)
    this.freshAutoCompleteWords()
    return res
  }
  // 表数据变化，需要刷新，且保持之前的选中项 3016 临时修改方案
  async refreshTables () {
    await this.loadTreeData(this.filterText)
    this.freshAutoCompleteWords()
    freshTreeOrder(this)
    // 刷完表之后，需要重置之前选中的表
    this.recoverySelectedTable()
  }
  async handleLoadMore (data, node) {
    let dbName = (data.parent.label).toLocaleLowerCase()
    const { id: databaseId } = data.parent
    // 加载更多时，要将查询的关键字解析处理
    let tableName = ''
    // 如果完全匹配 db，或者是搜索的关键字包含在 dbName 中，这时搜索table的关键字应该是空
    if (dbName.indexOf(this.filterText.toLocaleLowerCase()) > -1) {
      tableName = ''
    } else { // 只有没有完全匹配db时，才会将关键字传
      let idx = this.filterText.indexOf('.')
      tableName = idx === -1 ? this.filterText : this.filterText.substring(idx + 1, this.filterText.length)
    }
    await this.loadTables({ databaseId, tableName })
    this.freshAutoCompleteWords()
  }
  handleClick (data, node) {
    if (data && this.clickableNodeTypes.includes(data.type)) {
      if (this.isShowSelected) {
        this.isSwitchSource = false
        this.setSelectedTable(data)
      }
      this.$emit('click', data, node)
    }
  }
  handleNodeExpand (data, node) {
    const index = this.defaultExpandedKeys.indexOf(data.id)
    if (index === -1) {
      this.defaultExpandedKeys.push(data.id)
    }
  }
  handleNodeCollapse (data, node) {
    const index = this.defaultExpandedKeys.indexOf(data.id)
    if (index !== -1) {
      this.defaultExpandedKeys.splice(index, 1)
    }
    const idPrefix = `${data.id}.` // 合并DataSource树节点，同时合并索引DataSource的database树节点
    const defaultExpandedKeysClone = objectClone(this.defaultExpandedKeys)
    this.defaultExpandedKeys.forEach((k, index) => {
      if ((k + '').indexOf(idPrefix) !== -1) {
        defaultExpandedKeysClone.splice(index, 1)
      }
    })
    this.defaultExpandedKeys = defaultExpandedKeysClone
  }
  async handleToggleTop (data, node, event) {
    event && event.stopPropagation()
    event && event.preventDefault()
    const { projectName } = this
    const tableFullName = `${data.database}.${data.label}`
    const isTopSet = !data.isTopSet
    await this.updateTopTable({ projectName, tableFullName, isTopSet })
    data.isTopSet = isTopSet
    freshTreeOrder(this)
  }
  handleSwitchSource () {
    this.isSwitchSource = !this.isSwitchSource
    this.resetSourceTableSelect(this.isSwitchSource)
    this.$emit('show-source', this.isSwitchSource)
  }
  resetSourceTableSelect (type) {
    if (type) {
      for (let item of this.tableArray) {
        if (item.isSelected) {
          // this.dataSourceSelectedLabel = item.label
          item.isSelected = !item.isSelected
          return
        }
      }
    } else if (!type && this.dataSourceSelectedLabel) {
      for (let item of this.tableArray) {
        if (item.label === this.dataSourceSelectedLabel) {
          item.isSelected = !item.isSelected
          return
        }
      }
    }
  }
  setSelectedTable (data) {
    for (const table of this.tableArray) {
      data.id === table.id && (this.dataSourceSelectedLabel = table.label)
      table.isSelected = data.id === table.id
    }
  }
  selectFirstTable () {
    if (this.isShowSelected && this.tableArray.length) {
      const firstTable = getFirstTableData(this.datasources)
      this.handleClick(firstTable)
      // 展开默认选中的database
      const index = this.defaultExpandedKeys.indexOf(firstTable.parent.id)
      if (index === -1) {
        this.defaultExpandedKeys.push(firstTable.parent.id)
      }
      return true
    } else {
      return null // 当前数据为空，不存在第一张表
    }
  }
  freshAutoCompleteWords () {
    const datasourceWords = this.datasources.map(datasource => getWordsData(datasource))
    const databaseWords = this.databaseArray.map(database => getWordsData(database))
    const tableWords = this.tableArray.map(table => getWordsData(table))
    const databaseTableWords = this.tableArray.map(table => getTableDBWordsData(table))
    const columnWords = this.columnArray.map(column => getWordsData(column))
    this.allWords = [...datasourceWords, ...databaseWords, ...tableWords, ...columnWords]
    this.$emit('autoComplete', [...databaseWords, ...tableWords, ...databaseTableWords, ...columnWords])
  }
  async toImportDataSource (editType, project) {
    const result = await this.callDataSourceModal({ editType, project, databaseSizeObj: this.databaseSizeObj })
    if (result) {
      const { loaded, failed } = result
      this.loadedTables = loaded || []
      this.failedTables = failed || []
      // 提示
      if (failed && failed.length) {
        // 筛出失败的库
        let datasrouces = failed.filter((item) => {
          return item.indexOf('.') === -1
        })
        // 筛出失败的表
        let tables = failed.filter((item) => {
          return item.indexOf('.') > -1
        })
        let tempDetail = []
        if (datasrouces.length > 0) {
          tempDetail.push({
            title: this.$t('databases') + ' (' + datasrouces.length + ')',
            list: datasrouces
          })
        }
        if (tables.length > 0) {
          tempDetail.push({
            title: this.$t('tables') + ' (' + tables.length + ')',
            list: tables
          })
        }
        await this.callGlobalDetailDialog({
          theme: 'plain-mult',
          msg: this.$t('loadTablesFail', {db_counts: datasrouces.length, table_counts: tables.length}),
          showCopyBtn: true,
          details: tempDetail,
          needCallbackWhenClose: true
        })
      } else {
        this.$message({ type: 'success', message: this.$t('loadTablesSuccess') })
      }
      if (loaded.length > 0) {
        await new Promise((resolve) => setTimeout(() => resolve(), 1000))
        this.handleResultModalClosed(result.sourceType)
      }
    }
  }
  importDataSource (editType, project, event) {
    event && event.stopPropagation()
    event && event.preventDefault()
    event && event.target.blur()
    this.toImportDataSource(editType, project)
  }
  async handleResultModalClosed (sourceType) {
    try {
      await this.loadDataBases()
      await this.reloadTables(true) // true 表示不改变树开合情况
      // 展开新增database
      this.loadedTables.forEach((item) => {
        const database = item.split('.')[0]
        if (database && this.defaultExpandedKeys.indexOf(sourceType + '.' + database) === -1) {
          this.defaultExpandedKeys.push(sourceType + '.' + database)
        }
      })
      freshTreeOrder(this)
      this.loadedTables.length && this.$emit('tables-loaded')
      this.loadedTables = []
      this.failedTables = []
      this.selectFirstTable()
    } catch (e) {
      handleError(e)
    }
  }
  freshDatasourceTitle () {
    this.datasources.forEach(datasource => {
      const sourceName = sourceTypes[datasource.sourceType]
      let sourceNameStr = sourceNameMapping[sourceName]
      datasource.label = `${this.$t('source')}${sourceNameStr}`
    })
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';
.ky-drag-layout-line {
  height: 100%;
  border-left: 1px solid @ke-border-secondary;
  cursor: col-resize;
  float: left;
  z-index: 8;
  position: relative;
  .ky-drag-layout-bar {
    right:-5px;
    top:200px;
  }
  &:hover {
    border-left: 1px solid @base-color;
    .ky-drag-layout-bar {
      background: #e6f3fb;
      border: solid 1px #0988DE;
      color: #0988DE;
    }
  }
}
.data-source-bar {
  float: left;
  position:relative;
  height: 100%;
  width: 100%;
  .dispaly-flex {
    display: flex;
    .add-source-table-icon {
      line-height: 30px;
      margin-left: 5px;
      cursor: pointer;
    }
  }
  .header,
  .body {
    width: 240px;
    box-sizing: border-box;
  }
  .header {
    padding: 24px 16px 16px 16px;
    font-size: 16px;
    color: @text-title-color;
  }
  .body {
    padding: 0px 16px 16px;
  }
  .header-text {
    float: left;
    span {
      line-height: 20px;
    }
  }
  .icon-btns {
    position: relative;
    height: 22px;
    top: 2px;
  }
  .header-icons {
    float: right;
    width: 22px;
    height: 22px;
    right: 16px;
    text-align: center;
    line-height: 22px;
    &.selected {
      background-color: @line-split-color;
      i {
        color: @base-color;
      }
    }
    i {
      margin-right: 4px;
      color: @text-title-color;
      cursor: pointer;
      &:hover {
        color: @base-color;
      }
    }
    i:last-child {
      margin-right: 0;
    }
  }
  .add-source-table-icon {
    float: right;
    width: 22px;
    height: 22px;
    text-align: center;
    line-height: 22px;
    &:hover {
      color: @base-color;
    }
  }
  .body {
    height: calc(~"100% - 66px");
    overflow: hidden;
  }
  .body .btn-group {
    text-align: center;
  }
  .body .btn-group .el-button {
    width: 100%;
    margin-bottom: 10px;
  }
  // datasource tree样式
  .el-tree {
    min-height: calc(~"100vh - 263px");
    margin-bottom: 40px;
    border: 1px solid #ddd;
    .left {
      float: left;
      margin-right: 4px;
    }
    .right {
      position: absolute;
      right: 10px;
      top: 50%;
      transform: translateY(-50%);
      &.fact-icon {
        right: 25px;
      }
    }
    // .tree-icon {
    //   margin-right: 4px;
    //   display: inline-block;
    //   &:last-child {
    //     margin-right: 0;
    //   }
    // }
    .tree-item {
      position: relative;
      width: calc(~'100% - 24px');
      line-height: 34px;
      .database{
        .defaultIcon{
          color:#7f7f7f;
          font-size: 12px;
        }
      }
      .datatype {
        color: @text-placeholder-color !important;
      }
      .top {
        display: none;
        font-size: 13px;
        position: relative;
        top: -1px;
        cursor: pointer;
      }
      &:hover .top {
        display: inline;
      }
      .table {
        padding-right: 35px;
        line-height: 30px;
      }
      .table.has-range:hover {
        padding-right: 45px;
      }
      .column {
        padding-right: 10px;
        color: @text-placeholder-color;
        font-size: 12px;
        .column-name {
          color: @text-title-color;
          font-size: 14px;
        }
      }
      > div,
      .table {
        overflow: hidden;
        white-space: nowrap;
        text-overflow: ellipsis;
      }
      .frontground {
        position: relative;
        z-index: 1;
      }
      &.selected .background {
        background: @ke-background-color-secondary;
        position: absolute;
        top: 0;
        right: 0;
        bottom: 0;
        left: calc(-24px - 36px);
      }
    }
    .el-tree-node__expand-icon {
      position: relative;
      z-index: 1;
    }
    .datasource {
      color: #263238;
    }
    // .el-tree-node {
    //   .el-tree-node__content:hover > .tree-item {
    //     color: #087AC8;
    //   }
    // }
    .table-date-tip {
      color: #8E9FA8;
      &:hover {
        color: #087AC8;
      }
    }
    .table-action {
      color: #000000;
      &:hover {
        color: #087AC8;
      }
    }
    .table-tag {
      display: inline-block;
      width: 14px;
      height: 14px;
      line-height: 14px;
      margin-right: 2px;
      font-style: normal;
    }
    .column-tag {
      display: inline-block;
      font-size: 16px;
      color: #087AC8;
      margin-right: 2px;
      font-style: normal;
    }
    // & > .el-tree-node {
    //   border: 1px solid #ddd;
    //   min-height: calc(~"100vh - 263px");
    //   overflow: hidden;
    //   margin-bottom: 10px;
    //   & > .el-tree-node__content {
    //     padding: 7px 9px 8px 9px !important; // important用来去掉el-tree的内联样式
    //     height: auto;
    //     background: @regular-background-color;
    //     &:hover > .tree-item > span {
    //       color: #263238;
    //     }
    //   }
    //   // datasource的样式
    //   & > .el-tree-node__content {
    //     cursor: default;
    //     & > .tree-item {
    //       width: 100%;
    //       i {
    //         cursor: pointer;
    //       }
    //       .right {
    //         right: 0;
    //       }
    //     }
    //   }
    //   & > .el-tree-node__content .el-tree-node__expand-icon {
    //     display: none;
    //   }
    //   & > .el-tree-node__children {
    //     margin-left: -18px;
    //   }
    //   & > .el-tree-node__children > .el-tree-node {
    //     border-top: 1px solid #CFD8DC;
    //   }
    // }
  }
  .el-tree__empty-block {
    text-align: left;
  }
  .el-tree__empty-text {
    position: initial;
    transform: none;
    top: 0;
    left: 0;
  }
}
</style>
