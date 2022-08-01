<template>
  <div class="source-hive clearfix" :class="{'zh-lang': $store.state.system.lang !== 'en'}">
    <div class="list clearfix">
      <div class="ksd-ml-24 ksd-mt-24">
        <el-input :placeholder="$t('filterTableName')" 
                  v-model="filterText" 
                  prefix-icon="el-icon-search" 
                  @keyup.enter.native="handleFilter()" 
                  @clear="handleFilter()">
        </el-input>
      </div>
      <div class="treeBox" :class="{'hasRefreshBtn': (filterData || treeData.length === 0) && !loadingTreeData}">
        <TreeList
          :tree-key="treeKey"
          :show-overflow-tooltip="true"
          ref="tree-list"
          :class="['table-tree', {'has-refresh': loadHiveTableNameEnabled === 'true'}]"
          :data="treeData"
          :placeholder="$t('filterTableName')"
          :is-show-filter="false"
          :is-show-resize-bar="false"
          :filter-white-list-types="['datasource', 'database']"
          @resize="handleResize"
          @click="handleClickNode"
          @node-expand="handleNodeExpand"
          @load-more="handleLoadMore"
          :default-expanded-keys="defaultExpandedKeys"
        />
        <div class="split" v-if="false">
          <i class="el-icon-ksd-more_03"></i>
        </div>
        <div class="empty" v-if="!loadingTreeData && treeData.length===0">
          <p class="empty-text" v-html="emptyText"></p>
        </div>
        <p class="ksd-right refreshNow" :class="{'isRefresh': reloadHiveTablesStatus.isRunning || hasClickRefreshBtn}" v-if="loadHiveTableNameEnabled === 'true'">{{$t('refreshText')}} <a href="javascript:;" @click="refreshHive(true)" v-if="!(reloadHiveTablesStatus.isRunning || hasClickRefreshBtn)">{{$t('refreshNow')}}</a><span v-else class="el-ksd-icon-loading_22"></span></p>
      </div>
    </div>
    <div class="content" :style="contentStyle">
      <div class="content-body" :class="{ 'has-tips': isShowTips, 'has-error-msg': needSampling&&errorMsg }">
        <div class="category databases">
          <div class="header font-medium">
            <span>{{$t('database')}}</span>
            <span>({{selectDBNames.length}})</span>
          </div>
          <div class="names">
            <arealabel
              :duplicateremove="true"
              :validateRegex="regex.validateDB"
              @validateFail="selectedDBValidateFail"
              @refreshData="refreshDBData"
              splitChar="," 
              :selectedlabels="selectDBNames"
              :allowcreate="true"
              :placeholder="$t('dbPlaceholder')"
              @removeTag="removeSelectedDB" 
              :datamap="{label: 'label', value: 'value'}">
            </arealabel>
          </div>
        </div>
        <div class="category tables">
          <div class="header font-medium">
            <span>{{$t('tableName')}}</span>
            <span>({{tablesNum}})</span>
          </div>
          <div class="names">
            <arealabel
              :duplicateremove="true"
              :validateRegex="regex.validateTable"
              @validateFail="selectedTableValidateFail"
              @refreshData="refreshTableData"
              splitChar="," 
              :selectedlabels="selectTablesNames"
              :allowcreate="true"
              :placeholder="$t('dbTablePlaceholder')"
              @removeTag="removeSelectedTable" 
              :datamap="{label: 'label', value: 'value'}">
            </arealabel>
          </div>
        </div>
      </div>
      <transition name="fade">
        <div class="tips" v-if="isShowTips">
          <div class="close el-icon-ksd-close" @click="handleHideTips"></div>
          <i class="el-icon-ksd-info infoIcon"></i>
          <ul class="body" :class="{'zh-body': $store.state.system.lang !== 'en'}">
            <li>{{$t('loadTableTips1_1')}}<span class="font-medium">{{$t('loadTableTips1_2')}}</span>{{$t('loadTableTips1_3')}}</li>
            <li>{{$t('loadTableTips2_1')}}<span class="font-medium">{{$t('loadTableTips2_2')}}</span>{{$t('loadTableTips2_3')}}</li>
            <li>{{$t('loadTableTips3_1')}}<span class="font-medium">{{$t('loadTableTips3_2')}}</span>{{$t('loadTableTips3_3')}}</li>
          </ul>
        </div>
      </transition>
    </div>
    <div :class="['sample-block', {'has-error': needSampling && errorMsg}]">
      <span class="ksd-title-label-small ksd-mr-10">{{$t('samplingTitle')}}</span><el-switch
        @change="handleSampling"
        :value="needSampling"
        :active-text="$t('kylinLang.common.OFF')"
        :inactive-text="$t('kylinLang.common.ON')">
      </el-switch>
      <div class="sample-desc ksd-mt-5">{{$t('sampleDesc')}}</div>
      <div class="sample-desc">
        {{$t('sampleDesc1')}}<el-input size="small" style="width: 110px;" class="ksd-mrl-5" v-number="samplingRows" :value="samplingRows" :disabled="!needSampling" :class="{'is-error': needSampling&&errorMsg}" @input="handleSamplingRows"></el-input>{{$t('sampleDesc2')}}
        <div class="error-msg" v-if="needSampling&&errorMsg">{{errorMsg}}</div>
      </div>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapGetters, mapActions, mapState } from 'vuex'
import { Component, Watch } from 'vue-property-decorator'
import Scrollbar from 'smooth-scrollbar'
import locales from './locales'
import TreeList from '../../TreeList'
import { sourceTypes, pageSizeMapping } from '../../../../config'
import { getDatabaseTree, getTableTree, getDatabaseTablesTree } from './handler'
import { handleSuccessAsync, handleError } from '../../../../util'
import arealabel from '../../area_label.vue'

@Component({
  props: {
    selectedTables: {
      default: () => []
    },
    selectedDatabases: {
      default: () => []
    },
    needSampling: Boolean,
    samplingRows: {
      default: 20000000
    },
    sourceType: Number,
    databasesSize: {
      default: () => {}
    }
  },
  components: {
    TreeList,
    arealabel
  },
  computed: {
    ...mapGetters([
      'currentSelectedProject',
      'selectedProjectDatasource'
    ]),
    ...mapState({
      loadHiveTableNameEnabled: state => state.system.loadHiveTableNameEnabled
    })
  },
  methods: {
    ...mapActions({
      fetchDatabase: 'LOAD_HIVEBASIC_DATABASE',
      fetchTables: 'LOAD_HIVE_TABLES',
      fetctDatabaseAndTables: 'LOAD_HIVEBASIC_DATABASE_TABLES',
      reloadHiveDBAndTables: 'RELOAD_HIVE_DB_TABLES'
    })
  },
  locales
})
export default class SourceHive extends Vue {
  treeData = []
  contentStyle = {
    marginLeft: null,
    width: null,
    height: '367px'
  }
  sourceTypes = sourceTypes
  timer = null
  isDatabaseError = false
  isShowTips = true
  selectorWidth = 0
  filterText = ''
  errorMsg = ''
  defaultExpandedKeys= []
  loadingTreeData = true
  treeKey = 'tree' + Number(new Date())
  splitChar = ','
  regex = {
    validateTable: /^\s*;?(\w+\.\w+)\s*(,\s*\w+\.\w+)*;?\s*$/,
    validateDB: /^\s*;?(\w+)\s*(,\s*\w+)*;?\s*$/
  }
  selectTablesNames = []
  selectDBNames = []
  reloadHiveTablesStatus = { // 记录当前的刷新状态
    isRunning: false,
    time: 0
  }
  hasClickRefreshBtn = false
  pollingReloadStatusTimer = null // 轮询当前刷新状态的接口
  filterData = false // 打开弹窗时，不显示下面的立即刷新，执行了一次 handleFilter 后显示
  prevFilterText = ''
  allDatabasesSizeObj = {}
  tablesNum = 0

  get emptyText () {
    return this.filterText ? this.$t('kylinLang.common.noResults') : this.$t('noSourceData')
  }
  // get refreshBtnText () {
  //   return this.reloadHiveTablesStatus.isRunning || this.hasClickRefreshBtn ? this.$t('refreshIng') : this.$t('refreshNow')
  // }

  get databaseOptions () {
    return this.treeData.map(database => ({
      value: database.id,
      label: database.id
    }))
  }
  get tableOptions () {
    const tableOptions = []
    this.treeData.forEach(database => {
      return database.children.forEach(table => {
        tableOptions.push({
          value: table.id,
          label: table.id
        })
      })
    })
    return tableOptions
  }
  // 监听轮询的返回结果，如果轮询回来发现刷新结束了，刷新当前渲染的效果
  @Watch('reloadHiveTablesStatus.isRunning')
  async onRefreshTablesChange (newValue, oldValue) {
    // 由正在刷新变为刷新完成，需要刷下树的结果
    if (newValue !== oldValue && newValue === false) {
      let keyword = this.filterText || ''
      // 轮询出结果后，刷新树，需要重新渲染数据，这时候需要将key值修改下
      this.treeKey = 'tree' + Number(new Date())
      await this.loadDatabaseAndTables(keyword)
      this.onSelectedItemsChange()
    }
  }
  @Watch('selectedTables')
  @Watch('selectedDatabases')
  onSelectedItemsChange () {
    // 刷新table或者db的选中状态
    for (const database of this.treeData) {
      database.isSelected = this.selectedDatabases.includes(database.id)
      if (database.isSelected) {
        for (const table of database.children) {
          table.isSelected = true
          table.clickable = false
        }
      } else {
        for (const table of database.children) {
          if (!table.isLoaded) {
            table.isSelected = this.selectedTables.includes(table.id)
            table.clickable = true
          }
        }
      }
    }
    this.selectTablesNames = this.selectedTables.map((table) => {
      return table
    })
    this.selectDBNames = this.selectedDatabases.map((db) => {
      return db
    })
    this.calcSelectTablesNum()
  }
  selectedDBValidateFail () {
    this.$message(this.$t('selectedDBValidateFailText'))
  }
  selectedTableValidateFail () {
    this.$message(this.$t('selectedTableValidateFailText'))
  }

  calcSelectTablesNum () {
    let tablesLen = this.selectTablesNames.length
    let dbTables = 0
    for (let i = 0; i < this.selectedDatabases.length; i++) {
      let db = this.selectedDatabases[i]
      let total = this.allDatabasesSizeObj[db] ? this.allDatabasesSizeObj[db] : 0
      let loaded = this.databasesSize[db] ? this.databasesSize[db] : 0
      let size = total - loaded
      if (size < 0) {
        size = 0
      }
      dbTables = dbTables + size
    }
    this.tablesNum = dbTables + tablesLen
  }

  pollingReloadStatus () {
    if (this.pollingReloadStatusTimer) {
      window.clearTimeout(this.pollingReloadStatusTimer)
    }
    // 10 秒刷一次
    this.pollingReloadStatusTimer = setTimeout(() => {
      this.refreshHive(false)
    }, 10000)
  }

  // 手动刷新 hive 元数据
  refreshHive (isForce) {
    if (this.pollingReloadStatusTimer) {
      window.clearTimeout(this.pollingReloadStatusTimer)
    }
    if (this.loadHiveTableNameEnabled === 'false') return
    // 立即刷新按钮，防止重复提交，如果是强制刷新的请求，并且正在刷新中，就返回，不做接口请求
    if (isForce && (this.reloadHiveTablesStatus.isRunning || this.hasClickRefreshBtn)) {
      return false
    }
    if (isForce) { // 如果是强制刷新，就把正在刷新的标志位设置为正在，这样才能在接口返回后，数据变化了，触发 watch
      this.reloadHiveTablesStatus.isRunning = true
    }
    this.hasClickRefreshBtn = true
    let params = {
      force: isForce,
      project: this.currentSelectedProject // 统一都要传 project
    }
    this.reloadHiveDBAndTables(params).then((res) => {
      // 防止接口过慢导致销毁后回调继续执行
      if (this._isDestroyed) {
        return false
      }
      if (isForce) {
        this.$message({
          type: 'success',
          message: this.$t('refreshSuccess'),
          duration: 3000,
          closeOtherMessages: true
        })
      }
      this.hasClickRefreshBtn = false
      this.reloadHiveTablesStatus.isRunning = res.data.data.is_running
      this.reloadHiveTablesStatus.time = res.data.data.time
      this.pollingReloadStatus()
    }, (res) => {
      if (isForce) {
        this.$message({
          type: 'error',
          message: this.$t('refreshError'),
          closeOtherMessages: true
        })
      }
      this.reloadHiveTablesStatus.time = 0
      this.hasClickRefreshBtn = false
      this.reloadHiveTablesStatus.isRunning = false
    })
  }
  changeDataBase (dataBaseId) {
    const [{ size }] = this.treeData.filter(database => database.id === dataBaseId)
    this.selectTablesNames.filter(item => item.indexOf(`${dataBaseId}.`) > -1).length === size && this.handleAddDatabase(dataBaseId)
  }
  refreshDBData (val) {
    this.selectDBNames = val.map((item) => {
      return item.toLocaleUpperCase()
    })
    // DB 变更时 要去掉已加入的db下的表
    let selectedTables = this.selectedTables.filter((table) => {
      let itemDBIdx = table.indexOf('.')
      let str = table.substring(0, itemDBIdx)
      return this.selectDBNames.indexOf(str) === -1
    })
    this.selectTablesNames = [...selectedTables]
    this.$emit('input', { selectedDatabases: [...this.selectDBNames], selectedTables })
  }
  refreshTableData (val) {
    let selectedTables = val.map((item) => {
      return item.toLocaleUpperCase()
    })
    // 表变更的时候，如果库已经全部加了，该表就不单独加入了
    selectedTables = val.filter((table) => {
      let itemDBIdx = table.indexOf('.')
      let str = table.substring(0, itemDBIdx)
      return this.selectedDatabases.indexOf(str) === -1
    })
    this.selectTablesNames = [...selectedTables]
    this.$emit('input', { selectedTables })
    // this.changeDataBase(val[0].split('.')[0].toLocaleUpperCase())
  }
  removeSelectedDB (val) {
    this.selectDBNames.splice(this.selectDBNames.indexOf(val), 1)
    let selectedDatabases = this.selectedDatabases.filter((db) => {
      return db !== val
    })
    // 这个是用来通知，选中值变了，去更新左侧树的
    this.$emit('input', { selectedDatabases })
  }
  removeSelectedTable (val) {
    this.selectTablesNames.splice(this.selectTablesNames.indexOf(val), 1)
    const selectedTables = this.selectedTables.filter(tableId => tableId !== val)
    this.$emit('input', { selectedTables })
  }
  setNextPagination (pagination) {
    pagination.page_offset++
  }
  clearPagination (pagination) {
    pagination.page_offset = 0
  }
  hideNodeLoading (data) {
    data.isLoading = false
  }
  constructor () {
    super()
    this.getDatabaseTree = getDatabaseTree.bind(this)
    this.getTableTree = getTableTree.bind(this)
    this.getDatabaseTablesTree = getDatabaseTablesTree.bind(this)
  }
  async mounted () {
    this.refreshHive(false)
    await this.loadDatabaseAndTables()
    this.$on('samplingFormValid', () => {
      this.handleSamplingRows(this.samplingRows)
    })
  }
  beforeDestroy () {
    // 关闭弹窗时，会销毁这个组件，会触发到这里，去掉轮询
    window.clearTimeout(this.pollingReloadStatusTimer)
  }
  updated () {
    this.refreshSelectorWidth()
    this.refreshTagElWidth()
  }
  refreshSelectorWidth () {
    const selectorEl = this.$el.querySelector('.el-select')
    this.selectorWidth = selectorEl && selectorEl.getBoundingClientRect().width
  }
  refreshTagElWidth () {
    const tagBox = this.$el.querySelectorAll('.source-hive .el-select__tags')
    tagBox[0].style.maxWidth = `${this.selectorWidth}px`
    const tagEls = this.$el.querySelectorAll('.source-hive .el-tag')
    for (let i = 0; i < tagEls.length; i++) {
      const tagEl = tagEls[i]
      tagEl.title = tagEl.innerText
      tagEl.style.maxWidth = `${this.selectorWidth - 5}px`
    }
  }
  async loadDatabase () {
    if (this.$refs['tree-list']) {
      this.$refs['tree-list'].showLoading()
    }
    this.loadingTreeData = true
    try {
      const projectName = this.currentSelectedProject
      const sourceType = this.sourceType
      const res = await this.fetchDatabase({ projectName, sourceType })
      this.treeData = this.getDatabaseTree(await handleSuccessAsync(res))
      this.isDatabaseError = false
      this.$nextTick(() => {
        Scrollbar.init(this.$el.querySelector('.filter-tree'))
      })
    } catch (e) {
      this.isDatabaseError = true
      handleError(e)
    }
    if (this.$refs['tree-list']) {
      this.$refs['tree-list'].hideLoading()
    }
    this.loadingTreeData = false
  }
  async loadTables ({database, tableName = '', isTableReset = false}) {
    const projectName = this.currentSelectedProject
    const sourceType = this.sourceType
    const databaseName = database.id
    const pagination = database.pagination
    const response = await this.fetchTables({ projectName, sourceType, databaseName, tableName, ...pagination })
    const { total_size: size, value: tables } = await handleSuccessAsync(response)

    this.getTableTree(database, { size, tables }, isTableReset, this.selectTablesNames)
    this.setNextPagination(pagination)
    // this.$nextTick(() => {
    //   this.changeDataBase(databaseName)
    // })
    // this.$emit('input', { selectedTables: [...this.selectedTables] })
  }
  async loadDatabaseAndTables (filterText) {
    if (this.$refs['tree-list']) {
      this.$refs['tree-list'].showLoading()
    }
    this.loadingTreeData = true
    try {
      let params = {
        projectName: this.currentSelectedProject,
        sourceType: this.sourceType,
        page_offset: 0,
        page_size: pageSizeMapping.TABLE_TREE,
        table: filterText || ''
      }
      const res = await this.fetctDatabaseAndTables(params)
      const results = await handleSuccessAsync(res)
      results.databases.forEach((item) => {
        this.allDatabasesSizeObj[item.dbname] = item.size
      })
      this.treeKey = filterText ? filterText + Number(new Date()) : 'HIVETREE'
      this.treeData = this.getDatabaseTablesTree(results.databases)
      this.treeData.forEach((database, index) => {
        const pagination = database.pagination
        const size = database.size
        const tables = database.originTables
        this.getTableTree(database, { size, tables }, true)
        this.setNextPagination(pagination)
      })
      // 搜索后，没匹配上库名时，需要展开，匹配上库名不用展开
      this.defaultExpandedKeys = []
      if (filterText) {
        let tempArr = this.treeData.filter((item) => {
          let dbName = (item.id).toLocaleLowerCase()
          let searchText = (filterText).toLocaleLowerCase()
          // db 中没有含关键字的要展开 包括db. 这种情况
          if (dbName.indexOf(searchText) === -1) {
            return item
          }
        })
        let defaultExpandedKeysAll = tempArr.map((item) => {
          return item.id
        })
        // 如果需要展开的量超过100，就只展开前100，对页面一次渲染上千的情况进行保护，以防浏览器崩溃
        this.defaultExpandedKeys = defaultExpandedKeysAll.length > 30 ? defaultExpandedKeysAll.splice(0, 30) : defaultExpandedKeysAll
      }
      this.isDatabaseError = false
      this.$nextTick(() => {
        Scrollbar.init(this.$el.querySelector('.filter-tree'))
      })
    } catch (e) {
      this.isDatabaseError = true
      handleError(e)
    }
    // 搜索结果为空，刷新状态正在刷新，且距离上次刷新时间大于10年时间，说明是第一次刷新，这时候 loading 不隐藏，继续以 loading 的效果显示，反之则隐藏 loading  && this.reloadHiveTablesStatus.time > 10 * 365 * 24 * 60 * 60 * 1000
    if (!(this.reloadHiveTablesStatus.isRunning && this.reloadHiveTablesStatus.time > 10 * 365 * 24 * 60 * 60 * 1000 && this.treeData.length === 0)) {
      if (this.$refs['tree-list']) {
        this.$refs['tree-list'].hideLoading()
      }
    }
    this.loadingTreeData = false
  }
  handleFilter () {
    // 对比上一次搜索结果，如果一样就不请求接口 - 针对空值情况（修复筛选条件为空时多次调接口tree树不渲染问题）
    if (this.prevFilterText === this.filterText && !this.prevFilterText && !this.filterText) {
      return
    } else {
      this.prevFilterText = this.filterText
    }
    // 只要执行次这个，就设为操作过搜索了，显示刷新数据的条条
    this.filterData = true
    // 如果前一次查询还在进行中，不发第二次接口
    if (this.loadingTreeData) {
      return false
    }
    return new Promise(async resolve => {
      // 每次发起搜索时，清空前一次的数据树
      this.loadingTreeData = true
      this.treeData = []
      // 发一个接口就行
      await this.loadDatabaseAndTables(this.filterText)
      this.onSelectedItemsChange()
      resolve()
    })
  }
  async handleSelectDatabase (event, data) {
    event.preventDefault()
    event.stopPropagation()
    this.selectedDatabases.includes(data.id)
      ? this.handleRemoveDatabase(data.id)
      : this.handleAddDatabase(data.id)
  }
  async handleClickNode (data, node, event) {
    if ((data.type === 'table' && data.clickable)) {
      this.selectedTables.includes(data.id)
        ? this.handleRemoveTable(data.id)
        : this.handleAddTable(data.id)
    }
    // 点击数据库节点时，不用再重新获取了
    /* if (data.type === 'datasource' && this.isDatabaseError) {
      await this.loadDatabase()
    } */
  }
  handleResize (treeWidth) {
    const marginLeft = treeWidth + 25 + 20
    this.contentStyle.marginLeft = `${marginLeft}px`
    this.contentStyle.width = `${this.$el.clientWidth - marginLeft}px`
  }
  async handleNodeExpand (data) {
    if (data.isLoading) {
      if (data.type === 'database') {
        await this.loadTables({ database: data })
      }
      this.hideNodeLoading(data)
    }
  }
  async handleLoadMore (data) {
    let dbName = (data.parent.label).toLocaleLowerCase()
    const database = this.treeData.find(database => database.id === data.parent.id)
    // 加载更多时，要将查询的关键字解析处理
    let tableName = ''
    // 如果完全匹配 db，或者是搜索的关键字包含在 dbName 中，这时搜索table的关键字应该是空
    if (dbName.indexOf(this.filterText.toLocaleLowerCase()) > -1) {
      tableName = ''
    } else { // 只有没有完全匹配db时，才会将关键字传
      let idx = this.filterText.indexOf('.')
      tableName = idx === -1 ? this.filterText : this.filterText.substring(idx + 1, this.filterText.length)
    }
    this.loadTables({ database, tableName })
  }
  handleAddDatabase (addDatabaseId) {
    let selectedTables = this.selectedTables
    let selectedDatabases = addDatabaseId instanceof Array ? addDatabaseId : [...this.selectedDatabases, addDatabaseId]

    selectedDatabases.forEach(database => {
      selectedTables = selectedTables.filter(table => table.indexOf(`${database}.`) !== 0)
    })
    this.$emit('input', { selectedDatabases, selectedTables })
  }
  handleSampling (needSampling) {
    this.$emit('input', { needSampling })
    if (!needSampling) {
      this.errorMsg = ''
      this.contentStyle.height = '367px'
    }
  }
  handleSamplingRows (samplingRows) {
    if (samplingRows && samplingRows < 10000) {
      this.errorMsg = this.$t('minNumber')
      this.contentStyle.height = '328px'
    } else if (samplingRows && samplingRows > 20000000) {
      this.errorMsg = this.$t('maxNumber')
      this.contentStyle.height = '328px'
    } else if (!samplingRows) {
      this.errorMsg = this.$t('invalidType')
      this.contentStyle.height = '328px'
    } else {
      this.errorMsg = ''
      this.contentStyle.height = '367px'
    }
    this.$emit('input', { samplingRows })
  }
  handleRemoveDatabase (removeDatabaseId) { // 树上还是调用了这个的
    const selectedDatabases = this.selectedDatabases.filter(databaseId => databaseId !== removeDatabaseId)
    this.$emit('input', { selectedDatabases })
  }
  handleAddTable (addTableId) {
    const selectedTables = addTableId instanceof Array ? addTableId : [...this.selectedTables, addTableId]
    this.$emit('input', { selectedTables })
  }
  handleRemoveTable (removeTableId) { // 树上还是调用了这个的
    const selectedTables = this.selectedTables.filter(tableId => tableId !== removeTableId)
    this.$emit('input', { selectedTables })
  }
  handleValidateFail () {
    this.$message(this.$t('selectedHiveValidateFailText'))
  }
  handleHideTips () {
    this.isShowTips = false
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';

.source-hive {
  &.zh-lang{
    .content-body.has-tips{
      height: 264px;
    }
    .tips{
      height: 62px;
    }
  }
  .list {
    float: left;
  }
  .treeBox{
    width: 400px;
    float: left;
    position: relative;
    border: 1px solid @ke-border-secondary;
    margin: 8px 0 24px 24px;
    .filter-tree{
      border: none;
    }
    .table-tree {
      width: 400px;
    }
    .table-tree.has-refresh {
      .filter-tree {
        height: 410px;
      }
    }
    .refreshNow{
      z-index: 2;
      width: 100%;
      text-align: center!important;
      border-top: 1px solid @ke-border-secondary;
      height: 24px;
      line-height: 24px;
      font-size: 12px;
      color: @text-normal-color;
      background: #fff;
      a{
        color: @base-color;
        margin-right:5px;
        &:hover{
          text-decoration: none;
          color: @base-color-2;
          cursor: pointer;
        }
      }
      &.isRefresh{
        color: @text-disabled-color;
        a{
          color: @text-disabled-color;
          &:hover{
            text-decoration: none;
            cursor: not-allowed;
          }
        }
      }
    }
    &.hasRefreshBtn{
      .filter-tree{
        height: calc(410px);
        margin-bottom: 24px;
      }
    }
  }
  .split {
    position: absolute;
    top: 50%;
    right: 0;
    transform: translate(20px, 100%);
    * {
      cursor: default;
    }
  }
  .filter-box {
    box-sizing: border-box;
    margin-bottom: 10px;
    width: 210px;
  }
  .filter-tree {
    height: 430px;
    overflow: auto;
    border: 1px solid @ke-border-secondary;
  }
  .content {
    margin-left: calc(400px + 25px + 10px);
    padding: 66px 24px 16px 0;
    position: relative;
    // height: 453px;
  }
  .sample-block {
    margin-left: calc(400px + 25px + 10px);
    margin-top: -13px;
    .sample-desc {
      color: @text-title-color;
      word-break: break-word;
      .error-msg {
        color: @color-danger;
        font-size: 12px;
      }
      .is-error .el-input__inner{
        border-color: @color-danger;
      }
    }
    &.has-error {
      margin-top: 5px;
    }
  }
  .content-body {
    position: relative;
    height: 357px;
    border: 1px solid @ke-border-secondary;
    transition: height .2s .2s;
    overflow: auto;
    &.has-error-msg {
      height: 328px;
    }
  }
  &.zh-lang .content-body.has-tips {
    height: 265px;
    &.has-error-msg {
      height: 243px;
    }
  }
  .content-body.has-tips {
    height: 240px;
    &.has-error-msg {
      height: 213px;
    }
  }
  .el-tag {
    margin-right: 10px;
  }
  .databases,
  .tables {
    padding: 15px;
    .header {
      color: @text-normal-color;
      margin-bottom: 2px;
    }
    .names .el-select {
      width: 100%;
    }
    .names .el-select .el-input__inner {
      border: none;
      padding: 0 26px 0 0px;
    }
    .names .el-select .el-input__suffix {
      display: none;
    }
    .names .el-select .el-select__input {
      /* width: 1px !important; */
      margin-left: 0px;
    }
    .el-tag {
      position: relative;
      margin-right: 5px;
      margin-left: 0px;
      padding-right: 25px;
      display: inline-block;
      text-overflow: ellipsis;
      overflow: hidden;
      margin: 0 5px 2px 0;
    }
    .el-tag .el-tag__close {
      position: absolute;
      top: 50%;
      right: 2px;
      transform: scale(.8) translateY(-50%);
    }
  }
  .category {
    border-bottom: 1px solid @ke-border-secondary;
  }
  .category:last-child {
    border-bottom: none;
  }
  .empty {
    position: absolute;
    top: 30%;
    left: 50%;
    transform: translate(-50%, -30%);
    // text-align: center;
  }
  .empty-img {
    width: 40px;
    margin-bottom: 7px;
  }
  .empty-text {
    font-size: 14px;
    line-height: 1.5;
    color: @text-disabled-color;
  }
  .tips {
    position: absolute;
    padding: 10px;
    /* height: 63px; */
    height: 90px;
    border-radius: 2px;
    background-color: @regular-background-color;
    // bottom: 25px;
    margin-top: 10px;
    right: 20px;
    width: 485px;
    .infoIcon{
      position: absolute;
      top: 10px;
      left: 10px;
      color: @text-disabled-color;
    }
    .header {
      color: @text-title-color;
      font-size: 12px;
      margin-bottom: 2px;
    }
    .body {
      line-height: 1.4;
      color: @text-title-color;
      font-size: 12px;
      &.zh-body{
        line-height: 1.5;
      }
    }
    ul, li {
      list-style: none;
    }
    ul {
      padding-left:20px;
      li{
        margin-top:5px;
        &:first-child{
          margin-top:0;
        }
      }
    }
    .close {
      position: absolute;
      top: 12px;
      right: 12px;
      font-size: 14px;
      cursor: pointer;
      color: @text-normal-color;
    }
  }
  .fade-enter-active, .fade-leave-active {
    transition: opacity .2s;
  }
  .fade-enter, .fade-leave-to {
    opacity: 0;
  }
  // 定制化datasource tree样式
  .table-tree {
    // 定制样式: database
    .el-tree > .el-tree-node > .el-tree-node__content {
      position: relative;
    }
    .el-tree > .el-tree-node {
      border-bottom: 1px solid @line-border-color;
    }
    .el-tree > .el-tree-node > .el-tree-node__content > .tree-item {
      position: static;
    }
    .el-tree-node {
      overflow: hidden;
    }
    .el-tree .el-tree-node__content {
      .tree-item {
        width: 377px;
      }
      .database .label {
        text-overflow:ellipsis !important;
        overflow:hidden !important;
        word-break:keep-all !important;
        white-space:nowrap !important;
        width: 98%;
        line-height: 35px\0;
      }
      &:hover .database .label{
        width: 85%;
      }
    }
    .select-all {
      display: none;
      position: absolute;
      top: 50%;
      right: 10px;
      transform: translateY(-50%);
      line-height: 36px;
      font-size: 12px;
      &:hover {
        color: #0988de;
      }
    }
    .el-tree-node__expand-icon {
      padding-top: 0;
      padding-bottom: 0;
    }
    .el-tree-node__content {
      min-height: 16px;
      position:relative;
    }
    .el-tree-node__content:hover .select-all {
      display: block;
    }
    .label-synced {
      position: absolute;
      top: 50%;
      right: 14px;
      color: @text-disabled-color;
      transform: translateY(-50%);
      font-size: 13px\0;
    }
    .tree-item {
      &>div {
        margin-right:34px;
        &.is-synced {
          margin-right:108px;
        }
        &.database {
          margin-right:0;
        }
      }
      user-select: none;
      width: 100%;
      white-space: normal;
      line-height: 35px\0;
    }
    .el-icon-ksd-good_health {
      color: @color-success;
    }
    .database {
      margin-right: 0;
      .el-icon-ksd-good_health {
        margin-right: 5px;
      }
    }
    .table {
      .el-icon-ksd-good_health {
        position: absolute;
        // left: 0;
        top: 50%;
        transform: translate(-20px, -50%);
      }
    }
    .selected {
      .database,
      .table {
        color: @base-color;
        &.disabled {
          color: @text-title-color;
        }
      }
      .table {
        padding-left: 20px;
        &.synced {
          padding-left:0;
        }
      }
    }
    .database,
    .table {
      position:relative;
      overflow:hidden;
      text-overflow:ellipsis;
      color: @text-title-color;
    }
    .table.parent-selected .el-icon-ksd-good_health {
      transform: translate(-16px, -50%);
    }
    .table.parent-selected {
      padding-left: 18px;
    }
    .load-more {
      line-height: inherit;
    }
  }
}
</style>
