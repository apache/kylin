<template>
  <div>
    <el-dialog
      :visible="isShow"
      top="5vh"
      width="960px"
      limited-area
      :close-on-press-escape="false"
      :close-on-click-modal="false"
      @close="handleClose"
      :title="type === 'repair' ? $t('repairSnapshotTitle') : step === 'one' ? $t('addSnapshotTitle') : $t('sourceTablePartitionSetting')"
      class="add-snapshot-dialog ke-it-add_snapshot_dialog">
      <div class="add-snapshot clearfix" :class="{'zh-lang': $store.state.system.lang !== 'en'}" v-if="step === 'one'">
        <div class="list clearfix">
          <div class="ksd-ml-20 ksd-mt-20">
            <el-input :placeholder="$t('filterTableName')"
              v-model="filterText"
              prefix-icon="el-ksd-icon-search_22"
              @keyup.enter.native="handleFilter()"
              @clear="handleFilter()"
              class="ke-it-filter_input"
            >
            </el-input>
          </div>
          <div class="snapshot-tree">
            <TreeList
              :tree-key="treeKey"
              :show-overflow-tooltip="true"
              ref="tree-list"
              :class="['table-tree', {'has-refresh': loadHiveTableNameEnabled === 'true'}, 'ke-it-snapshot_tree']"
              :data="treeData"
              :placeholder="$t('filterTableName')"
              :is-show-filter="false"
              :is-show-resize-bar="false"
              :filter-white-list-types="['datasource', 'database']"
              @click="handleClickNode"
              @node-expand="handleNodeExpand"
              @load-more="handleLoadMore"
              :default-expanded-keys="defaultExpandedKeys"
            />
            <div class="empty" v-if="!loadingTreeData && treeData.length===0">
              <p class="empty-text" v-html="emptyText"></p>
            </div>
            <p class="ksd-right refreshNow" :class="{'isRefresh': reloadHiveTablesStatus.isRunning || hasClickRefreshBtn}" v-if="loadHiveTableNameEnabled === 'true'">{{$t('refreshText')}} <a href="javascript:;" @click="refreshHive(true)">{{refreshBtnText}}</a><el-tooltip class="item" effect="dark" :content="$t('refreshTips')" placement="top"><i class="el-icon-ksd-what"></i></el-tooltip></p>
          </div>
        </div>
        <div class="content">
          <div :class="['content-body', {'is-error': emptyPartitionSetting}]">
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
                  changeable="ke-it-select_db"
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
                  changeable="ke-it-select_table"
                  @removeTag="removeSelectedTable"
                  :datamap="{label: 'label', value: 'value'}">
                </arealabel>
              </div>
            </div>
            <p class="error-tip ksd-mt-5" v-if="emptyPartitionSetting"><i class="el-icon-ksd-error_01 ksd-mr-5"></i>{{$t('emptyPartitionSettingTip')}}</p>
          </div>
        </div>
      </div>
      <div class="partition-setting-layout" v-else>
        <p class="ksd-mb-10">{{$t('sourceTablePartitionTip')}}</p>
        <div class="search-partition-input"><el-input v-model="searchDBOrTableName" size="medium" v-global-key-event.enter.debounce="searchPartitionColumns" @clear="searchPartitionColumns" prefix-icon="el-ksd-icon-search_22" style="width:280px" :placeholder="$t('pleaseFilterDBOrTable')"></el-input></div>
        <template v-if="partitionColumnData && partitionColumnData.list.length">
          <el-row class="ksd-mb-10" :gutter="5">
            <el-col :span="7">{{$t('table')}}</el-col>
            <el-col :span="8">{{$t('partitionColumn')}}</el-col>
            <el-col :span="8">{{$t('partitionValue')}}</el-col>
          </el-row>
          <el-row v-for="(item, index) in partitionColumnData.list" :key="index" :gutter="5" class="ksd-mt-5">
            <el-col :span="7">
              <!-- <el-input :value="`${item.database}.${item.table}`" :disabled="true" style="width: 100%;"></el-input> -->
              <span class="snapshot-table-name" v-custom-tooltip="{text: `${item.database}.${item.table}`, w: 20, position: 'bottom-start', 'visible-arrow': false, 'popper-class': 'popper--small'}">{{`${item.database}.${item.table}`}}</span>
            </el-col>
            <el-col :span="8">
              <el-select v-model="item.partition_column" :placeholder="$t('selectPartitionPlaceholder')" @change="changePartitionColumns(item, item)" style="width: 100%;" :disabled="item.isLoadingPartition || item.source_type !== 9">
                <el-option :label="$t('noPartition')" value=""></el-option>
                <el-option :label="item.partition_col" :value="item.partition_col" v-if="item.partition_col">
                  <el-tooltip :content="item.partition_col" effect="dark" placement="top"><span style="float: left">{{ item.partition_col | omit(30, '...') }}</span></el-tooltip>
                  <span class="ky-option-sub-info">{{ item.partition_col_type.toLocaleLowerCase() }}</span>
                </el-option>
                <!-- <p class="more-partition" @click="showAllPartition(item)" v-if="!item.showMore">{{$t('viewAllPartition')}}</p> -->
                <template v-if="item.showMore">
                  <!-- <el-option :label="key" :value="key" v-for="(key, value) in item.other_column_and_type" :key="key"></el-option> -->
                  <el-option :label="value" :value="value" v-for="(key, value) in item.other_column_and_type" :key="value" :disabled="item.undefinedPartitionColErrorTip">
                    <el-tooltip :content="value" effect="dark" placement="top"><span style="float: left">{{ value | omit(30, '...') }}</span></el-tooltip>
                    <span class="ky-option-sub-info">{{ key.toLocaleLowerCase() }}</span>
                  </el-option>
                </template>
                <!-- <el-option label="" value="" v-if="partitionMeta.column && timeDataType.indexOf(getColumnInfo(partitionMeta.column).datatype)===-1"></el-option> -->
              </el-select>
              <p class="error-msg" v-if="item.fetchError"><i class="el-icon-ksd-error_01 error-icon"></i>{{$t('fetchPartitionErrorTip')}}</p>
              <p class="alert-msg" v-if="item.undefinedPartitionColErrorTip"><i class="el-icon-ksd-alert alert-icon"></i>{{$t('undefinedPartitionColErrorTip')}}</p>
            </el-col>
            <el-col :span="8">
              <el-select
                v-if="refreshSelectValue"
                :class="['partition-value-select', {'is-error': incrementalBuildErrorList.includes(`${item.database}.${item.table}`)}]"
                v-model="item.partition_values"
                multiple
                collapse-tags
                filterable
                :loading="item.loadPatitionValues"
                @change="changePartitionValues(item)"
                :disabled="!item.partition_column"
                :placeholder="$t('kylinLang.common.pleaseSelect')">
                <el-option-group
                  class="group-partitions"
                  :label="$t('readyPartitions')">
                  <span class="partition-count">{{item.readyPartitions.length}}</span>
                  <el-option
                    v-for="item in item.readyPartitions.slice(0, item.pageSize * item.pageReadyPartitionsSize)"
                    :key="item.value"
                    :label="item.label"
                    :value="item.value">
                  </el-option>
                  <p class="page-value-more" v-show="item.pageReadyPartitionsSize * item.pageSize < item.readyPartitions.length" @click.stop="item.pageReadyPartitionsSize += 1">{{$t('kylinLang.common.loadMore')}}</p>
                </el-option-group>
                <el-option-group
                  class="group-partitions"
                  :label="$t('notReadyPartitions')">
                  <span class="partition-count">{{item.notReadyPartitions.length}}</span>
                  <el-option
                    v-for="item in item.notReadyPartitions.slice(0, item.pageSize * item.pageNotReadyPartitions)"
                    :key="item.value"
                    :label="item.label"
                    :value="item.value">
                  </el-option>
                  <p class="page-value-more" v-show="item.pageNotReadyPartitions * item.pageSize < item.notReadyPartitions.length" @click.stop="item.pageNotReadyPartitions += 1">{{$t('kylinLang.common.loadMore')}}</p>
                </el-option-group>
              </el-select>
              <p class="error-tip" v-if="incrementalBuildErrorList.includes(`${item.database}.${item.table}`)">{{$t('noPartitionValuesError')}}</p>
            </el-col>
            <el-col :span="1">
              <el-tooltip effect="dark" :content="$t('detectPartition')" placement="top">
                <div style="display: inline-block;">
                  <el-button
                    size="medium"
                    :disabled="item.source_type !== 9"
                    :loading="item.isLoadingPartition"
                    icon="el-ksd-icon-data_range_search_old"
                    @click="handleLoadPartitionColumn(item)">
                  </el-button>
                </div>
              </el-tooltip>
            </el-col>
          </el-row>
          <el-pagination
            background
            layout="prev, pager, next"
            :total="partitionColumnData.total_size"
            :currentPage='partitionColumnData.page_offset + 1'
            :pageSize="partitionColumnData.page_size"
            @current-change="pageCurrentChange"
            class="ksd-center ksd-mt-10"
            v-show="partitionColumnData.list.length && type !== 'repair'">
          </el-pagination>
        </template>
      </div>
      <span slot="footer" class="dialog-footer ky-no-br-space ke-it-btns">
        <el-button size="medium" v-if="step === 'one'" @click="handleClose">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" size="medium" :disabled="!selectedTables.length && !selectedDatabases.length" :loading="submitLoading" v-if="step === 'one'" @click="submit">{{$t('kylinLang.common.next')}}</el-button>
        <el-button size="medium" v-if="step === 'two' && type === 'new'" @click="handlerPrevStep">{{$t('preStep')}}</el-button>
        <el-button type="primary" size="medium" :loading="submitLoading" v-if="step === 'two'" :disabled="!partitionColumnData.list.length" @click="submitPartition">{{$t('kylinLang.common.add')}}</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapGetters, mapMutations, mapActions } from 'vuex'

import vuex from '../../../../store'
import locales from './locales'
import store, { types } from './store'
import { handleSuccessAsync, handleError } from 'util/index'
import TreeList from '../../../common/TreeList'
import arealabel from '../../../common/area_label.vue'
import { getTableTree, getDatabaseTablesTree } from './handler'
import Scrollbar from 'smooth-scrollbar'

vuex.registerModule(['modals', 'SnapshotModel'], store)
@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ]),
    // Store数据注入
    ...mapState('SnapshotModel', {
      isShow: state => state.isShow,
      callback: state => state.callback,
      type: state => state.type,
      repairData: state => state.repairData
    }),
    ...mapState({
      // loadHiveTableNameEnabled: state => state.system.loadHiveTableNameEnabled
    })
  },
  methods: {
    ...mapActions({
      fetchUnbuildSnapshotTables: 'FETCH_UNBUILD_SNAPSHOT_TABLES',
      fetchDatabaseMoreTables: 'FETCH_DATABASE_MORE_TABLES',
      buildSnapshotTables: 'BUILD_SNAPSHOT_TABLES',
      fetchPartitionConfig: 'FETCH_PARTITION_CONFIG',
      reloadPartitionColumn: 'RELOAD_PARTITION_COLUMN',
      getSnapshotPartitionValues: 'GET_SNAPSHOT_PARTITION_VALUES'
    }),
    // Store方法注入
    ...mapMutations('SnapshotModel', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    })
  },
  components: {
    TreeList,
    arealabel
  },
  locales
})
export default class SnapshotModel extends Vue {
  step = 'one'
  loadHiveTableNameEnabled = false
  submitLoading = false
  filterData = false
  allDatabasesSizeObj = {}
  databasesSize = {}
  loadingTreeData = false
  treeData = []
  defaultExpandedKeys = []
  reloadHiveTablesStatus = { // 记录当前的刷新状态
    isRunning: false,
    time: 0
  }
  hasClickRefreshBtn = false
  treeKey = 'tree' + Number(new Date())
  splitChar = ','
  regex = {
    validateTable: /^\s*;?(\w+\.\w+)\s*(,\s*\w+\.\w+)*;?\s*$/,
    validateDB: /^\s*;?(\w+)\s*(,\s*\w+)*;?\s*$/
  }
  selectTablesNames = []
  selectDBNames = []
  prevFilterText = ''
  filterText = ''
  tablesNum = 0
  selectedTables = []
  selectedDatabases = []
  searchDBOrTableName = ''
  partitionColumnData = {
    list: [],
    page_offset: 0,
    page_size: 10,
    total_size: 0,
    excludeBroken: true
  }
  partitionOptions = {}
  emptyPartitionSetting = false
  incrementalBuildErrorList = []
  refreshSelectValue = true

  constructor () {
    super()
    this.getTableTree = getTableTree.bind(this)
    this.getDatabaseTablesTree = getDatabaseTablesTree.bind(this)
  }
  get emptyText () {
    return this.filterText ? this.$t('kylinLang.common.noResults') : this.$t('noSourceData')
  }
  get refreshBtnText () {
    return this.reloadHiveTablesStatus.isRunning || this.hasClickRefreshBtn ? this.$t('refreshIng') : this.$t('refreshNow')
  }
  @Watch('isShow')
  onDialogOpen (val) {
    if (val) {
      if (this.type === 'repair') {
        this.step = 'two'
        this.selectedTables = this.repairData.map(it => `${it.database}.${it.table}`)
        this.partitionColumnData.excludeBroken = false
        this.getPartitionColumns()
      } else {
        this.loadDatabaseAndTables()
      }
    }
  }
  async loadDatabaseAndTables (filterText) {
    const filter = filterText || ''
    if (this.$refs['snapshot-tree']) {
      this.$refs['snapshot-tree'].showLoading()
    }
    this.loadingTreeData = true
    try {
      const data = {
        project: this.currentSelectedProject,
        page_offset: 0,
        page_size: 10,
        table: filter
      }
      const res = await this.fetchUnbuildSnapshotTables(data)
      const results = await handleSuccessAsync(res)
      results.databases.forEach((item) => {
        this.allDatabasesSizeObj[item.dbname] = item.size
      })
      this.treeKey = filter + Number(new Date())
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
      this.$nextTick(() => {
        Scrollbar.init(this.$el.querySelector('.filter-tree'))
      })
      this.loadingTreeData = false
    } catch (e) {
      this.loadingTreeData = false
      handleError(e)
    }
  }
  async handleSelectDatabase (event, data) {
    event.preventDefault()
    event.stopPropagation()
    this.selectedDatabases.includes(data.id)
      ? this.handleRemoveDatabase(data.id)
      : this.handleAddDatabase(data.id)
  }
  handleAddDatabase (addDatabaseId) {
    let selectedTables = this.selectedTables
    let selectedDatabases = addDatabaseId instanceof Array ? addDatabaseId : [...this.selectedDatabases, addDatabaseId]

    selectedDatabases.forEach(database => {
      selectedTables = selectedTables.filter(table => table.indexOf(`${database}.`) !== 0)
    })
    this.selectedDatabases = selectedDatabases
    this.selectedTables = selectedTables
  }
  handleRemoveDatabase (removeDatabaseId) { // 树上还是调用了这个的
    const selectedDatabases = this.selectedDatabases.filter(databaseId => databaseId !== removeDatabaseId)
    this.selectedDatabases = selectedDatabases
  }
  handleAddTable (addTableId) {
    const selectedTables = addTableId instanceof Array ? addTableId : [...this.selectedTables, addTableId]
    this.selectedTables = selectedTables
  }
  handleRemoveTable (removeTableId) { // 树上还是调用了这个的
    const selectedTables = this.selectedTables.filter(tableId => tableId !== removeTableId)
    this.selectedTables = selectedTables
  }
  async handleClickNode (data, node, event) {
    if ((data.type === 'table' && data.clickable)) {
      this.selectedTables.includes(data.id)
        ? this.handleRemoveTable(data.id)
        : this.handleAddTable(data.id)
    }
  }
  async handleNodeExpand (data) {
    if (data.isLoading) {
      if (data.type === 'database') {
        await this.loadTables({ database: data })
      }
      this.hideNodeLoading(data)
    }
  }
  async loadTables ({database, table = '', isTableReset = false}) {
    const project = this.currentSelectedProject
    const databaseId = database.id
    const pagination = database.pagination
    const response = await this.fetchDatabaseMoreTables({ project, database: databaseId, table, ...pagination })
    const { total_size: size, value: tables } = await handleSuccessAsync(response)

    this.getTableTree(database, { size, tables }, isTableReset, this.selectTablesNames)
    this.setNextPagination(pagination)
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
  removeSelectedDB (val) {
    this.selectDBNames.splice(this.selectDBNames.indexOf(val), 1)
    let selectedDatabases = this.selectedDatabases.filter((db) => {
      return db !== val
    })
    this.selectedDatabases = selectedDatabases
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
    this.selectedDatabases = [...this.selectDBNames]
    this.selectedTables = selectedTables
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
    this.selectedTables = selectedTables
  }
  removeSelectedTable (val) {
    this.selectTablesNames.splice(this.selectTablesNames.indexOf(val), 1)
    const selectedTables = this.selectedTables.filter(tableId => tableId !== val)
    this.selectedTables = selectedTables
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
    this.emptyPartitionSetting = false
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
  selectedDBValidateFail () {
    this.$message(this.$t('selectedDBValidateFailText'))
  }
  selectedTableValidateFail () {
    this.$message(this.$t('selectedTableValidateFailText'))
  }
  resetData () {
    this.submitLoading = false
    this.filterData = false
    this.allDatabasesSizeObj = {}
    this.databasesSize = {}
    this.loadingTreeData = false
    this.treeData = []
    this.defaultExpandedKeys = []
    this.selectTablesNames = []
    this.selectDBNames = []
    this.prevFilterText = ''
    this.filterText = ''
    this.tablesNum = 0
    this.selectedTables = []
    this.selectedDatabases = []
    this.searchDBOrTableName = ''
    this.partitionOptions = {}
    this.partitionColumnData.page_offset = 0
    this.partitionColumnData.list = []
    this.partitionColumnData.excludeBroken = true
    this.emptyPartitionSetting = false
    this.incrementalBuildErrorList = []
    this.step = 'one'
  }
  handleClose (isSubmit) {
    this.hideModal()
    this.resetData()
    this.callback && this.callback(isSubmit)
  }

  // 更改表分区列
  // self 控制 dom 是否重新渲染
  async changePartitionColumns (item, self) {
    item.fetchError = false
    try {
      if (item.partition_column) {
        self.loadPatitionValues = true
        const result = await this.getSnapshotPartitionValues({
          project: this.currentSelectedProject,
          table_cols: {
            [`${item.database}.${item.table}`]: item.partition_column
          }
        })
        const partitionValues = result.data.data
        if (partitionValues && partitionValues[`${item.database}.${item.table}`]) {
          const values = partitionValues[`${item.database}.${item.table}`]
          self.readyPartitions = values.ready_partitions.map(it => ({label: it, value: it}))
          self.notReadyPartitions = values.not_ready_partitions.map(it => ({label: it, value: it}))
          // this.$set(item, 'readyPartitions', values.ready_partitions.map(it => ({label: it, value: it})))
          // this.$set(item, 'notReadyPartitions', values.not_ready_partitions.map(it => ({label: it, value: it})))
          this.refreshSelectValue = false
          this.$nextTick(() => {
            this.refreshSelectValue = true
          })
        } else {
          item.partition_values = []
          item.readyPartitions = []
          item.notReadyPartitions = []
        }
        self.loadPatitionValues = false
      } else {
        item.partition_values = []
        item.readyPartitions = []
        item.notReadyPartitions = []
      }

      this.partitionOptions[`${item.database}.${item.table}`] = {
        partition_col: item.partition_column
      }
    } catch (e) {
      self.loadPatitionValues = false
      handleError(e)
    }
  }

  // 是否展示全部分区
  showAllPartition (item) {
    item.showMore = true
  }
  // 分区列设置搜索表名或列名
  searchPartitionColumns () {
    this.getPartitionColumns()
  }
  async submit () {
    this.getPartitionColumns().then((res) => {
      if (res.total_size > 0) {
        this.step = 'two'
      } else {
        this.emptyPartitionSetting = true
      }
    })
  }
  async getPartitionColumns () {
    return new Promise(async (resolve, reject) => {
      const {page_offset, page_size, excludeBroken} = this.partitionColumnData
      try {
        const res = await this.fetchPartitionConfig({project: this.currentSelectedProject, table_pattern: this.searchDBOrTableName, tables: this.selectedTables.join(','), databases: this.selectedDatabases.join(','), page_offset, page_size, include_exist: false, exclude_broken: excludeBroken})
        const results = await handleSuccessAsync(res)
        this.partitionColumnData.list = results.value.map(item => {
          let partition_column = ''
          if (`${item.database}.${item.table}` in this.partitionOptions) {
            partition_column = this.partitionOptions[`${item.database}.${item.table}`].partition_col
          }
          return {
            ...item,
            partition_column,
            partition_values: [],
            isLoadingPartition: false,
            showMore: false,
            fetchError: false,
            undefinedPartitionColErrorTip: false,
            notReadyPartitions: [],
            readyPartitions: [],
            loadPatitionValues: false,
            pageReadyPartitionsSize: 1,
            pageNotReadyPartitions: 1,
            pageSize: 100
          }
        })
        this.partitionColumnData.total_size = results.total_size
        resolve(results)
      } catch (e) {
        handleError(e)
        reject(e)
      }
    })
  }
  // 设置分区列分页
  pageCurrentChange (val) {
    this.partitionColumnData.page_offset = val - 1
    this.getPartitionColumns()
  }
  async submitPartition () {
    try {
      const options = {}
      this.partitionColumnData.list.forEach(item => {
        if (!item.partition_column) return
        options[`${item.database}.${item.table}`] = {
          partition_col: item.partition_column,
          incremental_build: !!item.partition_column,
          partitions_to_build: item.partition_column ? item.partition_values : null
        }
      })
      const incrementalBuildErrorList = Object.keys(options).filter(key => options[key].incremental_build && options[key].partitions_to_build.length === 0)
      this.incrementalBuildErrorList = incrementalBuildErrorList
      if (incrementalBuildErrorList.length > 0) return
      this.submitLoading = true
      await this.buildSnapshotTables({project: this.currentSelectedProject, options: options, tables: this.selectedTables, databases: this.selectedDatabases})
      this.submitLoading = false
      this.$message({
        dangerouslyUseHTMLString: true,
        type: 'success',
        customClass: 'build-full-load-success',
        duration: 10000,
        showClose: true,
        message: (
          <div>
            <span>{this.$t('kylinLang.common.buildSuccess')}</span>
            <a href="javascript:void(0)" onClick={() => this.gotoJob()}>{this.$t('kylinLang.common.toJoblist')}</a>
          </div>
        )
      })
      this.handleClose(true)
    } catch (e) {
      handleError(e)
      this.hideModal()
    }
  }
  // 跳转至job页面
  gotoJob () {
    this.$router.push('/monitor/job')
  }
  // 单个获取分区列
  handleLoadPartitionColumn (item) {
    item.isLoadingPartition = true
    this.reloadPartitionColumn({project: this.currentSelectedProject, table: `${item.database}.${item.table}`}).then(async (res) => {
      item.isLoadingPartition = false
      item.fetchError = false
      try {
        const results = await handleSuccessAsync(res)
        if (!results.partition_col) {
          item.undefinedPartitionColErrorTip = true
        }
        item.partition_column = results.partition_col || ''
        if (results.partition_col) {
          this.changePartitionColumns({...results, partition_column: results.partition_col}, item)
        }
      } catch (e) {
      }
    }).catch((e) => {
      item.isLoadingPartition = false
      item.fetchError = true
      handleError(e)
    })
  }
  // 上一步
  handlerPrevStep () {
    this.step = 'one'
    this.partitionOptions = {}
    this.searchDBOrTableName = ''
  }

  // 改变分区列的值
  changePartitionValues (column) {
    const index = this.incrementalBuildErrorList.findIndex(it => it === `${column.database}.${column.table}`)
    index >= 0 && this.incrementalBuildErrorList.splice(index, 1)
  }
}
</script>

<style lang="less">
@import '../../../../assets/styles/variables.less';
.add-snapshot-dialog {
  .el-dialog__body {
    padding: 0;
  }
  .partition-setting-layout {
    padding: 20px;
    box-sizing: border-box;
    .partition-value-select {
      width: 100%;
      &.is-error {
        .el-input input {
          border: 1px solid @error-color-1;
        }
      }
      .el-input__inner {
        padding-left: 15px
      }
    }
    .search-partition-input {
      width: 100%;
      text-align: right;
    }
    .snapshot-table-name {
      line-height: 34px;
    }
    .error-msg {
      color: @color-danger;
      font-size: 12px;
      margin-top: 2px;
      .error-icon {
        margin-right: 5px;
        color: @color-danger;
      }
    }
    .alert-msg {
      font-size: 12px;
      margin-top: 2px;
      .alert-icon {
        margin-right: 5px;
        color: @color-warning;
      }
    }
  }
}
.more-partition {
  color: #989898;
  font-size: 12px;
  padding: 5px 10px;
  box-sizing: border-box;
  cursor: pointer;
}
.error-tip {
  color: @error-color-1;
  font-size: 12px;
}
.add-snapshot {
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
  .snapshot-tree{
    width: 400px;
    float: left;
    position: relative;
    border: 1px solid @ke-border-divider-color;
    margin: 10px 0 20px 20px;
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
      border-top: 1px solid @ke-border-divider-color;
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
    border: 1px solid @ke-border-divider-color;
  }
  .content {
    margin-left: calc(400px + 25px + 10px);
    padding: 65px 20px 0 0;
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
    height: 430px;
    // border: 1px solid @ke-border-divider-color;
    transition: height .2s .2s;
    overflow: auto;
    &.has-error-msg {
      height: 328px;
    }
  }
  .content-body:not(.is-error) {
    .category.tables {
      min-height: 247px;
    }
  }
  .content-body.is-error {
    .category.tables {
      min-height: 225px;
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
    border: 1px solid @ke-border-divider-color;
    min-height: 120px;
    &:first-child {
      border-bottom: 0;
    }
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
      border-bottom: 1px solid @ke-border-divider-color;
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
        text-overflow:ellipsis!important;
        overflow:hidden!important;
        word-break:keep-all!important;
        white-space:nowrap!important;
        width: 98%;
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
      line-height: 34px\0;
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
    }
    .tree-item {
      &>div {
        margin-right:34px;
        &.is-synced {
          margin-right:90px;
        }
        &.database {
          margin-right:0;
        }
      }
      user-select: none;
      width: 100%;
      white-space: normal;
      line-height: 34px\0;
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
.group-partitions {
  .partition-count {
    position: absolute;
    top: 5px;
    right: 16px;
    font-size: 12px;
    color: @text-disabled-color;
  }
  .page-value-more {
    margin-top: 8px;
    text-align: center;
    cursor: pointer;
    font-size: 12px;
    color: @text-disabled-color;
  }
}
</style>
