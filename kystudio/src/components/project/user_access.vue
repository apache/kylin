<template>
    <div class="user-access-block" v-loading="loading">
      <div class="data-permission clearfix">
        <div class="flex ksd-fleft">
          <span class="ksd-title-label">{{$t('dataPermission')}}</span>
          <el-tooltip class="item" effect="dark" :content="$t('dataPermissionTips')" placement="bottom">
            <i class="el-icon-ksd-info ksd-fs-14 ksd-ml-5"></i>
          </el-tooltip>
          <el-switch
            :value="ext_permissions"
            class="ksd-ml-8"
            @change="handleChangePermission"
            :disabled="!isDataPermission"
            :active-text="$t('kylinLang.common.OFF')"
            :inactive-text="$t('kylinLang.common.ON')">
          </el-switch>
        </div>
      </div>
      <el-button type="primary" plain size="small" icon="el-ksd-n-icon-edit-outlined" class="ksd-mt-10" @click="editAccess" v-if="!isEdit && isAuthority && ext_permissions">{{$t('editACL')}}</el-button>
      <el-row class="ksd-mt-10" v-if="ext_permissions">
        <el-col :span="8">
          <div class="access-card">
            <div class="access-title">
              <span v-if="!isEdit">{{$t('accessTables')}} ({{tableAuthorizedNum}})</span>
              <el-checkbox v-model="isAllTablesAccess" @change="checkAllTables" :indeterminate="tableAuthorizedNum !== totalNum && tableAuthorizedNum>0" :disabled="!tables.length" v-else>{{$t('accessTables')}} ({{tableAuthorizedNum}}/{{totalNum}})</el-checkbox>
            </div>
            <div class="access-search">
              <el-input size="mini" :placeholder="$t('searchKey')" v-model="tableFilter">
                <i slot="prefix" class="el-input__icon el-ksd-icon-search_16"></i>
              </el-input>
            </div>
            <div class="access-tips" v-if="isAllTablesAccess&&!isEdit">
              <i class="el-icon-ksd-info ksd-fs-14"></i>
              <span class="ksd-fs-12">{{$t('accessTips')}}</span>
            </div>
            <div class="access-content tree-content" :class="{'all-tips': isAllTablesAccess&&!isEdit}">
              <el-tree
                v-if="filterTableData.length&&isRerender"
                show-overflow-tooltip
                node-key="id"
                ref="tableTree"
                class="acl-tree"
                :data="filterTableData"
                :show-checkbox="isEdit"
                :props="defaultProps"
                :render-after-expand="false"
                :highlight-current="true"
                :default-expanded-keys="defaultExpandedKeys"
                :default-checked-keys="defaultCheckedKeys"
                @check="checkChange"
                @node-expand="pushTableId"
                @node-collapse="removeTableId"
                @node-click="handleNodeClick">
                <span class="custom-tree-node" slot-scope="{ node, data }">
                  <i class="ksd-mr-2" :class="data.icon"></i>
                  <span class="ky-ellipsis" :class="data.class" :title="node.label">{{ node.label }}</span>
                </span>
              </el-tree>
              <kylin-nodata :content="emptyText" v-else>
              </kylin-nodata>
            </div>
          </div>
        </el-col>
        <el-col :span="8">
          <div class="access-card column-card">
            <div class="access-title">
              <span v-if="!isEdit">{{$t('accessColumns')}} ({{colAuthorizedNum}})</span>
              <el-checkbox v-model="selectAllColumns" @change="checkAllColumns" :disabled="!isCurrentTableChecked || !columns.length" :indeterminate="colAuthorizedNum !== columns.length && colAuthorizedNum>0" v-else>{{$t('accessColumns')}} ({{colAuthorizedNum}}/{{columns.length}})</el-checkbox>
            </div>
            <div class="access-search">
              <el-input size="mini" :placeholder="$t('searchKey')" v-model="columnFilter">
                <i slot="prefix" class="el-input__icon el-ksd-icon-search_16"></i>
              </el-input>
            </div>
            <div class="access-tips" v-if="isAllColAccess&&!isEdit">
              <i class="el-icon-ksd-info ksd-fs-14"></i>
              <span class="ksd-fs-12">{{$t('accessColsTips')}}</span>
            </div>
            <div class="access-content" :class="{'all-tips': isAllColAccess&&!isEdit}">
              <div v-if="pagedFilterColumns.length">
                <ul>
                  <li v-for="col in pagedFilterColumns" :key="col.name">
                    <el-checkbox @change="val => selectColumn(val, col.name)" :disabled="!isCurrentTableChecked" size="medium" v-if="isEdit" :value="col.authorized">{{col.name}}</el-checkbox>
                    <span v-else>{{col.name}}</span>
                  </li>
                </ul>
                <div class="list-load-more" @click="loadMoreCols" v-if="pagedFilterColumns.length<filterCols.length">{{$t('loadMore')}}</div>
              </div>
              <kylin-nodata :content="emptyText2" v-else>
              </kylin-nodata>
            </div>
          </div>
        </el-col>
        <el-col :span="8">
          <div class="access-card row-card">
            <div class="access-title">
              <span>{{$t('accessRows')}}</span>
              <!-- <el-button type="primary" plain size="small" icon="el-ksd-icon-add_16" class="ksd-fright ksd-mt-5" @click="addRowAccess" v-if="isEdit" :disabled="!isCurrentTableChecked">{{$t('addRowAccess')}}</el-button> -->
            </div>
            <div class="access-search">
              <el-input size="mini" :placeholder="$t('searchKey')" v-model="rowSearch">
                <i slot="prefix" class="el-input__icon el-ksd-icon-search_16"></i>
              </el-input>
            </div>
            <div class="access-tips" v-if="isCurrentTableChecked&&row_filter&&!row_filter.filter_groups.length">
              <i class="el-icon-ksd-info ksd-fs-14"></i>
              <span class="ksd-fs-12">{{$t('accessRowsTips')}}</span>
            </div>
            <div class="access-content">
              <el-select class="ksd-mt-10 ksd-mb-5 ksd-ml-10 join-type" size="small" v-model="row_filter.type" v-if="isEdit && row_filter && getSearchFilterdGroup(row_filter.filter_groups).length" @change="changeRowFilterType">
                <el-option label="AND" value="AND"></el-option>
                <el-option label="OR" value="OR"></el-option>
              </el-select>
              <div v-for="(fg, fgIndex) in getSearchFilterdGroup(row_filter.filter_groups)" :key="fgIndex">
                <div class="filter-groups" :class="{'is-group': fg.is_group}">
                  <div class="filter-group-block">
                    <div class="clearfix">
                      <el-select class="join-type ksd-fleft" size="small" v-model="fg.type" @change="changeFilterGroupTpye(fgIndex)" v-if="fg.is_group&&fg.filters.length&&isEdit">
                        <el-option label="AND" value="AND"></el-option>
                        <el-option label="OR" value="OR"></el-option>
                      </el-select>
                    </div>
                      <el-dropdown class="group-action-btn" size="small" v-if="fg.is_group&&isEdit">
                        <span class="el-dropdown-link">
                          <i class="el-icon-ksd-table_others"></i>
                        </span>
                        <el-dropdown-menu slot="dropdown">
                          <el-dropdown-item @click.native="addRowAccess(fgIndex)">
                            <i class="el-ksd-icon-add_16"></i>
                            {{$t('filters')}}
                          </el-dropdown-item>
                          <el-dropdown-item @click.native="deleteFG(fgIndex)">
                            <i class="el-icon-ksd-table_delete"></i>
                            {{$t('kylinLang.common.delete')}}
                          </el-dropdown-item>
                        </el-dropdown-menu>
                      </el-dropdown>
                    <!-- getFilterFilters 搜索后的filters, getlimitFilters 是搜索后length 大于3时要收拢filterGroup -->
                    <ul v-if="getFilterFilters(fg.filters).length" :key="fg.isExpand">
                      <div v-for="(row, key) in getlimitFilters(fg)" :key="key" >
                        <li class="row-list">
                          <el-row>
                            <el-col :span="isEdit ? 23 : 24">
                              <span>{{row.column_name}}</span><span v-if="row.in_items.length"> IN </span><span v-if="row.in_items.length" class="row-values">({{row.in_items.toString()}})</span><span v-if="row.like_items.length">&nbsp;LIKE </span><span class="row-values" v-if="row.like_items.length">({{row.like_items.toString()}})</span>
                            </el-col>
                            <el-col :span="1" class="ky-no-br-space btn-icons" v-if="isEdit">
                              <!-- <i class="el-icon-ksd-table_edit ksd-fs-16" @click="editRowAccess(key, row)"></i>
                              <i class="el-icon-ksd-table_delete ksd-fs-16 ksd-ml-10" @click="deleteRowAccess(key, row)"></i> -->
                              <el-dropdown size="small" v-if="isEdit">
                                <span class="el-dropdown-link">
                                  <i class="el-icon-ksd-table_others"></i>
                                </span>
                                <el-dropdown-menu slot="dropdown">
                                  <el-dropdown-item @click.native="editRowAccess(fgIndex, key, row)">{{$t('kylinLang.common.edit')}}</el-dropdown-item>
                                  <el-dropdown-item @click.native="deleteRowAccess(fgIndex, key, row)">{{$t('kylinLang.common.delete')}}</el-dropdown-item>
                                </el-dropdown-menu>
                              </el-dropdown>
                            </el-col>
                          </el-row>
                        </li>
                        <div v-if="key !== getlimitFilters(fg).length - 1" class="join-type-label">{{fg.type}}</div>
                      </div>
                    </ul>
                    <div class="center ksd-mt-10" v-if="getFilterFilters(fg.filters).length > 3">
                      <el-button type="primary" size="small" @click="toggleExpandFG(fg)" text>
                        {{fg.isExpand ? $t('collapse') : $t('expandAll')}} {{fg.isExpand ? '' : `(${fg.filters.length})`}}
                      </el-button>
                    </div>
                    <el-button type="primary" size="small" v-if="fg.is_group&&isEdit" icon="el-ksd-icon-add_16" @click="addRowAccess(fgIndex)" text>{{$t('filters')}}</el-button>
                  </div>
                </div>
                <div v-if="fgIndex !== getSearchFilterdGroup(row_filter.filter_groups).length - 1" class="join-type-label">{{row_filter.type}}</div>
              </div>
              <el-dropdown size="small" class="ksd-ml-10 ksd-mb-10" v-if="isEdit&&row_filter&&getSearchFilterdGroup(row_filter.filter_groups).length">
                <span class="el-dropdown-link">
                  <el-button type="primary" size="small" :disabled="!isCurrentTableChecked" icon="el-ksd-icon-add_16" text>{{$t('add')}}</el-button>
                </span>
                <el-dropdown-menu slot="dropdown">
                  <el-dropdown-item @click.native="addRowAccess(-1)">{{$t('filters')}}</el-dropdown-item>
                  <el-dropdown-item @click.native="addFilterGroups">{{$t('filterGroups')}}</el-dropdown-item>
                </el-dropdown-menu>
              </el-dropdown>
              <kylin-nodata :content="emptyText3" v-if="isCurrentTableChecked&&row_filter&&!getSearchFilterdGroup(row_filter.filter_groups).length&&row_filter.filter_groups.length">
              </kylin-nodata>
              <div class="view-all-tips" v-if="isCurrentTableChecked&&row_filter&&!row_filter.filter_groups.length">
                <div><i class="point">•</i> {{$t('viewAllDataTips')}}</div>
                <div><i class="point">•</i> {{$t('viewAllDataTips1')}}</div>
                <div class="add-rows-btns">
                  <el-dropdown size="small" v-if="isEdit">
                    <span class="el-dropdown-link">
                      <el-button type="primary" size="small" :disabled="!isCurrentTableChecked" icon="el-ksd-icon-add_16" text>{{$t('add')}}</el-button>
                    </span>
                    <el-dropdown-menu slot="dropdown">
                      <el-dropdown-item @click.native="addRowAccess(-1)">{{$t('filters')}}</el-dropdown-item>
                      <el-dropdown-item @click.native="addFilterGroups">{{$t('filterGroups')}}</el-dropdown-item>
                    </el-dropdown-menu>
                  </el-dropdown>
                </div>
              </div>
            </div>
          </div>
        </el-col>
      </el-row>
      <div class="expand-footer ky-no-br-space ksd-right" v-if="isEdit">
        <el-button plain size="small" @click="cancelAccess">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" :disabled="disabledSubmitBtn" size="small" class="ksd-ml-10" :loading="submitLoading" @click="submitAccess">{{$t('kylinLang.common.submit')}}</el-button>
      </div>
      <el-dialog :title="rowAuthorTitle" width="960px" class="author_dialog" :close-on-press-escape="false" :close-on-click-modal="false" :visible.sync="rowAccessVisible" @close="resetRowAccess">
        <div v-if="filterTotalLength+newFiltersLenth>maxFilterAndFilterValues || overedRowValueFilters.length>0">
          <el-alert class="ksd-mb-10" type="error" show-icon :closable="false">
            <span slot="title">{{$t('overFilterMaxTips')}}
              <a class="a-like" @click="showErrorDetails = !showErrorDetails">{{$t('details')}}
                <i :class="[showErrorDetails ? 'el-icon-ksd-more_01-copy' : 'el-icon-ksd-more_02', 'arrow']"></i>
              </a>
            </span>
          </el-alert>
          <div class="ksd-mtb-10 detail-content" v-if="showErrorDetails">
            <div v-if="filterTotalLength+newFiltersLenth>maxFilterAndFilterValues">{{$t('filterTotal')}}{{filterTotalLength+newFiltersLenth}}/{{maxFilterAndFilterValues}}</div>
            <div v-if="overedRowValueFilters.length>0">
              {{$t('filterValuesTotal')}}{{overedRowValueFilters.toString()}}
            </div>
          </div>
        </div>
        <div class="like-tips-block ksd-mb-10">
          <div class="ksd-mb-5">{{$t('tipsTitle')}}<span class="review-details" @click="showDetails = !showDetails">{{$t('viewDetail')}}<i :class="[showDetails ? 'el-icon-ksd-more_01-copy' : 'el-icon-ksd-more_02', 'arrow']"></i></span></div>
          <div class="detail-content" v-if="showDetails">
            <p>{{$t('rules1')}}</p>
            <p>{{$t('rules2')}}</p>
            <p>{{$t('rules3')}}</p>
          </div>
        </div>
        <div v-for="(row, key) in rowLists" :key="key" class="ksd-mb-10">
          <el-select v-model="row.column_name" class="row-column" :placeholder="$t('kylinLang.common.pleaseSelectOrSearch')" filterable :disabled="isRowAuthorEdit" @change="isUnCharColumn(row.column_name, key)">
            <i slot="prefix" class="el-input__icon el-icon-search" v-if="!row.column_name"></i>
            <el-option v-for="c in checkedColumns" :disabled="c.datatype.indexOf('char') === -1 && c.datatype.indexOf('varchar') === -1 && row.joinType === 'LIKE'" :key="c.name" :label="c.name" :value="c.name">
              <el-tooltip :content="c.name" effect="dark" placement="top"><span>{{c.name | omit(30, '...')}}</span></el-tooltip>
              <span class="ky-option-sub-info">{{c.datatype.toLocaleLowerCase()}}</span>
            </el-option>
          </el-select>
          <el-select
            :placeholder="$t('kylinLang.common.pleaseSelect')"
            style="width:75px;"
            class="link-type"
            popper-class="js_like-type"
            :disabled="isRowAuthorEdit"
            v-model="row.joinType">
            <el-option :disabled="row.isNeedDisableLike && key === 'LIKE'" :value="key" v-for="(key, i) in linkKind" :key="i">{{key}}</el-option>
          </el-select>
          <el-select
            v-model="row.items"
            multiple
            filterable
            clearable
            remote
            allow-create
            default-first-option
            :class="{'row-values-edit': isRowAuthorEdit, 'row-values-add': !isRowAuthorEdit}"
            @change="setRowValues(row.items, key)"
            :placeholder="$t('pleaseInput')">
          </el-select>
          <span class="ky-no-br-space ksd-ml-10" v-if="!isRowAuthorEdit">
            <el-button type="primary" icon="el-ksd-icon-add_16" plain circle size="mini" @click="addRow" v-if="key==0"></el-button>
            <el-button icon="el-icon-minus" plain circle size="mini" @click="removeRow(key)"></el-button>
          </span>
        </div>
        <span slot="footer" class="dialog-footer ky-no-br-space">
          <div class="ksd-fleft">
            <el-alert
              :title="$t('filterTips')"
              type="info"
              class="ksd-ptb-5"
              :show-background="false"
              :closable="false"
              show-icon>
            </el-alert>
          </div>
          <el-button plain @click="cancelRowAccess" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
          <el-button type="primary" @click="submitRowAccess" size="medium">{{$t('kylinLang.common.submit')}}</el-button>
        </span>
      </el-dialog>
    </div>
  </template>
  
  <script>
  import Vue from 'vue'
  import { Component } from 'vue-property-decorator'
  import { handleSuccessAsync, indexOfObjWithSomeKey, objectClone, kylinConfirm } from '../../util'
  import { handleSuccess, handleError } from '../../util/business'
  import { mapActions, mapGetters } from 'vuex'
  import { pageSizeMapping, maxFilterAndFilterValues } from '../../config'
  @Component({
    props: ['row', 'projectName'],
    computed: {
      ...mapGetters([
        'isDataPermission'
      ])
    },
    methods: {
      ...mapActions({
        getAccessDetailsByUser: 'GET_ACCESS_DETAILS_BY_USER',
        submitAccessData: 'SUBMIT_ACCESS_DATA',
        getAclPermission: 'GET_ACL_PERMISSION',
        changeProjectUserDataPermission: 'CHANGE_PROJECT_USER_DATA_PERMISSION'
      })
    },
    locales: {
      'en': {
        accessTables: 'Table Access List',
        accessColumns: 'Column Access List',
        accessRows: 'Row Access List',
        searchKey: 'Search by table or column name',
        accessTips: 'All tables in current datasource are accessible.',
        accessColsTips: 'All columns in current table are accessible.',
        accessRowsTips: 'All rows in current table are accessible.',
        viewAllDataTips: 'After the row ACL was set, the user/user group could only access the data that match the specified filters.',
        viewAllDataTips1: 'For the columns without conditions set, user/user group could access all the data.',
        addRowAccess: 'Add Row ACL',
        addRowAccess1: 'Add Row ACL (Table: {tableName})',
        editRowAccess: 'Edit Row ACL (Table: {tableName})',
        pleaseInput: 'Confirm by pressing "enter" key and separate multiple values by comma.',
        loadMore: 'Load More',
        tipsTitle: 'The row ACL provides IN and LIKE operators. The LIKE operator could only be used for char or varchar data type, and needs to be used with wildcards. ',
        viewDetail: 'View Rules',
        details: 'Details',
        rules1: '_ (underscore) wildcard characters, matches any single character. ',
        rules2: '% (percent) wildcard characters, matches with zero or more characters.',
        rules3: '\\ (backslash) escape character. The characters following "\\" won\'t be regarded as any special characters.',
        add: 'Add',
        filters: 'Filter',
        filterGroups: 'Filter Group',
        filterTips: 'The relation between different values ​​of the same filter is "OR"',
        deleteFilterGroupTips: 'Are you sure you want to delete the filter group? All the included filters would be deleted.',
        deleteFilterGroupTitle: 'Delete Filter Group',
        expandAll: 'Expand All',
        collapse: 'Collapse',
        overFilterMaxTips: 'The number of filters or the included values of a single filter exceeds the upper limit. Please modify.',
        filterTotal: 'Total number of filters: ',
        filterValuesTotal: 'The filter(s) including excess values: ',
        dataPermission: 'Data Permission',
        dataPermissionTips: 'Allow users to access data, including viewing sample data and querying with SQL',
        confirmOpen: 'Turn Open',
        confirmOff: 'Turn Off',
        Group: 'User Group',
        User: 'User',
        editACL: 'Manage Table Column, or Row Access',
        openDataPermissionConfirm: 'Do you want to turn ON the data permissions of {type} "{name}"? This {type} will be able to view sample data and query.',
        closeDataPermissionConfirm: 'Are you sure to turn OFF the data permissions of {type} "{name}"? This {type} will not be able to view sample data and query.'
      },
      'zh-cn': {
        accessTables: '表级访问列表',
        accessColumns: '列级访问列表',
        accessRows: '行级访问列表',
        searchKey: '搜索表名或列名',
        accessTips: '当前数据源上所有表均可访问',
        accessColsTips: '当前表上所有列均可访问',
        accessRowsTips: '当前表上所有行均可访问',
        viewAllDataTips: '设置行级权限后，用户/用户组仅能查看到表中符合筛选条件的数据。',
        viewAllDataTips1: '对于没有设置权限的列，用户/用户组仍能够查看该列所有数据。',
        addRowAccess: '添加行级权限',
        addRowAccess1: '添加行级权限（表： {tableName}）',
        editRowAccess: '编辑行级权限（表： {tableName}）',
        pleaseInput: '请用回车进行输入确认并用逗号进行多个值分割',
        loadMore: '加载更多',
        tipsTitle: '行级权限支持 IN 和 LIKE 操作符。其中，LIKE 仅支持 char 和 varchar 类型的列，需配合通配符使用。',
        viewDetail: '查看规则',
        details: '详情',
        rules1: '_（下划线）通配符，匹配任意单个字符。',
        rules2: '%（百分号）通配符，匹配空白字符或任意多个字符。',
        rules3: '\\（反斜杠）转义符，转义符后的通配符或转义符将不被识别为特殊字符。',
        add: '添加',
        filters: '过滤器',
        filterGroups: '过滤组',
        filterTips: '同一过滤器的不同值的关系为 “或”（OR）',
        deleteFilterGroupTips: '确定要删除过滤组吗？过滤组中的过滤器会被一并删除。',
        deleteFilterGroupTitle: '删除过滤组',
        expandAll: '展开全部',
        collapse: '收起',
        overFilterMaxTips: '过滤器包含的值或过滤器总数超过上限，请修改。',
        filterTotal: '过滤器总数：',
        filterValuesTotal: '包含值超额的过滤器：',
        dataPermission: '数据权限',
        dataPermissionTips: '允许用户进行数据访问，包括查看样例数据和 SQL 查询',
        confirmOpen: '开启',
        confirmOff: '关闭',
        Group: '用户组',
        User: '用户',
        editACL: '编辑表列行级访问权限',
        openDataPermissionConfirm: '确定要打开{type} “{name}” 的数据权限吗？这个{type}将能查看样例数据和查询。',
        closeDataPermissionConfirm: '确定要关闭{type} “{name}” 的数据权限吗？这个{type}将不能查看样例数据和查询。'
      }
    }
  })
  export default class UserAccess extends Vue {
    maxFilterAndFilterValues = maxFilterAndFilterValues
    defaultProps = {
      children: 'children',
      label: 'label'
    }
    tables = []
    filterOriginDatas = []
    isEdit = false
    isRerender = true
    isRowAuthorEdit = false
    rowAccessVisible = false
    tableFilter = ''
    columnFilter = ''
    rowSearch = ''
    columns = []
    filterCols = []
    rows = []
    rowLists = [{column_name: '', joinType: 'IN', items: []}]
    row_filter = {
      type: 'AND',
      filter_groups: []
    }
    linkKind = ['IN', 'LIKE']
    isSelectTable = false
    tableAuthorizedNum = 0
    totalNum = 0
    defaultCheckedKeys = []
    defaultExpandedKeys = ['0']
    catchDefaultExpandedKeys = ['0']
    allTables = []
    copyOriginTables = []
    databaseIndex = -1
    tableIndex = -1
    editFilterGroupIndex = -1
    editRowIndex = -1
    currentTable = ''
    isAllTablesAccess = false
    isAllColAccess = false
    colAuthorizedNum = 0
    submitLoading = false
    isCurrentTableChecked = false
    selectAllColumns = false
    currentTableId = ''
    loading = false
    columnPageSize = 100
    columnCurrentPage = 1
    isAuthority = false
    showDetails = false
    showErrorDetails = false
    isAddFilterForGroup = false
    filterTotalLength = 0
    newFiltersLenth = 0
    overedRowValueFilters = []
    ext_permissions = !!this.row.ext_permissions && this.row.ext_permissions.length > 0
    get emptyText () {
      return this.tableFilter ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
    }
    get emptyText2 () {
      return this.columnFilter ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
    }
    get emptyText3 () {
      return this.rowSearch ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
    }
    get disabledSubmitBtn () {
      return JSON.stringify(this.copyOriginTables) === JSON.stringify(this.allTables)
    }
    get currentProjectId () {
      return this.$route.query.projectId
    }
    showLoading () {
      this.loading = true
    }
    hideLoading () {
      this.loading = false
    }
    async handleChangePermission (val) {
      try {
        const option = { type: this.$t(this.row.type), name: this.row.role_or_name }
        const msg = val ? this.$t('openDataPermissionConfirm', option) : this.$t('closeDataPermissionConfirm', option)
        const confirmBtnText = val ? this.$t('confirmOpen') : this.$t('confirmOff')
        await kylinConfirm(msg, {confirmButtonText: confirmBtnText, centerButton: true}, this.$t('dataPermission'))
        const reqsdata = { access_entry_id: this.row.id, permissions: val ? ['DATA_QUERY'] : [], principal: this.row.type === 'User', sid: this.row.role_or_name }
        const res = await this.changeProjectUserDataPermission({data: reqsdata, projectId: this.currentProjectId})
        const { data } = await handleSuccessAsync(res)
        this.ext_permissions = data
        this.$emit('reload')
      } catch (e) {
        handleError(e)
      }
    }
    pushTableId (data) {
      const index = this.catchDefaultExpandedKeys.indexOf(data.id)
      if (index === -1) {
        this.catchDefaultExpandedKeys.push(data.id)
      }
    }
    removeTableId (data) {
      const index = this.catchDefaultExpandedKeys.indexOf(data.id)
      if (index !== -1) {
        this.catchDefaultExpandedKeys.splice(index, 1)
      }
    }
    handleNodeClick (data, node) {
      if (!data.children && !data.isMore) { // tables data 中‘非加载更多’的node
        this.isSelectTable = true
        this.currentTable = data.database + '.' + data.label
        this.isCurrentTableChecked = data.authorized
        this.currentTableId = data.id
        this.$refs.tableTree && this.$refs.tableTree.setCurrentKey(this.currentTableId)
        const indexs = data.id.split('_')
        this.databaseIndex = indexs[0]
        this.tableIndex = indexs[1]
        this.initColsAndRows(data.columns, data.row_filter, data.totalColNum)
      } else if (!data.children && data.isMore) {
        node.parent.data.currentIndex++
        const renderNums = node.parent.data.currentIndex * pageSizeMapping.TABLE_TREE
        const renderMoreTables = node.parent.data.originTables.slice(0, renderNums)
        node.parent.data.children = []
        node.parent.data.children = renderMoreTables
        if (renderNums < node.parent.data.originTables.length) {
          renderMoreTables.push(data)
        }
        this.reRenderTree(true)
      }
    }
    initColsAndRows (columns, row_filter, totalColNum) {
      this.columnCurrentPage = 1
      this.colAuthorizedNum = 0
      this.columns = columns.map((col) => {
        if (col.authorized) {
          this.colAuthorizedNum++
        }
        return {name: col.column_name, authorized: col.authorized, datatype: col.datatype}
      })
      this.isAllColAccess = this.colAuthorizedNum === totalColNum
      this.selectAllColumns = this.isAllColAccess
      this.row_filter = objectClone(row_filter)
    }
    getColumns (type) {
      let columns = this.columns
      if (type === 'LIKE') {
        columns = columns.filter((c) => {
          return c.datatype.indexOf('char') !== -1 || c.datatype.indexOf('varchar') !== -1
        })
      }
      return columns
    }
    isUnCharColumn (columnName, key) {
      if (columnName) {
        const index = indexOfObjWithSomeKey(this.columns, 'name', columnName)
        let datatype = ''
        if (index !== -1) {
          datatype = this.columns[index].datatype
        }
        const isNeedDisableLike = datatype.indexOf('char') === -1 && datatype.indexOf('varchar') === -1
        this.$set(this.rowLists[key], 'isNeedDisableLike', isNeedDisableLike)
      } else {
        this.$set(this.rowLists[key], 'isNeedDisableLike', false)
      }
    }
    get checkedColumns () {
      return this.columns.filter(c => c.authorized)
    }
    get pagedFilterColumns () {
      const filterCols = objectClone(this.columns)
      this.filterCols = filterCols.filter((col) => {
        return col.name.toLowerCase().indexOf(this.columnFilter.trim().toLowerCase()) !== -1
      })
      return this.filterCols.slice(0, this.columnCurrentPage * this.columnPageSize)
    }
    loadMoreCols () {
      this.columnCurrentPage++
    }
    checkAllTables (val) {
      this.showLoading()
      setTimeout(() => {
        for (let i = this.tables.length - 1; i >= 0; i--) {
          this.tables[i].originTables.forEach((d) => {
            if (!d.isMore) {
              this.handleTableData(d, val)
              this.setCurrentTable(d, val)
            }
          })
        }
        this.setCurrentTable(this.tables[0].children[0], val)
        this.reRenderTree()
        this.hideLoading()
      }, 500)
    }
    checkAllColumns (val) {
      this.colAuthorizedNum = 0
      this.columns.forEach((col) => {
        col.authorized = val
        if (col.authorized) {
          this.colAuthorizedNum++
        }
      })
      const columns = this.allTables[this.databaseIndex].tables[this.tableIndex].columns
      columns.forEach((col) => {
        col.authorized = val
      })
      this.tables[this.databaseIndex].originTables[this.tableIndex].columns = columns
    }
    handleTableData (data, isChecked) {
      const indexs = data.id.split('_')
      this.allTables[indexs[0]].tables[indexs[1]].authorized = isChecked
      let database = objectClone(this.tables[indexs[0]])
      let defaultCheckedKeys = objectClone(this.defaultCheckedKeys)
      if (isChecked && !data.authorized) {
        this.tableAuthorizedNum++
        database.authorizedNum++
        database.label = database.databaseName + ` (${database.authorizedNum}/${database.totalNum})`
        defaultCheckedKeys.push(data.id)
        if (database.authorizedNum && database.authorizedNum === database.totalNum) {
          defaultCheckedKeys.push(database.id)
        }
      } else if (!isChecked && data.authorized) {
        this.tableAuthorizedNum--
        database.authorizedNum--
        database.label = database.databaseName + ` (${database.authorizedNum}/${database.totalNum})`
        const removeKeyIndex = defaultCheckedKeys.indexOf(data.id)
        defaultCheckedKeys.splice(removeKeyIndex, 1)
        const removeDatabaseKeyIndex = defaultCheckedKeys.indexOf(database.id)
        if (removeDatabaseKeyIndex !== -1) {
          defaultCheckedKeys.splice(removeDatabaseKeyIndex, 1)
        }
      }
      data.authorized = isChecked
      data.columns.forEach((col) => {
        col.authorized = isChecked
      })
      database.originTables[indexs[1]] = data
      this.tables[indexs[0]] = database
      this.defaultCheckedKeys = defaultCheckedKeys
      this.isAllTablesAccess = this.tableAuthorizedNum === this.totalNum
    }
    setCurrentTable (data, isChecked) {
      const indexs = data.id.split('_')
      this.databaseIndex = indexs[0]
      this.tableIndex = indexs[1]
      this.currentTable = data.database + '.' + data.label
      this.currentTableId = data.id
      this.isCurrentTableChecked = isChecked
      this.selectAllColumns = isChecked
      this.allTables[indexs[0]].tables[indexs[1]].columns.forEach((col) => {
        col.authorized = isChecked
      })
      this.allTables[indexs[0]].tables[indexs[1]].row_filter = { type: 'AND', filter_groups: [] }
      this.initColsAndRows(this.allTables[indexs[0]].tables[indexs[1]].columns, this.allTables[indexs[0]].tables[indexs[1]].row_filter, data.totalColNum)
    }
    checkChange (data, checkNode, node) {
      this.showLoading()
      setTimeout(() => {
        const isChecked = node.checked
        if (!data.children && !data.isMore) { // tables data 中‘非加载更多’的node
          this.handleTableData(data, isChecked)
          this.setCurrentTable(data, isChecked)
          this.reRenderTree()
        } else if (data.children && data.children.length) {
          data.originTables.forEach((d) => {
            if (!d.isMore) {
              this.handleTableData(d, isChecked)
              this.setCurrentTable(d, isChecked)
            }
          })
          this.setCurrentTable(data.children[0], isChecked)
          this.reRenderTree()
        }
        this.hideLoading()
      }, 100)
    }
    reRenderTree (isLoadMoreRender) { // isLoadMoreRender 为 true 时，不重置数据
      this.isRerender = false
      if (!isLoadMoreRender) {
        this.tables = [...this.tables]
      }
      this.defaultExpandedKeys = objectClone(this.catchDefaultExpandedKeys)
      this.$nextTick(() => {
        this.isRerender = true
        this.handleLoadMoreStyle()
        setTimeout(() => {
          this.$refs.tableTree.setCurrentKey(this.currentTableId)
        })
      })
    }
    get rowAuthorTitle () {
      return !this.isRowAuthorEdit ? this.$t('addRowAccess1', {tableName: this.currentTable}) : this.$t('editRowAccess', {tableName: this.currentTable})
    }
    handleLoadMoreStyle () {
      this.$nextTick(() => {
        const loadMore = this.$el.querySelectorAll('.acl-tree .load-more') || this.$el.getElementsByClassName('.acl-tree .load-more')
        const indeterminateNodes = this.$el.querySelectorAll('.acl-tree .indeterminate-node')
        if (loadMore.length) {
          Array.prototype.forEach.call(loadMore, (m) => {
            const targetCheckbox = m.parentNode.parentNode.querySelector('.el-checkbox')
            if (targetCheckbox) {
              targetCheckbox.style.display = 'none'
            }
          })
        }
        if (indeterminateNodes.length) {
          Array.prototype.forEach.call(indeterminateNodes, (n) => {
            const indeterminateCheckbox = n.parentNode.parentNode.querySelector('.el-checkbox .el-checkbox__input')
            if (indeterminateCheckbox) {
              indeterminateCheckbox.className = 'el-checkbox__input is-indeterminate'
            }
          })
        }
      })
    }
    get filterTableData () {
      let filterOriginDatas = objectClone(this.tables)
      filterOriginDatas = filterOriginDatas.filter((data) => {
        const originFilterTables = data.originTables.filter((t) => {
          return t.label.toLowerCase().indexOf(this.tableFilter.trim().toLowerCase()) !== -1
        })
        const pagedFilterTables = originFilterTables.slice(0, pageSizeMapping.TABLE_TREE * data.currentIndex)
        if (pageSizeMapping.TABLE_TREE < originFilterTables.length) {
          pagedFilterTables.push({
            id: data.id + '_more',
            label: this.$t('loadMore'),
            class: 'load-more ksd-fs-12',
            isMore: true
          })
        }
        data.children = pagedFilterTables
        return pagedFilterTables.length > 0
      })
      this.defaultExpandedKeys = objectClone(this.catchDefaultExpandedKeys)
      this.$nextTick(() => {
        this.handleLoadMoreStyle()
        if (this.$refs.tableTree) {
          this.$refs.tableTree.setCurrentKey(this.currentTableId)
        }
      })
      return filterOriginDatas
    }
    isShowRow (row) {
      let isShow = false
      if (row.column_name.toLowerCase().indexOf(this.rowSearch.trim().toLowerCase()) !== -1) {
        isShow = true
      }
      for (let i = 0; i < row.in_items.length; i++) {
        if (row.in_items[i].toLowerCase().indexOf(this.rowSearch.trim().toLowerCase()) !== -1) {
          isShow = true
          break
        }
      }
      for (let k = 0; k < row.like_items.length; k++) {
        if (row.like_items[k].toLowerCase().indexOf(this.rowSearch.trim().toLowerCase()) !== -1) {
          isShow = true
          break
        }
      }
      return isShow
    }
    editAccess () {
      this.isEdit = true
      this.loadAccessDetails(false)
    }
    cancelAccess () {
      this.isEdit = false
      this.loadAccessDetails(true)
    }
    submitAccess () {
      this.submitLoading = true
      this.submitAccessData({projectName: this.projectName, userType: this.row.type, roleOrName: this.row.role_or_name, accessData: this.allTables}).then((res) => {
        handleSuccess(res, () => {
          this.$message({
            type: 'success',
            message: this.$t('kylinLang.common.submitSuccess')
          })
          this.submitLoading = false
          this.isEdit = false
          this.loadAccessDetails(true)
        })
      }, (res) => {
        handleError(res)
        this.submitLoading = false
      })
    }
    selectColumn (val, col) {
      let columns = this.allTables[this.databaseIndex].tables[this.tableIndex].columns
      const index = indexOfObjWithSomeKey(columns, 'column_name', col)
      columns[index].authorized = val
      this.tables[this.databaseIndex].originTables[this.tableIndex].columns = columns
      this.columns[index].authorized = val
      if (val) {
        this.colAuthorizedNum++
      } else {
        this.colAuthorizedNum--
      }
      this.selectAllColumns = this.colAuthorizedNum === this.columns.length
    }
    setRowValues (values, key) {
      let formatValues = []
      values.forEach((v) => {
        const val = v.trim().split(/[,，]/g)
        formatValues = [...formatValues, ...val]
      })
      this.rowLists[key].items = formatValues.filter(it => !!it)
    }
    addRowAccess (fgIndex) {
      if (fgIndex !== -1) {
        this.editFilterGroupIndex = fgIndex
        this.isAddFilterForGroup = true // 在指定筛选组里添加筛选器
      } else {
        this.editFilterGroupIndex = fgIndex
        this.isAddFilterForGroup = false
      }
      this.filterTotalLength = 0
      this.overedRowValueFilters = []
      this.isRowAuthorEdit = false
      this.rowAccessVisible = true
    }
    addFilterGroups () {
      this.row_filter.filter_groups.push({is_group: true, type: 'AND', filters: []})
    }
    getSearchFilterdGroup (filterGroups) { // 有搜索关键时，如果没有符合搜索的过滤器内容，去掉空的过滤组
      if (this.rowSearch) {
        return filterGroups.filter(fg => {
          return this.getFilterFilters(fg.filters).length > 0
        })
      } else {
        return filterGroups
      }
    }
    getFilterFilters (filters) {
      return filters.filter((f) => {
        return this.isShowRow(f)
      })
    }
    getlimitFilters (fg) {
      const filterFilters = this.getFilterFilters(fg.filters)
      if (filterFilters.length <= 3 || (fg.isExpand && filterFilters.length > 3)) {
        return filterFilters
      } else {
        return filterFilters.slice(0, 3)
      }
    }
    toggleExpandFG (fg) {
      this.$set(fg, 'isExpand', !fg.isExpand)
    }
    changeRowFilterType () {
      this.allTables[this.databaseIndex].tables[this.tableIndex].row_filter.type = this.row_filter.type
      this.tables[this.databaseIndex].originTables[this.tableIndex].row_filter.type = this.row_filter.type
    }
    changeFilterGroupTpye (fgIndex) {
      this.allTables[this.databaseIndex].tables[this.tableIndex].row_filter.filter_groups[fgIndex].type = this.row_filter.filter_groups[fgIndex].type
      this.tables[this.databaseIndex].originTables[this.tableIndex].row_filter.filter_groups[fgIndex].type = this.row_filter.filter_groups[fgIndex].type
    }
    editRowAccess (fgIndex, index, row) {
      this.isRowAuthorEdit = true
      this.editFilterGroupIndex = fgIndex
      this.editRowIndex = index
      this.rowLists = []
      if (row.in_items.length > 0) {
        this.rowLists.push({column_name: row.column_name, joinType: 'IN', items: row.in_items})
      }
      if (row.like_items.length > 0) {
        this.rowLists.push({column_name: row.column_name, joinType: 'LIKE', items: row.like_items})
      }
      this.filterTotalLength = 0
      this.overedRowValueFilters = []
      this.rowAccessVisible = true
    }
    deleteRowAccess (fgIndex, index, row) {
      let idx = index
      this.row_filter.filter_groups[fgIndex].filters.splice(idx, 1)
      this.allTables[this.databaseIndex].tables[this.tableIndex].row_filter.filter_groups[fgIndex].filters.splice(idx, 1)
      this.tables[this.databaseIndex].originTables[this.tableIndex].row_filter.filter_groups[fgIndex].filters.splice(idx, 1)
      if (!this.row_filter.filter_groups[fgIndex].filters.length) {
        if (!this.row_filter.filter_groups[fgIndex].is_group) {
          this.row_filter.filter_groups.splice(fgIndex, 1)
        }
        this.allTables[this.databaseIndex].tables[this.tableIndex].row_filter.filter_groups.splice(fgIndex, 1)
        this.tables[this.databaseIndex].originTables[this.tableIndex].row_filter.filter_groups.splice(fgIndex, 1)
      }
    }
    async deleteFG (fgIndex) {
      await kylinConfirm(this.$t('deleteFilterGroupTips'), {confirmButtonText: this.$t('kylinLang.common.delete'), centerButton: true}, this.$t('deleteFilterGroupTitle'))
      this.row_filter.filter_groups.splice(fgIndex, 1)
      this.allTables[this.databaseIndex].tables[this.tableIndex].row_filter.filter_groups.splice(fgIndex, 1)
      this.tables[this.databaseIndex].originTables[this.tableIndex].row_filter.filter_groups.splice(fgIndex, 1)
    }
    cancelRowAccess () {
      this.rowAccessVisible = false
      this.filterTotalLength = 0
      this.overedRowValueFilters = []
    }
    fromRowArrToObj (rowsList, key) {
      var len = rowsList && rowsList.length || 0
      var obj = {}
      for (var k = 0; k < len; k++) {
        if (rowsList[k].items.length) {
          obj[rowsList[k][key]] = obj[rowsList[k][key]] || []
          obj[rowsList[k][key]] = [...obj[rowsList[k][key]], ...rowsList[k].items]
        }
      }
      return obj
    }
    checkFilterValues (filters) {
      const overedRowValueFilters = []
      filters.forEach(f => {
        let copyRowList = objectClone(this.rowLists)
        let index = indexOfObjWithSomeKey(copyRowList, 'column_name', f.column_name)
        let newLenth = 0
        while (index !== -1) {
          newLenth = newLenth + copyRowList[index].items.length
          copyRowList.splice(index, 1)
          index = indexOfObjWithSomeKey(copyRowList, 'column_name', f.column_name)
        }
        if (f.in_items.length + f.like_items.length + newLenth > maxFilterAndFilterValues) { // 单个 filter 中的值的数量最多支持 100 个 （包含LIKE）
          overedRowValueFilters.push(f.column_name + `(${f.in_items.length + f.like_items.length + newLenth}/${maxFilterAndFilterValues})`)
        }
      })
      return overedRowValueFilters
    }
    checkMaxRowAccess () {
      this.filterTotalLength = 0 // filter 总数不超过 100 个 （包含 group 中的 filter）
      this.newFiltersLenth = 0
      this.overedRowValueFilters = []
      if (!this.isRowAuthorEdit) { // 新添加过滤器的入口
        if (!this.isAddFilterForGroup) {
          for (let fgKey in this.row_filter.filter_groups) {
            const fg = this.row_filter.filter_groups[fgKey]
            if (fg.is_group) continue // 添加外层过滤器，只在外层过滤器里比对列名值是否超过max
            this.overedRowValueFilters = this.checkFilterValues(fg.filters)
          }
        } else {
          const fg = this.row_filter.filter_groups[this.editFilterGroupIndex]
          this.overedRowValueFilters = this.checkFilterValues(fg.filters)
        }
        if (!this.overedRowValueFilters.length) { // overedRowValueFilters 为空说明已存在的filter没有超过max的值
          this.row_filter.filter_groups.forEach(fg => {
            this.filterTotalLength = this.filterTotalLength + fg.filters.length
          })
          const rowsObject = this.fromRowArrToObj(this.rowLists, 'column_name')
          this.newFiltersLenth = Object.keys(rowsObject).length
          for (let key in rowsObject) {
            if (rowsObject[key].length > maxFilterAndFilterValues) { // 新增的单条过滤值超过max
              this.overedRowValueFilters.push(key + `(${rowsObject[key].length}/${maxFilterAndFilterValues})`)
            }
          }
        }
        return (this.filterTotalLength + this.newFiltersLenth > maxFilterAndFilterValues) || this.overedRowValueFilters.length > 0
      } else { // 编辑是同一列的in 和 like 值已合并
        const rowsObject = this.fromRowArrToObj(this.rowLists, 'column_name')
        for (let key in rowsObject) {
          if (rowsObject[key].length > maxFilterAndFilterValues) { // 新增的单条过滤值超过max
            this.overedRowValueFilters.push(key + `(${rowsObject[key].length}/${maxFilterAndFilterValues})`)
          }
        }
        return this.overedRowValueFilters.length > 0
      }
    }
    submitRowAccess () {
      if (this.checkMaxRowAccess()) return
      if (!this.isRowAuthorEdit) {
        this.rowLists.forEach((row) => {
          if (row.column_name && row.items.length) {
            if (this.isAddFilterForGroup) { // 只在指定筛选组里合并相同维度
              const filters = this.row_filter.filter_groups[this.editFilterGroupIndex].filters
              const index = indexOfObjWithSomeKey(filters, 'column_name', row.column_name)
              if (index !== -1) {
                if (row.joinType === 'IN') {
                  filters[index].in_items = [...filters[index].in_items, ...row.items]
                } else if (row.joinType === 'LIKE') {
                  filters[index].like_items = [...filters[index].like_items, ...row.items]
                }
              } else {
                const rowObj = {column_name: row.column_name, in_items: row.joinType === 'IN' ? row.items : [], like_items: row.joinType === 'LIKE' ? row.items : []}
                filters.push(rowObj)
              }
            } else { // 在筛选组外的所有筛选器合并相同维度
              let isExistedColumn = false
              for (let fg in this.row_filter.filter_groups) {
                if (this.row_filter.filter_groups[fg].is_group) continue
                const index = indexOfObjWithSomeKey(this.row_filter.filter_groups[fg].filters, 'column_name', row.column_name)
                if (index !== -1) {
                  if (row.joinType === 'IN') {
                    this.row_filter.filter_groups[fg].filters[index].in_items = [...this.row_filter.filter_groups[fg].filters[index].in_items, ...row.items]
                  } else if (row.joinType === 'LIKE') {
                    this.row_filter.filter_groups[fg].filters[index].like_items = [...this.row_filter.filter_groups[fg].filters[index].like_items, ...row.items]
                  }
                  isExistedColumn = true
                  break
                }
              }
              if (!isExistedColumn) { // 新增的列，需要新增一个filterGroups, is_group 为false
                const rowObj = {column_name: row.column_name, in_items: row.joinType === 'IN' ? row.items : [], like_items: row.joinType === 'LIKE' ? row.items : []}
                this.row_filter.filter_groups.push({is_group: false, type: 'AND', filters: [rowObj]})
              }
            }
          }
        })
      } else {
        this.rowLists.forEach((r) => {
          if (r.joinType === 'IN') {
            this.row_filter.filter_groups[this.editFilterGroupIndex].filters[this.editRowIndex].in_items = r.items
          } else if (r.joinType === 'LIKE') {
            this.row_filter.filter_groups[this.editFilterGroupIndex].filters[this.editRowIndex].like_items = r.items
          }
        })
        if (this.row_filter.filter_groups[this.editFilterGroupIndex].filters[this.editRowIndex].in_items.length === 0 && this.row_filter.filter_groups[this.editFilterGroupIndex].filters[this.editRowIndex].like_items.length === 0) {
          this.row_filter.filter_groups[this.editFilterGroupIndex].filters.splice(this.editRowIndex, 1)
          if (!this.row_filter.filter_groups[this.editFilterGroupIndex].filters.length) {
            if (!this.row_filter.filter_groups[this.editFilterGroupIndex].is_group) {
              this.row_filter.filter_groups.splice(this.editFilterGroupIndex, 1)
            }
            this.allTables[this.databaseIndex].tables[this.tableIndex].row_filter.filter_groups.splice(this.editFilterGroupIndex, 1)
            this.tables[this.databaseIndex].originTables[this.tableIndex].row_filter.filter_groups.splice(this.editFilterGroupIndex, 1)
          }
        }
      }
      this.allTables[this.databaseIndex].tables[this.tableIndex].row_filter = objectClone(this.row_filter)
      this.tables[this.databaseIndex].originTables[this.tableIndex].row_filter = objectClone(this.row_filter)
      this.rowAccessVisible = false
    }
    addRow () {
      this.rowLists.unshift({column_name: '', joinType: 'IN', items: []})
    }
    removeRow (index) {
      this.rowLists.splice(index, 1)
    }
    resetRowAccess () {
      this.showErrorDetails = false
      this.showDetails = false
      this.rowLists = [{column_name: '', joinType: 'IN', items: []}]
    }
    async loadAccessDetails (authorizedOnly) {
      this.defaultCheckedKeys = []
      this.allTables = []
      this.tables = []
      this.rows = []
      this.columns = []
      const response = await this.getAccessDetailsByUser({data: {authorized_only: authorizedOnly, project: this.projectName}, roleOrName: this.row.role_or_name, type: this.row.type, projectName: this.projectName})
      const result = await handleSuccessAsync(response)
      if (result.length) {
        this.allTables = objectClone(result)
        this.copyOriginTables = objectClone(result)
        this.tableAuthorizedNum = 0
        this.totalNum = 0
        this.currentTableId = ''
        this.tables = result.map((database, key) => {
          this.tableAuthorizedNum = this.tableAuthorizedNum + database.authorized_table_num
          this.totalNum = this.totalNum + database.total_table_num
          const labelNum = this.authorizedOnly ? ` (${database.authorized_table_num})` : ` (${database.authorized_table_num}/${database.total_table_num})`
          const originTableDatas = database.tables.map((t, i) => {
            const id = key + '_' + i
            if (t.authorized && !authorizedOnly) {
              this.defaultCheckedKeys.push(id)
            }
            if (database.database_name + '.' + t.table_name === this.currentTable) {
              this.currentTableId = id
            }
            return {id: id, label: t.table_name, database: database.database_name, authorized: t.authorized, columns: t.columns, row_filter: t.row_filter, totalColNum: t.total_column_num}
          })
          const pegedTableDatas = originTableDatas.slice(0, pageSizeMapping.TABLE_TREE)
          if (pageSizeMapping.TABLE_TREE < originTableDatas.length) {
            pegedTableDatas.push({
              id: key + '_more',
              label: this.$t('loadMore'),
              class: 'load-more ksd-fs-12',
              isMore: true
            })
          }
          if (database.authorized_table_num && database.authorized_table_num === database.total_table_num) {
            this.defaultCheckedKeys.push(key + '')
          }
          return {
            id: key + '',
            label: database.database_name + labelNum,
            class: database.authorized_table_num && database.authorized_table_num < database.total_table_num ? 'indeterminate-node' : '',
            databaseName: database.database_name,
            authorizedNum: database.authorized_table_num,
            totalNum: database.total_table_num,
            originTables: originTableDatas,
            currentIndex: 1,
            children: pegedTableDatas
          }
        })
        this.isAllTablesAccess = this.tableAuthorizedNum === this.totalNum
        this.$nextTick(() => {
          if (this.currentTableId) {
            const indexs = this.currentTableId.split('_')
            if (indexs[1] <= pageSizeMapping.TABLE_TREE) {
              this.handleNodeClick(this.tables[indexs[0]].children[indexs[1]])
            } else {
              this.handleNodeClick(this.tables[0].children[0])
            }
          } else {
            this.handleNodeClick(this.tables[0].children[0])
          }
          this.handleLoadMoreStyle()
        })
      }
    }
    created () {
      this.loadAccessDetails(true)
      this.getAclPermission({project: this.projectName}).then((res) => {
        const { data } = res.data
        handleSuccess(res, () => {
          this.isAuthority = data
        })
      }, (err) => {
        handleError(err)
      })
    }
  }
  </script>
  
  <style lang="less">
    @import '../../assets/styles/variables.less';
    .data-permission {
      .flex {
        display: flex;
        align-items: center;
      }
      .el-icon-ksd-info {
        color: @text-placeholder-color;
      }
    }
    .access-content {
      .join-type {
        width: 70px;
      }
      .row-list {
        .el-col {
          display: block !important;
          word-break: break-all;
          line-height: 1.4;
          padding: 5px;
        }
      }
    }
    .row-column {
      width: 210px;
    }
    .row-values-edit {
      width: calc(~'100% - 295px');
    }
    .row-values-add {
      width: calc(~'100% - 365px');
    }
    .row-values-edit,
    .row-values-add {
      .el-select__input {
        margin-left: 12px;
      }
      .el-select__tags > span {
        max-width: 100%;
        .el-tag {
          max-width: 100%;
          position: relative;
          padding-right: 16px;
          white-space: normal;
          word-break: break-word;
          .el-select__tags-text {
            max-width: 100%;
            overflow: hidden;
            text-overflow: ellipsis;
            display: inline-block;
          }
          .el-tag__close {
            position: absolute;
            right: 1px;
            // top: 5px;
          }
        }
      }
    }
    .author_dialog {
      .el-dialog {
        position: absolute;
        left: 0;
        right: 0;
        margin: auto;
        max-height: 70%;
        overflow: hidden;
        display: flex;
        flex-direction: column;
        .el-dialog__header {
          min-height: 47px;
        }
        .el-dialog__body {
          overflow: auto;
        }
      }
    }
    .a-like {
      color: @base-color;
      position: relative;
      .arrow {
        transform: rotate(90deg) scale(0.8);
        margin-left: 3px;
        font-size: 7px;
        color: @base-color;
        position: absolute;
        top: 2px;
      }
    }
    .error-msg {
      color: @text-normal-color;
      word-break: break-all;
    }
    .like-tips-block {
      color: @text-normal-color;
      .review-details {
        color: @base-color;
        cursor: pointer;
        position: relative;
      }
      .el-icon-ksd-more_01-copy {
        transform: scale(0.8);
      }
      .arrow {
        transform: rotate(90deg) scale(0.8);
        margin-left: 3px;
        font-size: 7px;
        color: @base-color;
        position: absolute;
        top: 4px;
      }
    }
    .author_dialog {
      .detail-content {
        background-color: @base-background-color-1;
        padding: 10px 15px;
        box-sizing: border-box;
        font-size: 12px;
        color: @text-normal-color;
      }
    }
  </style>
