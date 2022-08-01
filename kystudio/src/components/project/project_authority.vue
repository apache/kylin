<template>
  <div id="projectAuth">
    <div class="ksd-title-label titleBox">
      <el-button
        type="primary"
        text
        icon-button-mini
        icon="el-ksd-icon-arrow_left_16"
        size="small"
        @click="$router.push('/admin/project')"></el-button>
      <span>{{$t('projectTitle', {projectName: currentProject})}}</span>
    </div>
    <el-row class="ksd-mt-10 ksd-mb-10">
      <el-col :span="24">
        <div class="ksd-fleft ky-no-br-space">
          <el-button type="primary" icon="el-ksd-icon-add_22" v-if="projectActions.includes('accessActions')" @click="authorUser()">{{$t('userAccess')}}</el-button>
        </div>
        <div style="width:240px;" class="ksd-fright">
          <el-input class="show-search-btn"
            size="medium"
            v-model="serarchChar"
            :placeholder="$t('userNameOrGroup')"
            v-global-key-event.enter.debounce="inputFilter"
            @clear="inputFilter"
          >
            <i slot="prefix" class="el-input__icon" :class="{'el-icon-search': !searchLoading, 'el-icon-loading': searchLoading}"></i>
          </el-input>
        </div>
      </el-col>
    </el-row>
    <div>
      <el-table :data="userAccessList" :empty-text="emptyText" class="user-access-table" key="user">
        <el-table-column :label="$t('userOrGroup')" prop="role_or_name" class-name="role-name-cell" show-overflow-tooltip>
          <template slot-scope="props">
            <i :class="{'el-icon-ksd-table_admin': props.row.type === 'User', 'el-icon-ksd-table_group': props.row.type === 'Group'}"></i>
            <span>{{props.row.role_or_name}}</span>
          </template>
        </el-table-column>
        <el-table-column :label="$t('type')" prop="type">
          <template slot-scope="props">{{$t(props.row.type)}}</template>
        </el-table-column>
        <el-table-column :label="$t('accessType')" prop="promission">
          <template slot-scope="props">{{$t(props.row.promission)}}</template>
        </el-table-column>
        <el-table-column :label="$t('kylinLang.common.action')" :width="87">
          <template slot-scope="scope">
            <el-tooltip :content="$t('kylinLang.common.edit')" effect="dark" placement="top">
              <i class="el-ksd-icon-edit_16 ksd-mr-10 ksd-fs-16" @click="editAuthorUser(scope.row)"></i>
            </el-tooltip><span>
            </span><el-tooltip :content="$t('kylinLang.common.delete')" effect="dark" placement="top">
              <i class="el-ksd-icon-table_delete_16 ksd-fs-16" @click="removeAccess(scope.row.id, scope.row.role_or_name, scope.row.promission, !scope.row.sid.grantedAuthority)"></i>
            </el-tooltip>
          </template>
        </el-table-column>
      </el-table>
      <kylin-pager
        class="ksd-center ksd-mtb-10" ref="pager"
        :refTag="pageRefTags.authorityUserPager"
        :totalSize="totalSize"
        :curPage="pagination.page_offset+1"
        :perPageSize="pagination.page_size"
        @handleCurrentChange="handleCurrentChange">
      </kylin-pager>
    </div>

    <el-dialog :title="authorTitle" width="960px" class="user-access-dialog" :close-on-press-escape="false" :close-on-click-modal="false" :visible.sync="authorizationVisible" @close="initAccessData">
      <div class="content-container">
        <div class="author-tips">
          <div class="item-point">{{$t('authorTips')}}</div>
          <div class="item-point">{{$t('authorTips1')}}</div>
          <div class="item-point" v-html="$t('authorTips2')"></div>
        </div>
        <div class="ksd-title-label-small">{{$t('selectUserAccess')}}</div>
        <div v-for="(accessMeta, index) in accessMetas" :key="index" class="user-group-select ksd-mt-10 ky-no-br-space">
          <el-select placeholder="Type" v-model="accessMeta.principal" :disabled="isEditAuthor" @change="changeUserType(index)" size="medium" class="user-select" :popper-class="'js_principal' + (index + 1)">
            <el-option :label="$t('users')" :value="true"></el-option>
            <el-option :label="$t('userGroups')" :value="false"></el-option>
          </el-select>
          <el-select
            :class="['name-select', 'name-select' + (index + 1), 'author-select', {'has-selected': !accessMeta.sids.length}]"
            v-model="accessMeta.sids"
            :disabled="isEditAuthor"
            multiple
            filterable
            remote
            @blur="(e) => filterUser(e.target.value)"
            :placeholder="$t('kylinLang.common.pleaseInputUserName')"
            :remote-method="filterUser"
            :popper-class="'js_author-select' + (index + 1)"
            v-if="accessMeta.principal">
            <i slot="prefix" class="el-input__icon el-icon-search" v-if="!accessMeta.sids.length"></i>
            <el-option
              v-for="item in renderUserList"
              :key="item.value"
              :label="item.label"
              :value="item.value">
            </el-option>
            <div class="over-limit-tip" v-if="showLimitTips(accessMeta.principal)">{{$t('overLimitTip')}}</div>
          </el-select>
          <el-select
            :class="['name-select', 'name-select' + (index + 1), {'has-selected': !accessMeta.sids.length}]"
            v-model="accessMeta.sids"
            :disabled="isEditAuthor"
            multiple
            filterable
            remote
            @blur="(e) => filterGroup(e.target.value)"
            :placeholder="$t('kylinLang.common.pleaseInputUserGroup')"
            :remote-method="filterGroup"
            :popper-class="'js_author-select' + (index + 1)"
            v-else>
            <i slot="prefix" class="el-input__icon el-icon-search" v-if="!accessMeta.sids.length"></i>
            <el-option
              v-for="item in renderGroupList"
              :key="item.value"
              :label="item.label"
              :value="item.value">
            </el-option>
            <div class="over-limit-tip" v-if="showLimitTips(accessMeta.principal)">{{$t('overLimitTip')}}</div>
          </el-select>
          <el-select :class="['type-select', 'type-select' + (index + 1)]" :placeholder="$t('access')" v-model="accessMeta.permission" size="medium" :popper-class="'js_access_type_sel' + (index + 1)">
            <el-option :label="$t(item.key)" :value="item.value" :key="item.value" v-for="item in showMaskByOrder"></el-option>
          </el-select>
          <span class="ky-no-br-space ksd-ml-10 repeatBtn" v-if="!isEditAuthor">
            <el-button type="primary" icon="el-icon-ksd-add_2" class="ksd-mr-5" plain circle size="mini" @click="addAccessMetas" v-if="index==0"></el-button>
            <el-button icon="el-icon-minus" class="minus" plain circle size="mini" :disabled="index==0&&accessMetas.length==1" @click="removeAccessMetas(index)"></el-button>
          </span>
        </div>
      </div>
      <span slot="footer" class="dialog-footer ky-no-br-space">
        <el-button plain @click="cancelAuthor" size="medium">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" :disabled="disabledSubmit" @click="submitAuthor" :loading="submitLoading" size="medium">{{$t('kylinLang.common.submit')}}</el-button>
      </span>
    </el-dialog>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { objectClone } from '../../util'
import { handleSuccess, handleError, kylinConfirm, hasRole, hasPermissionOfProjectAccess } from '../../util/business'
import { mapActions, mapGetters } from 'vuex'
import { permissions, pageRefTags, pageCount } from 'config'
@Component({
  methods: {
    ...mapActions({
      getUserAndGroups: 'GET_USER_AND_GROUPS',
      getProjectAccess: 'GET_PROJECT_ACCESS',
      delProjectAccess: 'DEL_PROJECT_ACCESS',
      getAvailableUserOrGroupList: 'ACCESS_AVAILABLE_USER_OR_GROUP',
      saveProjectAccess: 'SAVE_PROJECT_ACCESS',
      editProjectAccess: 'EDIT_PROJECT_ACCESS',
      getUserAccessByProject: 'USER_ACCESS',
      resetProjectState: 'RESET_PROJECT_STATE',
      saveTabs: 'SET_QUERY_TABS',
      loginOut: 'LOGIN_OUT'
    })
  },
  computed: {
    ...mapGetters([
      'projectActions'
    ])
  },
  locales: {
    'en': {
      projectTitle: 'Authorization (Project: {projectName})',
      back: 'Back',
      userAccess: 'User/Group',
      selectUserAccess: 'Select User/Group',
      userNameOrGroup: 'Search by user/user group',
      toggleTableView: 'Swtich to Table Operation',
      toggleUserView: 'Swtich to User Operation',
      userOrGroup: 'Name',
      type: 'Type',
      accessType: 'Role',
      accessTables: 'Access Tables',
      author: 'Add User/User Group',
      editAuthor: 'Edit User/User Group',
      overLimitTip: 'Only the first 100 results would be displayed. Please use another keyword to refine the search results.',
      selectUserOrUserGroups: 'Please enter or select the username/user group name',
      userGroups: 'User Group',
      users: 'User',
      Group: 'User Group',
      User: 'User',
      Query: 'Query',
      Admin: 'Admin',
      Management: 'Management',
      Operation: 'Operation',
      tableName: 'Table Name',
      datasourceType: 'Data Source',
      tableType: 'Table',
      deleteAccessTip: 'Are you sure you want to delete the authorization of "{userName}" in this project?',
      access: 'Role',
      deleteAccessTitle: 'Delete Authorization',
      authorTips: 'Can\'t add system admin to the list, as this role already has full access to all projects.',
      authorTips1: 'By default, the added user/user group would be granted full access to all the tables in the project.',
      authorTips2: `What roles does Kylin provide?<br>
      The relationship of each role is: Admin > Management > Operation > Query. For example, Admin includes all the permissions of the other three roles. Management includes all the permissions of Operation and Query. Operation includes all the permissions of Query.<br>
      1. Query: For business analyst who would need permissions to query tables or indexes.<br>
      2. Operation: For the operator who need permissions to build indexes and monitor job status.<br>
      3. Management: For the model designer who would need permissions to load tables and design models.<br>
      4. Admin: For the project admin who would need all permissions and could manage and maintain this project, including loading tables, authorizing user access permissions, etc.`,
      noAuthorityTip: 'Access denied. Please try again after logging in.'
    }
  }
})
export default class ProjectAuthority extends Vue {
  pageRefTags = pageRefTags
  userTimer = null
  groupTimer = null
  accessView = 'user'
  userAccessList = []
  tableAccessList = [{name: 'Table A', type: 'Hive', users: []}]
  totalSize = 1
  tableTotalSize = 1
  serarchChar = ''
  searchLoading = false
  pagination = {
    page_size: +localStorage.getItem(this.pageRefTags.authorityUserPager) || pageCount,
    page_offset: 0
  }
  authorizationVisible = false
  authorForm = {name: [], editName: '', role: 'Admin'}
  isEditAuthor = false
  authorOptions = [{
    label: this.$t('userGroups'),
    options: []
  }, {
    label: this.$t('users'),
    options: []
  }]
  showMask = {
    1: 'Query',
    16: 'Admin',
    32: 'Management',
    64: 'Operation'
  }
  mask = {
    1: 'READ',
    16: 'ADMINISTRATION',
    32: 'MANAGEMENT',
    64: 'OPERATION'
  }
  accessMetas = [{permission: 1, principal: true, sids: []}]
  originAccessMetas = [{permission: 1, principal: true, sids: []}]
  userList = []
  groupList = []
  userTotalSize = 0
  groupTotalSize = 0
  showMaskByOrder = [
    { key: 'Query', value: 1 },
    { key: 'Operation', value: 64 },
    { key: 'Management', value: 32 },
    { key: 'Admin', value: 16 }
  ]
  submitLoading = false
  projectAccess = null
  get emptyText () {
    return this.serarchChar ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }
  get hasProjectAdminPermission () {
    return hasPermissionOfProjectAccess(this, this.projectAccess, permissions.ADMINISTRATION.mask)
  }
  get isAdmin () {
    return hasRole(this, 'ROLE_ADMIN')
  }
  get currentProject () {
    return this.$route.params.projectName
  }
  get authorTitle () {
    return this.isEditAuthor ? this.$t('editAuthor') : this.$t('author')
  }
  get currentProjectId () {
    return this.$route.query.projectId
  }
  get renderUserList () {
    var result = this.userList.filter((user) => {
      let isSelected = false
      for (let i = 0; i < this.accessMetas.length; i++) {
        if (this.accessMetas[i].principal && this.accessMetas[i].sids.indexOf(user) !== -1) {
          isSelected = true
          break
        }
      }
      return !isSelected
    })
    result = result.map((u) => {
      return {label: u, value: u}
    })
    return result
  }
  get renderGroupList () {
    var result = this.groupList.filter((user) => {
      let isSelected = false
      for (let i = 0; i < this.accessMetas.length; i++) {
        if (!this.accessMetas[i].principal && this.accessMetas[i].sids.indexOf(user) !== -1) {
          isSelected = true
          break
        }
      }
      return !isSelected
    })
    result = result.map((u) => {
      return {label: u, value: u}
    })
    return result
  }
  get disabledSubmit () {
    if (this.isEditAuthor) {
      if (this.accessMetas[0].permission !== this.originAccessMetas[0].permission) {
        return false
      } else {
        return true
      }
    } else {
      let flag = true
      this.accessMetas.forEach((item) => {
        if (item.sids.length > 0) {
          flag = false
        }
      })
      return flag
    }
  }
  showLimitTips (val) {
    return val ? this.userTotalSize > 100 : this.groupTotalSize > 100
  }
  inputFilter () {
    this.searchLoading = true
    this.pagination.page_offset = 0
    this.loadAccess().then(() => {
      this.searchLoading = false
    }, () => {
      this.searchLoading = false
    })
  }
  handleCurrentChange (pager, pageSize) {
    this.pagination.page_offset = pager
    this.pagination.page_size = pageSize
    this.loadAccess()
  }
  removeAccess (id, username, promission, principal) {
    kylinConfirm(this.$t('deleteAccessTip', {userName: username}), null, this.$t('deleteAccessTitle')).then(() => {
      this.delProjectAccess({id: this.currentProjectId, aid: id, userName: username, principal: principal}).then((res) => {
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.delSuccess')
        })

        const { data } = res.data
        if (typeof data === 'boolean' && !data) {
          this.noAuthorityModal()
          return
        }
        this.initAccessData()
        this.loadAccess()
        this.reloadAvaliableUserAndGroup()
      }, (res) => {
        handleError(res)
      })
    })
  }
  // 无权限登出二次确认弹窗
  noAuthorityModal () {
    kylinConfirm(this.$t('noAuthorityTip'), { showClose: false, showCancelButton: false, type: 'warning' }).then(() => {
      const resetDataAndLoginOut = () => {
        localStorage.setItem('buyit', false)
        // reset 所有的project信息
        this.resetProjectState()
        this.saveTabs({tabs: null})
        this.$router.push({name: 'Login', params: { ignoreIntercept: true }})
      }
      this.loginOut().then(() => {
        resetDataAndLoginOut()
      }).catch(() => {
        resetDataAndLoginOut()
      })
    })
  }
  toggleView (view) {
    this.accessView = view
  }
  loadUserOrGroup (filterUserName, type) {
    var para = {data: {page_size: 100, page_offset: 0, project: this.projectName}}
    if (filterUserName) {
      para.data.name = filterUserName
    }
    para.uuid = this.currentProjectId
    para.type = type
    return this.getAvailableUserOrGroupList(para)
  }
  addAccessMetas () {
    this.accessMetas.unshift({permission: 1, principal: true, sids: []})
  }
  removeAccessMetas (index) {
    this.accessMetas.splice(index, 1)
  }
  filterUser (filterUserName) {
    window.clearTimeout(this.userTimer)
    this.userTimer = setTimeout(() => {
      this.loadUserOrGroup(filterUserName, 'user').then((res) => {
        handleSuccess(res, (data) => {
          this.userList = data.value
          this.userTotalSize = data.total_size || 0
        })
      }, (res) => {
        handleError(res)
      })
    }, 500)
  }
  filterGroup (filterUserName) {
    window.clearTimeout(this.groupTimer)
    this.groupTimer = setTimeout(() => {
      this.loadUserOrGroup(filterUserName, 'group').then((res) => {
        handleSuccess(res, (data) => {
          this.groupList = data.value
          this.groupTotalSize = data.total_size || 0
        })
      }, (res) => {
        handleError(res)
      })
    }, 500)
  }
  changeUserType (index) {
    if (this.accessMetas[index].sids.length && !this.isEditAuthor) {
      this.accessMetas[index].sids = []
    }
  }
  authorUser () {
    this.isEditAuthor = false
    this.authorizationVisible = true
  }
  cancelAuthor () {
    this.authorizationVisible = false
  }
  editAuthorUser (row) {
    this.isEditAuthor = true
    this.authorizationVisible = true
    const sids = row.sid.grantedAuthority ? [row.sid.grantedAuthority] : [row.sid.principal]
    this.accessMetas = [{permission: row.permission.mask, principal: row.type === 'User', sids: sids, access_entry_id: row.id}]
    this.originAccessMetas = [{permission: row.permission.mask, principal: row.type === 'User', sids: sids, access_entry_id: row.id}]
  }
  submitAuthor () {
    const accessMetas = objectClone(this.accessMetas)
    accessMetas.filter((acc) => {
      return acc.sids.length && acc.permission
    }).forEach((access) => {
      access.permission = this.mask[access.permission]
    })
    this.submitLoading = true
    let accessData = null
    if (this.isEditAuthor) {
      accessData = accessMetas[0]
      accessData.sid = accessData.sids[0]
      delete accessData.sids
    } else {
      accessData = accessMetas
    }
    const actionType = this.isEditAuthor ? 'editProjectAccess' : 'saveProjectAccess'
    this[actionType]({accessData: accessData, id: this.currentProjectId}).then((res) => {
      handleSuccess(res, (data) => {
        this.submitLoading = false
        this.authorizationVisible = false
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.saveSuccess')
        })
        if (actionType === 'editProjectAccess' && typeof data === 'boolean' && !data) {
          this.noAuthorityModal()
          return
        }
        this.initAccessData()
        this.loadAccess()
        !this.isEditAuthor && this.reloadAvaliableUserAndGroup()
      })
    }, (res) => {
      handleError(res)
      this.submitLoading = false
      this.authorizationVisible = false
    })
  }
  initAccessData () {
    this.accessMetas = [{permission: 1, principal: true, sids: []}]
  }
  reloadAvaliableUserAndGroup () {
    this.filterUser()
    this.filterGroup()
  }
  loadAccess () {
    const para = {
      data: this.pagination,
      project_id: this.currentProjectId
    }
    para.data.name = this.serarchChar
    return this.getProjectAccess(para).then((res) => {
      handleSuccess(res, (data) => {
        this.userAccessList = data.value
        this.totalSize = data.total_size
        this.settleAccessList = this.userAccessList && this.userAccessList.map((access) => {
          access.role_or_name = access.sid.grantedAuthority || access.sid.principal
          access.type = access.sid.principal ? 'User' : 'Group'
          access.promission = this.showMask[access.permission.mask]
          access.accessDetails = []
          return access
        }) || []
      })
    }, (res) => {
      handleError(res)
    })
  }
  created () {
    this.loadAccess()
    this.getUserAccessByProject({
      project: this.currentProject,
      not_cache: true
    }).then((res) => {
      handleSuccess(res, (data) => {
        this.projectAccess = data
        if (this.hasProjectAdminPermission || this.isAdmin) {
          this.reloadAvaliableUserAndGroup()
        }
      })
    }, (res) => {
      handleError(res)
    })
  }
}
</script>
<style lang="less">
  @import '../../assets/styles/variables.less';
  #projectAuth {
    padding: 20px;
    .titleBox{
      display: flex;
      align-items: center;
    }
    .user-access-table {
      .el-icon-ksd-table_edit,
      .el-icon-ksd-table_delete {
        &:hover {
          color: @base-color;
        }
      }
      .role-name-cell {
        .cell {
          white-space: pre;
        }
      }
    }
  }
  .author-select {
    width: 570px;
    .el-select-dropdown__item {
      white-space: pre-wrap;
    }
  }
  .user-access-dialog {
    .el-dialog {
      position: absolute;
      left: 0;
      right: 0;
      margin: auto;
      max-height: 70%;
      overflow: hidden;
      display: flex;
      flex-direction: column;
      .el-dialog__body {
        overflow: auto;
        .content-container{
          overflow:overlay;
        }
      }
    }
    .author-tips {
      position: relative;
      padding: 0 10px;
      margin-bottom: 15px;
      font-size: 14px;
      .item-point {
        position: relative;
        &::before {
          content: ' ';
          width: 3px;
          height: 3px;
          border-radius: 100%;
          background: @000;
          color: @000;
          position: absolute;
          top: 8px;
          left: -10px;
        }
      }
    }
    .user-group-select {
      .user-select {
        width: 128px;
      }
      .name-select {
        width: 550px;
        margin-left: 5px;
        .el-select__tags-text {
          max-width: 480px;
          overflow: hidden;
          text-overflow: ellipsis;
          float: left;
        }
      }
      .type-select {
        width: 150px;
        margin-left: 5px;
      }
      .name-select.has-selected {
        .el-select__tags {
          input {
            margin-left: 30px;
          }
        }
      }
      .repeatBtn{
        .minus{
          margin-left:0;
        }
      }
    }
  }
  .over-limit-tip {
    height: 32px;
    color: @text-normal-color;
    line-height: 32px;
    text-align: center;
    font-size: 12px;
  }
  .user-access-block,
  .table-access-block {
    .access-card {
      border: 1px solid @line-border-color;
      background-color: @fff;
      height: 370px;
      .access-title {
        background-color: @background-disabled-color;
        border-bottom: 1px solid @line-border-color;
        height: 36px;
        color: @text-title-color;
        font-size: 14px;
        font-weight: bold;
        padding: 7px 10px;
        .el-checkbox__label {
          color: @text-title-color;
          font-weight: bold;
        }
      }
      .access-search {
        height: 32px;
        padding: 3px 10px;
        border-bottom: 1px solid @line-border-color;
      }
      .access-tips {
        height: 24px;
        line-height: 24px;
        color: @text-title-color;
        background-color: @background-disabled-color;
        padding: 0 10px;
        i {
          color: @text-disabled-color;
        }
      }
      .access-content {
        height: 264px;
        overflow-y: auto;
        position: relative;
        &.all-tips {
          height: 240px;
        }
        &.tree-content {
          height: 300px;
          &.all-tips {
            height: 276px;
          }
        }
        .view-all-tips {
          margin: 0 auto;
          margin-top: 80px;
          font-size: 12px;
          color: @text-title-color;
          width: 70%;
          .add-rows-btns {
            width: 100%;
            text-align: center;
          }
        }
        ul {
          overflow-y: auto;
          li {
            height: 30px;
            padding: 4px 10px;
            box-sizing: border-box;
            &.row-list {
              display: table;
              width: 100%;
              // border-bottom: 1px solid @line-border-color;
              span {
                word-break: break-all;
                &:first-child {
                  word-break: keep-all;
                }
                &:nth-child(3) {
                  line-height: 1.5;
                  max-height: 55px;
                  overflow: auto;
                  padding-top: 5px;
                }
                &.row-values {
                  color: @text-disabled-color;
                  font-size: 12px;
                }
              }
            }
            &:hover {
              background-color: @base-color-9;
            }
          }
        }
        .filter-groups {
          margin-bottom: 10px;
          &:last-child {
            margin-bottom: 0;
          }
          &.is-group {
            margin: 10px;
            position: relative;
            .group-action-btn {
              position: absolute;
              top: 10px;
              right: 10px;
            }
            .filter-group-block {
              padding: 10px;
              background-color: @base-background-color-1;
              border: 1px solid @base-background-color-1;
              &:hover {
                border: 1px solid @base-color-2;
              }
              .row-list {
                padding: 0px;
              }
              .join-type-label {
                margin: 10px 0;
              }
            }
          }
        }
        .join-type-label {
          font-size: 12px;
          color: @text-normal-color;
          background-color: @background-disabled-color;
          width: 36px;
          height: 20px;
          line-height: 20px;
          padding: 0 7px;
          border-radius: 2px;
          margin: 10px;
          position: relative;
          &::before,
          &::after {
            content: '';
            border-left: 2px solid @background-disabled-color;
            height: 10px;
            position: absolute;
            left: 50%;
          }
          &::before {
            top: -10px;
          }
          &::after {
            top: 20px;
          }
        }
      }
      &.column-card,
      &.row-card {
        margin-left: -1px;
        margin-top: 36px;
        height: 334px;
      }
      &.column-card {
        .el-checkbox__input.is-checked+.el-checkbox__label {
          color: @text-title-color;
        }
        .list-load-more {
          height: 30px;
          line-height: 30px;
          text-align: center;
          font-size: 12px;
          cursor: pointer;
          &:hover {
            color: @base-color;
            background-color: @base-color-9;
          }
        }
      }
      &.row-card {
        .access-content {
          // padding: 10px;
          ul li {
            .el-row {
              position: relative;
            }
            .el-col-23 {
              width: calc(~'100% - 22px');
              display: flex;
              line-height: 1.5;
              padding: 5px 0;
            }
            .el-col-1 {
              width: 22px;
              height: 18px;
              position: absolute;
              right: 0;
            }
            .btn-icons {
              text-align: right;
            }
          }
        }
      }
    }
    .expand-footer {
      border-top: 1px solid @line-border-color;
      margin: 15px -15px 0 -15px;
      padding: 10px 15px 0;
    }
  }
</style>
