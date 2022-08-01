<template>
  <div class="security-user" v-loading="isLoadingUsers">
    <div class="ksd-title-label ksd-mt-20">{{$t('userList')}}</div>
    <!-- 新建/过滤用户 -->
    <el-row class="ksd-mt-10 ksd-mb-10">
      <el-col :span="24">
        <el-button type="primary"
          size="medium"
          icon="el-ksd-icon-back_old"
          v-if="currentGroup"
          @click="$router.push('/admin/group')">
          {{$t('back')}}
        </el-button><el-button
          type="primary"
          size="medium"
          icon="el-ksd-icon-add_22"
          v-if="userActions.includes('addUser')"
          :disabled="!isTestingSecurityProfile"
          @click="editUser('new')">
          {{$t('user')}}
        </el-button>
        <div style="width:240px;" class="ksd-fright">
          <el-input class="show-search-btn"
            size="medium"
            v-model="filterName"
            v-global-key-event.enter.debounce="inputFilter"
            @clear="inputFilter('')"
            prefix-icon="el-ksd-icon-search_22"
            :placeholder="$t('userName')"
          >
          </el-input>
        </div>
      </el-col>
    </el-row>

    <el-alert class="ksd-mb-16" type="info"
      v-if="!isTestingSecurityProfile"
      :title="$t('securityProfileTip')"
      :closable="false">
    </el-alert>

    <el-table :data="usersList" :empty-text="emptyText" class="user-table" v-scroll-shadow>
      <!-- 表：username列 -->
      <el-table-column :label="$t('user')" prop="username" :width="220" show-overflow-tooltip>
        <template slot-scope="scope">
          <i class="el-icon-ksd-table_admin ksd-fs-14" style="cursor: default;"></i>
          <span class="user-name-col">{{scope.row.username}}</span>
        </template>
      </el-table-column>
      <!-- 表：group列 -->
      <el-table-column :label="$t('kylinLang.common.group')">
        <template slot-scope="scope">
          <common-tip :content="scope.row.groups && scope.row.groups.join('<br/>')" placement="top">
              <span>{{scope.row.groups && scope.row.groups.join(',')}}</span>
          </common-tip>
        </template>
      </el-table-column>
      <!-- 表：是否系统管理员列 -->
      <el-table-column :label="$t('admin')" align="center" :width="120">
        <template slot-scope="scope">
          <i class="el-icon-ksd-good_health admin-svg" v-if="scope.row.admin"></i>
        </template>
      </el-table-column>
      <!-- 表：status列 -->
      <el-table-column :label="$t('status')" :width="120">
        <template slot-scope="scope">
          <el-tag size="small" type="info" v-if="scope.row.disabled">DISABLED</el-tag>
          <el-tag size="small" type="success" v-else>ENABLED</el-tag>
        </template>
      </el-table-column>
      <!-- 表：action列 -->
      <el-table-column v-if="isActionShow" :label="$t('action')" :width="87">
        <template slot-scope="scope">
          <el-tooltip :content="$t('resetPassword')" effect="dark" placement="top">
            <i class="el-icon-ksd-table_reset_password ksd-fs-14 ksd-mr-10" :class="{'is-disabled': !isTestingSecurityProfile}" v-if="userActions.includes('changePassword') || scope.row.uuid === currentUser.uuid" @click="editUser(scope.row.uuid === currentUser.uuid ? 'password' : 'resetUserPassword', scope.row)"></i>
          </el-tooltip><span>
          </span><el-tooltip :content="$t('groupMembership')" effect="dark" placement="top">
            <i class="el-icon-ksd-table_group ksd-fs-14 ksd-mr-10" :class="{'is-disabled': !isTestingSecurityProfile}" v-if="userActions.includes('assignGroup')" @click="editUser('group', scope.row)"></i>
          </el-tooltip><span>
          </span><common-tip :content="$t('kylinLang.common.moreActions')" v-if="isMoreActionShow"><el-dropdown trigger="click">
            <i class="el-icon-ksd-table_others" :class="{'is-disabled': !isTestingSecurityProfile}"></i>
            <el-dropdown-menu slot="dropdown">
              <el-dropdown-item :disabled="!isTestingSecurityProfile" v-if="userActions.includes('editUser')&&scope.row.uuid !== currentUser.uuid" @click.native="editUser('edit', scope.row)">{{$t('editRole')}}</el-dropdown-item>
              <el-dropdown-item :disabled="!isTestingSecurityProfile" v-if="userActions.includes('deleteUser')" @click.native="dropUser(scope.row)">{{$t('drop')}}</el-dropdown-item>
              <el-dropdown-item :disabled="!isTestingSecurityProfile" v-if="userActions.includes('disableUser') && scope.row.disabled" @click.native="changeStatus(scope.row)">{{$t('enable')}}</el-dropdown-item>
              <el-dropdown-item :disabled="!isTestingSecurityProfile" v-if="userActions.includes('disableUser') && !scope.row.disabled" @click.native="changeStatus(scope.row)">{{$t('disable')}}</el-dropdown-item>
            </el-dropdown-menu>
          </el-dropdown>
          </common-tip>
        </template>
      </el-table-column>
    </el-table>

    <kylin-pager
      class="ksd-center ksd-mtb-16" ref="pager"
      :refTag="pageRefTags.userPager"
      :totalSize="totalSize"
      :perPageSize="20"
      :curPage="pagination.page_offset+1"
      @handleCurrentChange="handleCurrentChange">
    </kylin-pager>
  </div>
</template>

<script>
import Vue from 'vue'
import { mapState, mapGetters, mapActions } from 'vuex'
import { Component } from 'vue-property-decorator'

import locales from './locales'
import { pageRefTags, bigPageCount } from 'config'
import { handleError, kylinConfirm } from '../../../util'

@Component({
  computed: {
    ...mapState({
      currentUser: (state) => state.user.currentUser
    }),
    ...mapGetters([
      'userActions',
      'currentSelectedProject',
      'isTestingSecurityProfile'
    ])
  },
  methods: {
    ...mapActions({
      removeUser: 'REMOVE_USER',
      loadUsersList: 'LOAD_USERS_LIST',
      loadUserListByGroupName: 'GET_USERS_BY_GROUPNAME',
      updateStatus: 'UPDATE_STATUS'
    }),
    ...mapActions('UserEditModal', {
      callUserEditModal: 'CALL_MODAL'
    })
  },
  locales,
  beforeRouteEnter: (to, from, next) => {
    if (from.name === 'GroupDetail') {
      // 进入user页面清除filter，重刷列表
      next(vm => {
        vm.filterName = ''
        vm.loadUsers()
      })
    } else {
      next()
    }
  }
})
export default class SecurityUser extends Vue {
  pageRefTags = pageRefTags
  userData = []
  totalSize = 0
  filterTimer = null
  filterName = ''
  pagination = {
    page_size: +localStorage.getItem(this.pageRefTags.userPager) || bigPageCount,
    page_offset: 0
  }
  isLoadingUsers = false
  get currentGroup () {
    const current = this.$store.state.user.usersGroupList.filter((g) => {
      return g.group_name === this.$route.params.groupName
    })
    return current.length ? current[0] : null
  }
  get isActionShow () {
    return this.userActions.filter(action => ['addUser'].includes(action)).length
  }
  get isMoreActionShow () {
    return this.userActions.filter(action => ['resetPassword', 'addUser'].includes(action)).length
  }
  get usersList () {
    return this.userData.map(user => ({
      username: user.username,
      disabled: user.disabled,
      admin: user.authorities.some(role => role.authority === 'ROLE_ADMIN'),
      modeler: user.authorities.some(role => role.authority === 'ROLE_MODELER'),
      analyst: user.authorities.some(role => role.authority === 'ROLE_ANALYST'),
      default_password: user.default_password,
      authorities: user.authorities,
      groups: user.authorities.map(role => role.authority),
      uuid: user.uuid
    }))
  }

  inputFilter (value) {
    this.pagination.page_offset = 0
    this.filterName = value
    this.loadUsers(value)
  }

  get emptyText () {
    return this.filterName ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }

  handleCurrentChange (pager, pageSize) {
    this.pagination.page_offset = pager
    this.pagination.page_size = pageSize
    this.loadUsers(this.filterName)
  }

  async loadUsers (name) {
    this.isLoadingUsers = true
    try {
      const parameter = {
        ...this.pagination,
        // project: this.currentSelectedProject, // 处理资源组时，发现这个接口不用传 project 参数
        name: name || '',
        group_uuid: this.currentGroup && this.currentGroup.uuid
      }
      const res = !this.currentGroup
        ? await this.loadUsersList(parameter)
        : await this.loadUserListByGroupName(parameter)

      // this.userData = res.data.data && (res.data.data.users || res.data.data.groupMembers) || []
      this.userData = res.data.data && res.data.data.value || []
      this.totalSize = res.data.data && res.data.data.total_size || 0
      this.isLoadingUsers = false
      if (res.status !== 200) {
        handleError(res)
        this.isLoadingUsers = false
      }
    } catch (e) {
      handleError(e)
      this.isLoadingUsers = false
    }
  }

  async editUser (editType, userDetail) {
    if (!this.isTestingSecurityProfile) return
    const isSubmit = await this.callUserEditModal({ editType, userDetail })
    isSubmit && this.loadUsers(this.filterName)
  }

  async dropUser (userDetail) {
    if (!this.isTestingSecurityProfile) return
    try {
      const { uuid, username } = userDetail

      await kylinConfirm(this.$t('cofirmDelUser', {userName: username}), {confirmButtonText: this.$t('kylinLang.common.delete')}, this.$t('delUserTitle'))
      await this.removeUser(uuid)

      this.loadUsers()
      this.$message({ type: 'success', message: this.$t('kylinLang.common.delSuccess') })
    } catch (e) {
      e !== 'cancel' && handleError(e)
    }
  }

  async changeStatus (userDetail) {
    if (!this.isTestingSecurityProfile) return
    const status = userDetail.disabled ? this.$t('enable') : this.$t('disable')
    await kylinConfirm(this.$t('changeUserTips', {status: status.toLowerCase(), userName: userDetail.username}), {cancelButtonText: this.$t('kylinLang.common.cancel'), confirmButtonText: status, type: 'warning'})
    try {
      await this.updateStatus({
        uuid: userDetail.uuid,
        username: userDetail.username,
        disabled: !userDetail.disabled
      })
      this.loadUsers()
    } catch (e) {
      handleError(e)
    }
  }

  mounted () {
    this.loadUsers()
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.security-user {
  padding: 0 20px;
  .el-icon-ksd-good_health {
    color: @color-success;
    cursor: default;
  }
  .user-table {
    .el-icon-ksd-table_reset_password:hover,
    .el-icon-ksd-table_group:hover,
    .el-icon-ksd-table_others:hover {
      color: @base-color;
    }
    .user-name-col {
      white-space: pre;
    }
    .is-disabled {
      cursor: not-allowed;
      color: @text-disabled-color;
      &:hover {
        cursor: not-allowed;
        color: @text-disabled-color;
      }
    }
  }
}
</style>
