<template>
  <div class="security-group" v-loading="isLoadingUserGroups">
    <div class="ksd-title-label ksd-mt-20 ksd-mrl-20">{{$t('userGroupsList')}}</div>
    <el-row class="ksd-mb-10 ksd-mt-10 ksd-mrl-20">
      <el-button type="primary" size="medium" v-if="groupActions.includes('addGroup')" :disabled="!isTestingSecurityProfile" icon="el-ksd-icon-add_22" @click="editGroup('new')">{{$t('userGroup')}}</el-button>
      <div style="width:240px;" class="ksd-fright">
        <el-input class="show-search-btn"
          size="medium"
          prefix-icon="el-ksd-icon-search_22"
          :placeholder="$t('groupFilter')"
          v-global-key-event.enter.debounce="inputFilter"
          @clear="inputFilter('')"
        >
        </el-input>
      </div>
    </el-row>
    <el-alert class="ksd-mb-16 ksd-ml-20" type="info"
      v-if="!isTestingSecurityProfile"
      :title="$t('securityProfileTip')"
      :closable="false">
    </el-alert>
    <el-row class="ksd-mrl-20">
      <el-table
        :data="groupUsersList"
        class="group-table"
        :empty-text="emptyText"
        v-scroll-shadow>
        <el-table-column
          :label="$t('kylinLang.common.name')"
          show-overflow-tooltip
          prop="group_name">
          <template slot-scope="scope">
            <i class="el-icon-ksd-table_group ksd-fs-14" style="cursor: default;"></i>
            <router-link :to="{path: '/admin/group/' + encodeURIComponent(scope.row.group_name)}" class="group-name">{{scope.row.group_name}}</router-link>
          </template>
        </el-table-column>
        <el-table-column
          :label="$t('usersCount')"
          prop="users"
          show-overflow-tooltip>
          <template slot-scope="scope">
            {{scope.row.users && scope.row.users.length || 0}}
          </template>
        </el-table-column>
        <el-table-column v-if="groupActions.includes('editGroup') && groupActions.includes('deleteGroup')"
          :label="$t('kylinLang.common.action')" :width="83">
          <template slot-scope="scope">
            <el-tooltip :content="$t('assignUsers')" effect="dark" placement="top" v-show="scope.row.group_name!=='ALL_USERS' && groupActions.includes('editGroup')">
              <i class="el-icon-ksd-table_assign ksd-fs-14 ksd-mr-10" :class="{'is-disabled': !isTestingSecurityProfile}" @click="editGroup('assign', scope.row)"></i>
            </el-tooltip><span>
            </span><el-tooltip :content="$t('kylinLang.common.drop')" effect="dark" placement="top" v-show="(scope.row.group_name!=='ROLE_ADMIN' && scope.row.group_name!=='ALL_USERS') && groupActions.includes('deleteGroup')">
              <i class="el-icon-ksd-table_delete ksd-fs-14" :class="{'is-disabled': !isTestingSecurityProfile}" @click="dropGroup(scope.row)"></i>
            </el-tooltip>
          </template>
        </el-table-column>
      </el-table>

      <kylin-pager
        class="ksd-center ksd-mtb-10" ref="pager"
        :refTag="pageRefTags.userGroupPager"
        :totalSize="groupUsersListSize"
        :curPage="pagination.page_offset+1"
        @handleCurrentChange="handleCurrentChange">
      </kylin-pager>
    </el-row>

    <GroupEditModal />
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { mapState, mapGetters, mapActions } from 'vuex'

import locales from './locales'
import { pageRefTags, bigPageCount } from 'config'
import GroupEditModal from '../../common/GroupEditModal/index.vue'
import { handleError, kylinConfirm } from 'util/business'

@Component({
  components: {
    GroupEditModal
  },
  computed: {
    ...mapGetters([
      'groupActions',
      'currentSelectedProject',
      'isTestingSecurityProfile'
    ]),
    ...mapState({
      groupUsersListSize: state => state.user.usersGroupSize,
      userListData: state => state.user.usersList,
      groupUsersList: state => state.user.usersGroupList
    })
  },
  methods: {
    ...mapActions({
      loadGroupUsersList: 'GET_GROUP_USERS_LIST',
      delGroup: 'DEL_GROUP',
      loadUsersList: 'LOAD_USERS_LIST'
    }),
    ...mapActions('GroupEditModal', {
      callGroupEditModal: 'CALL_MODAL'
    })
  },
  locales
})
export default class SecurityGroup extends Vue {
  pageRefTags = pageRefTags
  pagination = {
    page_size: +localStorage.getItem(this.pageRefTags.userGroupPager) || bigPageCount,
    page_offset: 0
  }
  filterName = ''
  isLoadingUserGroups = false

  created () {
    this.loadGroupUsers()
    if (this.groupActions.includes('viewGroup')) {
      this.loadUsers().then((res) => {
        if (res.status !== 200) {
          handleError(res)
        }
      })
    }
  }

  async editGroup (editType, group) {
    if (!this.isTestingSecurityProfile) return
    const isSubmit = await this.callGroupEditModal({ editType, group })
    isSubmit && this.loadGroupUsers()
  }

  inputFilter (value) {
    this.pagination.page_offset = 0
    this.filterName = value
    this.loadGroupUsers(value)
  }

  get emptyText () {
    return this.filterName ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
  }

  loadUsers (filterName) {
    return this.loadUsersList({
      ...this.pagination,
      name: filterName
      // ,
      // project: this.currentSelectedProject // 处理资源组时，发现这个接口不用传 project 参数
    })
  }

  async dropGroup (group) {
    if (!this.isTestingSecurityProfile) return
    try {
      await kylinConfirm(this.$t('confirmDelGroup', {groupName: group.group_name}), {confirmButtonText: this.$t('kylinLang.common.delete')}, this.$t('delGroupTitle'))
      await this.delGroup({group_uuid: group.uuid})
      await this.loadGroupUsers()
      this.$message({ type: 'success', message: this.$t('kylinLang.common.delSuccess') })
    } catch (e) {
      e !== 'cancel' && handleError(e)
    }
  }

  loadGroupUsers (filterGroupName) {
    this.isLoadingUserGroups = true
    this.loadGroupUsersList({
      ...this.pagination,
      user_group_name: filterGroupName
    }).then(() => {
      this.isLoadingUserGroups = false
    }, (res) => {
      handleError(res)
      this.isLoadingUserGroups = false
    })
  }

  handleCurrentChange (pager, pageSize) {
    this.pagination.page_offset = pager
    this.pagination.page_size = pageSize
    this.loadGroupUsers()
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';
.security-group {
  .el-icon-ksd-table_group {
    color: @base-color-1;
  }
  .group-table {
    .el-icon-ksd-table_assign:hover,
    .el-icon-ksd-table_delete:hover {
      color: @base-color;
    }
    .is-disabled {
      cursor: not-allowed;
      color: @text-disabled-color;
      &:hover {
        cursor: not-allowed;
        color: @text-disabled-color;
      }
    }
    .group-name {
      white-space: pre;
    }
  }
}
</style>
