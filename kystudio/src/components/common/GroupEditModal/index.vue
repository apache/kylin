<template>
  <el-dialog :class="['group-edit-modal', {'assign-group': editType === 'assign'}]" :width="modalWidth"
    :title="$t(modalTitle)"
    :visible="isShow"
    limited-area
    :close-on-click-modal="false"
    :close-on-press-escape="false"
    @close="isShow && closeHandler(false)">
    <el-form :model="form" @submit.native.prevent :rules="rules" ref="form" v-if="isFormShow">
      <!-- 表单：组名 -->
      <el-form-item :label="$t('kylinLang.common.groupName')" prop="group_name" v-if="isFieldShow('group_name')">
        <el-input auto-complete="off" :placeholder="$t('groupnamePld')" @input="value => inputHandler('group_name', value)" :value="form.group_name" @keyup.enter.native="submit"></el-input>
      </el-form-item>
      <!-- 表单：分配用户 -->
      <el-form-item v-if="isFieldShow('users')">
        <el-transfer
          filterable
          :data="totalUserData"
          :value="form.selected_users"
          :before-query="queryHandler"
          :filter-placeholder="$t('userPld')"
          :total-elements="totalSizes"
          :show-overflow-tip="true"
          :titles="[$t('willCheckGroup'), $t('checkedGroup')]"
          @change="value => transferInputHandler('selected_users', value)">
            <div class="load-more-uers" slot="left-remote-load-more" v-if="isShowLoadMore" @click="loadMoreUsers(searchValueLeft)">{{$t('kylinLang.common.loadMore')}}</div>
        </el-transfer>
      </el-form-item>
    </el-form>
    <div slot="footer" class="dialog-footer">
      <el-button size="medium" @click="closeHandler(false)">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button type="primary" size="medium" :loading="submitLoading" :disabled="submitLoading" @click="submit">{{$t('ok')}}</el-button>
    </div>
  </el-dialog>
</template>

<script>
import Vue from 'vue'
import { Component, Watch } from 'vue-property-decorator'
import { mapState, mapGetters, mapMutations, mapActions } from 'vuex'

import vuex from '../../../store'
import locales from './locales'
import store, { types } from './store'
import { fieldVisiableMaps, titleMaps, getSubmitData } from './handler'
import { validate, validateTypes, handleError } from '../../../util'

const { GROUP_NAME } = validateTypes

vuex.registerModule(['modals', 'GroupEditModal'], store)

@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ]),
    // Store数据注入
    ...mapState('GroupEditModal', {
      form: state => state.form,
      isShow: state => state.isShow,
      editType: state => state.editType,
      callback: state => state.callback,
      totalUsers: state => state.totalUsers
    })
  },
  methods: {
    // Store方法注入
    ...mapMutations('GroupEditModal', {
      setModal: types.SET_MODAL,
      hideModal: types.HIDE_MODAL,
      setModalForm: types.SET_MODAL_FORM,
      resetModalForm: types.RESET_MODAL_FORM
    }),
    // 后台接口请求
    ...mapActions({
      saveGroup: 'ADD_GROUP',
      loadUsersList: 'LOAD_USERS_LIST',
      addUserToGroup: 'ADD_USERS_TO_GROUP'
    })
  },
  locales
})
export default class GroupEditModal extends Vue {
  // Data: 用来销毁el-form
  isFormShow = false
  // Data: el-form表单验证规则
  rules = {
    group_name: [{
      validator: this.validate(GROUP_NAME), trigger: 'blur', required: true
    }]
  }

  // 获取user分页页码
  page_offset = 0
  // 每页请求数量
  pageSize = 100

  totalUsersSize = 0
  // 返回的数据总数
  totalSizes = [0, 10]
  searchValueLeft = ''
  searchValueRight = ''
  clickLoadMore = false
  submitLoading = false
  autoLoadLimit = 100
  timer = null

  // Computed: Modal宽度
  get modalWidth () {
    return this.editType === 'assign'
      ? '600px'
      : '440px'
  }

  // Computed: Modal标题
  get modalTitle () {
    return titleMaps[this.editType]
  }

  get isShowLoadMore () {
    return this.page_offset < Math.ceil(this.totalUsersSize / this.pageSize) - 1
  }

  get totalUserData () {
    return this.totalUsers.length ? this.totalUsers : []
  }

  // Computed Method: 计算每个Form的field是否显示
  isFieldShow (fieldName) {
    return fieldVisiableMaps[this.editType].includes(fieldName)
  }

  // Watcher: 监视销毁上一次elForm
  @Watch('isShow')
  onModalShow (newVal, oldVal) {
    if (newVal) {
      this.isFormShow = true
      this.page_offset = 0
      this.setModal({totalUsers: []})
      this.editType === 'assign' && this.fetchUsers('')
    } else {
      setTimeout(() => {
        this.isFormShow = false
      }, 300)
    }
  }

  // Action: 模态框关闭函数
  closeHandler (isSubmit) {
    this.hideModal()

    setTimeout(() => {
      this.resetModalForm()
      this.callback && this.callback(isSubmit)
    }, 200)
  }

  queryHandler (title, query) {
    const that = this
    return new Promise(async (resolve, reject) => {
      if (title === that.$t('willCheckGroup')) {
        clearTimeout(this.timer)
        this.timer = setTimeout(async function () {
          this.page_offset = 0
          await that.setModal({totalUsers: []})
          await that.fetchUsers(query)
          resolve()
        }, 500)
      } else if (title === that.$t('checkedGroup')) {
        try {
          that.searchValueRight = query
          that.$set(that.totalSizes, 1, that.searchResults(query).length)
          resolve()
        } catch (e) {
          console.error(e)
          reject(e)
        }
      }
    })
  }

  // 匹配搜索结果的用户
  searchResults (content) {
    return this.form.selected_users.filter(user => user.toLowerCase().indexOf(content.toString().toLowerCase()) >= 0)
  }

  // Action: 修改Form函数
  inputHandler (key, value) {
    this.setModalForm({[key]: value})
  }

  transferInputHandler (key, value) {
    this.setModalForm({[key]: value})
    this.totalSizes[0] = this.totalUsersSize - (!this.searchValueLeft.length ? value.length : this.searchResults(this.searchValueLeft).length)
    const surplusUsers = this.totalUsers.filter(user => !value.includes(user.key))
    surplusUsers.length < this.autoLoadLimit && (!this.searchValueLeft.length ? this.loadMoreUsers() : this.loadMoreUsers(this.searchValueLeft))
  }

  // Action: Form递交函数
  async submit () {
    try {
      this.submitLoading = true
      // 获取Form格式化后的递交数据
      const data = getSubmitData(this)
      // 验证表单
      await this.$refs['form'].validate()
      // 针对不同的模式，发送不同的请求
      this.editType === 'new' && await this.saveGroup(data)
      this.editType === 'assign' && await this.addUserToGroup(data)
      // 成功提示
      this.$message({
        type: 'success',
        message: this.editType === 'assign' ? this.$t('updateUserOrGroupSuccess') : this.$t('saveUserGroupSuccess')
      })
      this.submitLoading = false
      // 关闭模态框，通知父组件成功
      this.closeHandler(true)
    } catch (e) {
      this.submitLoading = false
      // 异常处理
      e && handleError(e)
    }
  }

  // Helper: 给el-form用的验证函数
  validate (type) {
    // TODO: 这里的this是vue的实例，而data却是class的实例
    return validate[type].bind(this)
  }

  // Helper: 从后台获取用户组
  async fetchUsers (value) {
    this.searchValueLeft = typeof value === 'undefined' ? '' : value

    const { data: { data } } = await this.loadUsersList({
      page_size: this.pageSize,
      page_offset: this.page_offset,
      // project: this.currentSelectedProject, // 处理资源组时，发现这个接口不用传 project 参数
      name: value
    })

    const remoteUsers = data.value
      .map(user => ({ key: user.username, label: user.username }))

    // const filterNotSelected = [...this.totalUsers, ...remoteUsers].filter(item => !this.form.selected_users.includes(item.key))

    const selectedUsersNotInRemote = this.form.selected_users
      .map(sItem => ({key: sItem, label: sItem}))
      .filter(sItem => ![...(this.page_offset === 0 ? [] : this.totalUsers), ...remoteUsers].some(user => user.key === sItem.key))

    const searchUserIsSelected = (typeof value !== 'undefined' && value) ? this.form.selected_users.filter(user => user.toLowerCase().indexOf(value.toString().toLowerCase()) >= 0) : [...this.totalUsers, ...remoteUsers].filter(user => this.form.selected_users.includes(user.key))

    this.totalUsersSize = data.total_size

    typeof value !== 'undefined' && value ? (this.totalSizes = [this.totalUsersSize - searchUserIsSelected.length]) : (this.totalSizes = [data.total_size - this.form.selected_users.length])

    const users = [...new Set([ ...(this.page_offset === 0 ? [] : this.totalUsers), ...remoteUsers, ...selectedUsersNotInRemote ].map(it => it.key))].map(item => ({key: item, label: item}))

    this.autoLoadMoreData(users, value)

    this.setModal({totalUsers: users})
  }

  // 判断是否自动加载更多的数据
  autoLoadMoreData (users, value) {
    this.clickLoadMore = false
    const len = users.filter(user => this.form.selected_users.includes(user.key)).length
    if (users.length - len < this.autoLoadLimit && this.isShowLoadMore) {
      typeof value !== 'undefined' && !value.length ? this.loadMoreUsers() : this.loadMoreUsers(value)
      return
    }
  }

  loadMoreUsers (value) {
    if (this.clickLoadMore) return
    this.clickLoadMore = true
    this.isShowLoadMore && (this.page_offset += 1, this.fetchUsers(value))
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';
.group-edit-modal {
  &.assign-group {
    .el-dialog {
      width: 650px\0 !important;
    }
  }
  .el-form-item__content {
    line-height: 1;
  }
  .el-transfer-panel {
    width: 250px;
  }
  .load-more-uers {
    color: @text-title-color;
    font-size: @text-assist-size;
    text-align: left;
    cursor: pointer;
    margin-left: 15px;
    &:hover {
      color: @base-color;
    }
  }
  .option-items {
    white-space: pre;
  }
}
</style>
