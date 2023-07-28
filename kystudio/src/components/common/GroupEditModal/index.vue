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
          @change="(value, dir, arr) => transferInputHandler('selected_users', value, dir, arr)">
            <div class="load-more-uers" slot="left-remote-load-more" v-if="isShowLoadMore" @click="loadMoreUsers(searchValueLeft)">{{$t('kylinLang.common.loadMore')}}</div>
            <div class="load-more-uers" slot="right-remote-load-more" v-if="isShowRightLoadMore" @click="loadMoreSelectedUsers(searchValueRight)">{{$t('kylinLang.common.loadMore')}}</div>
        </el-transfer>
      </el-form-item>
    </el-form>
    <div slot="footer" class="dialog-footer">
      <el-button size="medium" @click="closeHandler(false)">{{$t('kylinLang.common.cancel')}}</el-button>
      <el-button type="primary" size="medium" :loading="submitLoading" :disabled="submitLoading" @click="submit">{{$t(this.editType === 'assign' ? 'ok' : 'kylinLang.common.create')}}</el-button>
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
      callback: state => state.callback
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
      loadUsersList: 'GET_UNASSIGNED_USERS',
      addUserToGroup: 'ADD_USERS_TO_GROUP'
    })
  },
  locales
})
export default class GroupEditModal extends Vue {
  /**
   * 穿梭框组件的几个字段重点说明
   * data: 左右列表数据的总集合
   * value: 右侧数据列表集合
   * 所以：左侧列表 = data - value
   * total-elements: 是两个元素的数组，分别对应左右两侧的总数值
   */
  // Data: 用来销毁el-form
  isFormShow = false
  // Data: el-form表单验证规则
  rules = {
    group_name: [{
      validator: this.validate(GROUP_NAME), trigger: 'blur', required: true
    }]
  }

  // 表单数据提交防重复提交标记
  submitLoading = false
  // 用于记录穿梭框左右列表总数的
  totalSizes = [0, 0] // 第一个值是左侧的总数，第二个值是右侧的总数
  timer = null // 左侧搜索框输入触发过滤的防抖标记（因为左侧是 ajax 的，需要防抖下）

  // 穿梭框左侧相关字段
  page_offset = 0 // 获取 user 分页页码
  pageSize = 100 // 左侧每页显示条数
  searchValueLeft = ''
  clickLoadMore = false // 左侧加载更多按钮的防重复提交标记
  leftAjaxTotalUsers = [] // 左侧接口回来的数据列表
  leftAjaxTotalSize = 0
  // 穿梭框右侧相关字段
  searchValueRight = ''
  rightPageOffset = 0
  rightPageSize = 100
  reduceTemp = [] // 临时从右挪向左的数据
  addTemp = [] // 临时从左挪向右的数据
  realSelectedUsers = [] // 实际在右侧的数据（综合各种临时挪动后的结果数据）

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

  /**
   * 左侧的当前页码 < ceil(接口总数 / size) - 1 显示加载更多按钮
   * TODO 这里的计算可能有问题
   */
  get isShowLoadMore () {
    return this.page_offset < Math.ceil(this.leftAjaxTotalSize / this.pageSize) - 1
  }
  /**
   * 右侧的当前页码 < ceil(右侧列表总数 / size) - 1 显示加载更多按钮
   */
  get isShowRightLoadMore () {
    const filterArr = this.searchValueRight ? this.realSelectedUsers.filter(user => user.toLowerCase().indexOf(this.searchValueRight.toString().toLowerCase()) >= 0) : this.realSelectedUsers
    const notShowList = filterArr.filter((user) => {
      return !this.form.selected_users.includes(user)
    })
    return notShowList.length > 0
  }

  /**
   * 穿梭框 data 字段绑定的数据
   * 它的值 = ajax 回来的数据 + 临时从右侧移入的非远程数据 + 右侧实际展现的数据 - 临时加入到右侧，但没显示出来的数据
   */
  get totalUserData () {
    // 当前接口取回来的数据
    const leftRemoteData = this.leftAjaxTotalUsers.map((user) => {
      return user.username
    })
    // 临时移出的数据
    const filterReduceTempArr = this.searchValueLeft ? this.reduceTemp.filter((user) => {
      return user.toLowerCase().indexOf(this.searchValueLeft.toString().toLowerCase()) >= 0
    }) : this.reduceTemp
    // 临时加入到用户组，但因为分页，没显示出来的数据，要从总数据中踢掉
    const filterAddTempArr = this.addTemp.filter((user) => {
      return !this.form.selected_users.includes(user)
    })
    const arrTemp = [...new Set([...leftRemoteData, ...filterReduceTempArr, ...this.form.selected_users])]
    const arr = arrTemp.filter((user) => {
      return !filterAddTempArr.includes(user)
    })
    return arr.map((user) => {
      return { key: user, label: user }
    })
  }

  // Computed Method: 计算每个Form的field是否显示
  isFieldShow (fieldName) {
    return fieldVisiableMaps[this.editType].includes(fieldName)
  }

  resetDialogInfo () {
    this.timer = null
    // 左侧相关字段重置
    this.page_offset = 0
    this.searchValueLeft = ''
    this.clickLoadMore = false
    this.leftAjaxTotalUsers = []
    this.leftAjaxTotalSize = 0
    // 右侧字段重置
    this.searchValueRight = ''
    this.rightPageOffset = 0
    this.reduceTemp = []
    this.addTemp = []
    // 初始等于接口返回的列表数据
    this.realSelectedUsers = this.form.origin_selected_users
  }

  // Watcher: 监视销毁上一次elForm
  @Watch('isShow')
  onModalShow (newVal, oldVal) {
    if (newVal) {
      // 初始变量的重置
      this.isFormShow = true
      this.resetDialogInfo()
      if (this.editType === 'assign') {
        this.$set(this.totalSizes, 1, this.form.origin_selected_users.length)
        this.fetchUsers('')
      }
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

  // 穿梭框两侧的搜索框回调
  queryHandler (title, query) {
    const that = this
    return new Promise(async (resolve, reject) => {
      if (title === that.$t('willCheckGroup')) { // 左侧的搜索，走 ajax 搜索
        that.page_offset = 0
        clearTimeout(that.timer)
        that.timer = setTimeout(async function () {
          // 先置空接口总数据
          that.leftAjaxTotalUsers = []
          // 然后获取接口数据
          await that.fetchUsers(query)
          resolve()
        }, 500)
      } else if (title === that.$t('checkedGroup')) { // 右侧的搜索，走前端已有数据的搜索
        try {
          that.searchValueRight = query
          // 进行前端数据的搜索
          const result = that.rightSearchResults(query)
          // 重置穿梭框右侧总数
          that.$set(that.totalSizes, 1, result.length)
          // 针对搜索结果处理分页
          const arr = result.length > that.rightPageSize ? result.slice(0, that.rightPageSize) : result
          // 重置右侧显示数据
          that.setModalForm({selected_users: arr})
          resolve()
        } catch (e) {
          console.error(e)
          reject(e)
        }
      }
    })
  }

  // 右侧前端搜索
  rightSearchResults (content) {
    // 触发搜索时，页码置回 0
    this.rightPageOffset = 0
    // 因为前端分页了，这里的搜索要基于原始所有选中项进行搜索
    if (content) {
      return this.realSelectedUsers.filter(user => user.toLowerCase().indexOf(content.toString().toLowerCase()) >= 0)
    } else {
      return this.realSelectedUsers
    }
  }

  // Action: 修改Form函数
  inputHandler (key, value) {
    this.setModalForm({[key]: value})
  }

  /**
   * 穿梭框左右数据变化时的回调
   * 左右移动时，临时存放数据的字段，要进行处理
   * 同时要修正实际右侧数据的字段
   */
  transferInputHandler (key, value, dir, arr) {
    const leftOld = this.totalSizes[0]
    const rightOld = this.totalSizes[1]
    // arr 是被移动的用户数组，格式为用户名字符串数组
    // 向左移动，临时减用户，向右移动，临时加用户
    if (dir === 'left') {
      this.reduceTemp = [...new Set(this.reduceTemp.concat(arr))]
      // 同时要把临时加用户的数组中，对应的这些用户去掉
      this.addTemp = this.addTemp.filter((auser) => {
        return !arr.includes(auser)
      })
      // 右侧的实际选中数据，要将这几个数据减掉
      this.realSelectedUsers = this.realSelectedUsers.filter((auser) => {
        return !arr.includes(auser)
      })
      const filterTemp = this.searchValueLeft ? arr.filter((user) => {
        return user.toLowerCase().indexOf(this.searchValueLeft.toString().toLowerCase()) >= 0
      }) : arr
      // 修改两边右上角的总数
      this.$set(this.totalSizes, 0, leftOld + filterTemp.length)
      this.$set(this.totalSizes, 1, rightOld - arr.length)
      // 如果移动后数据为空了，而下一页数据还有，那自动取下一页数据
      if (value.length === 0 && this.isShowRightLoadMore) {
        const filterArr = this.searchValueRight ? this.realSelectedUsers.filter(user => user.toLowerCase().indexOf(this.searchValueRight.toString().toLowerCase()) >= 0) : this.realSelectedUsers
        const notShowList = filterArr.filter((user) => {
          return !this.form.selected_users.includes(user)
        })
        const temp = notShowList.slice(0, this.rightPageSize)
        this.setModalForm({[key]: temp})
      } else {
        this.setModalForm({[key]: value})
      }
    } else {
      this.addTemp = [...new Set(this.addTemp.concat(arr))]
      // 同时要把临时减用户的数组中，对应的这些用户去掉
      this.reduceTemp = this.reduceTemp.filter((auser) => {
        return !arr.includes(auser)
      })
      // 右侧的实际选中数据，要加上这几个数据
      this.realSelectedUsers = [...new Set(this.realSelectedUsers.concat(arr))]
      const filterTemp = this.searchValueRight ? arr.filter((user) => {
        return user.toLowerCase().indexOf(this.searchValueRight.toString().toLowerCase()) >= 0
      }) : arr
      // 修改两边右上角的总数
      this.$set(this.totalSizes, 0, leftOld - arr.length)
      this.$set(this.totalSizes, 1, rightOld + filterTemp.length)
      this.setModalForm({[key]: value})
      /**
       * 如果移动的数据超过 100 条，就自动再取 100 条;
       * 但有概率自动取的 100 条，已经在之前操作中，临时挪入右侧了;
       * 这时候触发 fetchUsers 中的自动获取的逻辑
       * */
      if (arr.length >= this.pageSize && this.isShowLoadMore) {
        this.loadMoreUsers(this.searchValueLeft)
      }
    }
  }

  // Action: Form递交函数
  async submit () {
    try {
      this.submitLoading = true
      // 获取Form格式化后的递交数据
      // 传给后端的数据是背后存的字段，不是直接的 form 下的 selected_users
      const data = getSubmitData({
        editType: this.editType,
        form: {
          group_name: this.form.group_name,
          selected_users: this.realSelectedUsers
        }
      })
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

  // Helper: 左侧列表从后台获取用户列表
  async fetchUsers (value) {
    this.searchValueLeft = typeof value === 'undefined' ? '' : value
    // 临时移入
    const filterReduceTempArr = this.searchValueLeft ? this.reduceTemp.filter((user) => {
      return user.toLowerCase().indexOf(this.searchValueLeft.toString().toLowerCase()) >= 0
    }) : this.reduceTemp
    // 本身在右侧，然后临时从右侧移入左侧的数据
    const realReduceTempArr = filterReduceTempArr.filter((user) => {
      return this.form.origin_selected_users.includes(user)
    })
    // 本身就是左侧的数据，临时加到右侧，还未提交的数据
    const filterAddTempArr = this.searchValueLeft ? this.addTemp.filter((user) => {
      return user.toLowerCase().indexOf(this.searchValueLeft.toString().toLowerCase()) >= 0
    }) : this.addTemp
    const realAddTempArr = filterAddTempArr.filter((user) => {
      return !this.form.origin_selected_users.includes(user)
    })
    try {
      const { data: { data } } = await this.loadUsersList({
        page_size: this.pageSize,
        page_offset: this.page_offset,
        // project: this.currentSelectedProject, // 处理资源组时，发现这个接口不用传 project 参数
        name: this.searchValueLeft,
        group_name: this.form.group_name
      })
      this.leftAjaxTotalSize = data.total_size
      this.leftAjaxTotalUsers = this.page_offset === 0 ? data.value : this.leftAjaxTotalUsers.concat(data.value)
      // 左侧的总数 = 接口的 total_size + 本身在右侧，然后临时从右侧移入左侧的数据的个数
      const leftTotalCount = data.total_size + realReduceTempArr.length - realAddTempArr.length
      this.$set(this.totalSizes, 0, leftTotalCount)
      // 实际会显示在左侧的列表个数
      const leftListLen = this.totalUserData.length - this.form.selected_users.length
      // 左侧总个数 > 0,但实际显示的列表个数为 0 ，这时候需要再次自动获取数据
      if (leftTotalCount > 0 && leftListLen < this.pageSize) {
        this.clickLoadMore = false
        this.loadMoreUsers(this.searchValueLeft)
      }
    } catch (e) {
      this.leftAjaxTotalSize = 0
      this.leftAjaxTotalUsers = []
      this.$set(this.totalSizes, 0, realReduceTempArr.length)
    } finally {
      this.clickLoadMore = false
    }
  }

  // 左侧点击加载更多
  loadMoreUsers (value) {
    if (this.clickLoadMore) return
    this.clickLoadMore = true
    this.isShowLoadMore && (this.page_offset += 1, this.fetchUsers(value))
  }

  // 右侧是前端分页，加载更多，是基于原始数据进行追加
  loadMoreSelectedUsers (value) {
    const size = this.rightPageSize
    this.rightPageOffset += 1
    // 先过滤
    const filterArr = value ? this.realSelectedUsers.filter(user => user.toLowerCase().indexOf(value.toString().toLowerCase()) >= 0) : this.realSelectedUsers
    const notShowList = filterArr.filter((user) => {
      return !this.form.selected_users.includes(user)
    })
    // 从未展现列表里，再取两个出来
    const arr = [...new Set(this.form.selected_users.concat(notShowList.slice(0, size)))]
    // 右侧实际展现数据变了后，会自动变化总数据
    this.setModalForm({selected_users: arr})
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
  .el-transfer__buttons {
    .button-text {
      min-width: initial;
    }
  }
  .load-more-uers {
    color: @text-title-color;
    font-size: @text-assist-size;
    text-align: left;
    cursor: pointer;
    margin-left: 15px;
    line-height: 30px;
    &:hover {
      color: @base-color;
    }
  }
  .option-items {
    white-space: pre;
  }
  .el-transfer-panel__checkbox:hover .el-transfer-panel__item-hover{
    display: none!important;
  }
}
</style>
