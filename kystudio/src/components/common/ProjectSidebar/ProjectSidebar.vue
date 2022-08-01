<template>
  <aside class="project-sidebar">
    <section class="header clearfix">
      <div class="header-text font-medium">
        {{$t('kylinLang.common.project')}}
      </div>
      <div class="header-icons">
      </div>
    </section>

    <section class="body">
      <template v-if="project">
        <div class="project-type">
          <i :class="projectTypeClass"></i>
        </div>
        <el-tooltip :content="project.name" placement="top" :disabled="!isShowNameTooltip">
          <div class="project-name">
            <span class="font-medium">{{project.name}}</span>
          </div>
        </el-tooltip>
        <div class="project-detail-list">
          <div class="detail" v-for="(value, key) in projectDetails" :key="key">
            <div class="detail-title font-medium">{{$t(key)}}</div>
            <div class="detail-value" :style="{ maxHeight: `${windowHeight - 565 > 90 ? windowHeight - 565 : 90}px` }" v-if="value">{{value}}</div>
            <div class="detail-value empty-text" v-else>{{$t('empty')}}</div>
            <el-popover
              v-if="key in form"
              placement="bottom"
              popper-class="project-sidebar-popper"
              trigger="click"
              :title="$t(key)"
              v-model="form[key].isEdit"
              @after-leave="handleHide(form[key])">
              <el-input class="popover-input" size="small" v-model="form[key].value" type="textarea"></el-input>
              <div style="text-align: right;">
                <el-button type="info" size="mini" text @click="handleHide(form[key])" :disabled="isLoading">{{$t('kylinLang.common.cancel')}}</el-button>
                <el-button type="primary" size="mini" text @click="handleSubmit(form[key])" :loading="isLoading">{{$t('kylinLang.common.submit')}}</el-button>
              </div>
              <div class="edit-action" slot="reference">
                <i class="el-icon-ksd-table_edit"></i>
              </div>
            </el-popover>
          </div>
        </div>
      </template>
      <div class="empty-page" v-else>
        <el-row class="center"><img :src="emptyImg" /></el-row>
        <el-row class="center">{{$t('kylinLang.common.noData')}}</el-row>
      </div>
    </section>
  </aside>
</template>

<script>
import Vue from 'vue'
import dayjs from 'dayjs'
import { mapActions } from 'vuex'
// import Scrollbar from 'smooth-scrollbar'
import { Component, Watch } from 'vue-property-decorator'

import locales from './locales'
import emptyImg from '../../../assets/img/empty/empty_state_empty.svg'

@Component({
  props: {
    project: {
      type: Object,
      default: (value) => ({})
    }
  },
  methods: {
    ...mapActions({
      updateProject: 'UPDATE_PROJECT'
    })
  },
  locales
})
export default class ProjectSidebar extends Vue {
  form = {
    name: {
      value: '',
      isEdit: false
    },
    description: {
      value: '',
      isEdit: false
    }
  }
  isLoading = false
  windowHeight = 0
  isShowNameTooltip = false
  emptyImg = emptyImg
  get projectTypeClass () {
    switch (this.project.maintain_model_type) {
      case 'MANUAL_MAINTAIN':
        return 'el-icon-ksd-model_designer'
    }
  }
  get projectDetails () {
    return {
      owner: this.project.owner || 'None',
      createTime: dayjs(this.project.create_time_utc).format('MM-DD-YYYY'),
      projectType: this.$t(this.project.maintain_model_type),
      description: this.project.description
    }
  }
  get submitData () {
    const newProject = JSON.parse(JSON.stringify(this.project))
    for (const [fieldName, data] of Object.entries(this.form)) {
      const { value, isEdit } = data
      if (isEdit) {
        newProject[fieldName] = value
      }
    }
    return {
      name: newProject.name,
      desc: JSON.stringify(newProject)
    }
  }
  @Watch('project', { immediate: true })
  onProjectChange (value) {
    if (value) {
      this.resetFormData()
      setTimeout(() => {
        this.freshProjectNameTooltipIsShow()
      })
    }
  }
  handleHide (popoverData) {
    popoverData.isEdit = false
    this.resetFormData()
  }
  async handleSubmit (popoverData) {
    this.isLoading = true
    await this.updateProject(this.submitData)
    this.handleHide(popoverData)
    this.isLoading = false
  }
  resetFormData () {
    this.form.name.value = this.project.name || ''
    this.form.description.value = this.project.description || ''
  }
  refreshHeight () {
    this.windowHeight = window.innerHeight
  }
  freshProjectNameTooltipIsShow () {
    const projectNameEl = this.$el.querySelector('.project-name')
    const textEl = projectNameEl.querySelector('span')
    this.isShowNameTooltip = textEl.clientWidth > projectNameEl.clientWidth
  }
  handleResize () {
    this.refreshHeight()
  }
  mounted () {
    this.handleResize()
    window.addEventListener('resize', this.handleResize)
  }
  beforeDestory () {
    window.removeEventListener('resize', this.handleResize)
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.project-sidebar {
  .header,
  .body {
    padding: 20px;
    box-sizing: border-box;
  }
  .header {
    font-size: 16px;
    color: #263238;
    border-bottom: 1px solid @line-split-color;
  }
  .header-text {
    float: left;
  }
  .header-icons {
    float: right;
    position: relative;
    transform: translateY(4px);
    i {
      margin-right: 4px;
    }
    i:last-child {
      margin-right: 0;
    }
  }
  .body {
    height: calc(~"100% - 63px");
    overflow: auto;
    position: relative;
  }
  .body .btn-group {
    text-align: center;
  }
  .body .btn-group .el-button {
    width: 100%;
    margin-bottom: 10px;
  }
  .project-type,
  .project-name {
    text-align: center;
  }
  .project-type {
    font-size: 67px;
    line-height: 1;
    margin-bottom: 10px;
    i {
      cursor: default;
    }
  }
  .project-name {
    margin-bottom: 20px;
    overflow: hidden;
    text-overflow: ellipsis;
    span {
      display: inline-block;
    }
  }
  .edit-action {
    position: absolute;
    bottom: 8px;
    right: 7px;
    font-size: 12px;
    display: inline-block;
    width: 18px;
    height: 18px;
    border-radius: 50%;
    overflow: hidden;
    vertical-align: middle;
    cursor: pointer;
  }
  .edit-action:hover {
    background: @grey-3;
    .el-icon-ksd-table_edit {
      color: @base-color;
    }
  }
  .el-icon-ksd-table_edit {
    position: absolute;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
  }
  .detail {
    position: relative;
    padding: 10px;
    border-right: 1px solid @line-border-color;
    border-bottom: 1px solid @line-border-color;
    border-left: 1px solid @line-border-color;
    &:first-child {
      border-top: 1px solid @line-border-color;
    }
    &:nth-child(even) {
      background: @table-stripe-color;
    }
    .empty-text {
      opacity: 0;
    }
  }
  .detail-title {
    margin-bottom: 6px;
  }
  .detail-value {
    padding-right: 15px;
    overflow: auto;
    hyphens: auto;
    word-wrap: break-word;
  }
}
.project-sidebar-popper {
  .popover-input {
    margin-bottom: 13px;
    textarea {
      min-height: 80px !important;
      width: 300px;
    }
  }
  .el-popover__title {
    font-weight: @font-medium;
  }
}
</style>
