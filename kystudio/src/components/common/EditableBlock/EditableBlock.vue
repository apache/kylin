<template>
  <div class="editable-block" :class="{ 'is-edit': isEditing, 'is-loading': isLoading }">
    <div class="block-header" v-if="headerContent">
      <span>{{headerContent}}</span>
      <slot name="header"></slot>
      <i class="icon el-icon-ksd-table_edit"
        v-if="isEditable && !isEditing"
        @click="handleEdit">
      </i>
    </div>
    <div class="block-body" :class="{'no-footer': !isEditing}">
      <slot></slot>
    </div>
    <div class="block-foot ksd-btn-group-minwidth" v-if="isEditing">
      <el-button size="small" v-if="isReset" :loading="isResetLoading" @click="handleCancel">{{cancelText}}</el-button><el-button
      type="primary" size="small" :loading="isLoading" :disabled="!isEdited && isKeepEditing" @click="handleSubmit">{{$t('kylinLang.common.save')}}</el-button>
    </div>
  </div>
</template>

<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'

@Component({
  props: {
    isEditable: {
      type: Boolean,
      default: true
    },
    headerContent: {
      type: String
    },
    isKeepEditing: {
      type: Boolean,
      default: false
    },
    isEdited: {
      type: Boolean,
      default: false
    },
    isReset: {
      type: Boolean,
      default: true
    }
  }
})
export default class EditableBlock extends Vue {
  isUserEditing = false
  isLoading = false
  isResetLoading = false
  set isEditing (value) {
    this.isUserEditing = value
  }
  get isEditing () {
    return (this.isUserEditing || this.isKeepEditing) && this.isEditable
  }
  get cancelText () {
    return this.isKeepEditing ? this.$t('kylinLang.common.reset') : this.$t('kylinLang.common.cancel')
  }
  handleEdit () {
    this.isEditing = true
  }
  handleCancel () {
    this.isEditing = false
    this.isResetLoading = true
    this.$emit('cancel', this.handleSuccess, this.handleError)
  }
  handleSubmit () {
    this.isLoading = true
    this.$emit('submit', this.handleSuccess, this.handleError)
  }
  handleError () {
    this.isLoading = false
    this.isResetLoading = false
  }
  handleSuccess () {
    this.isLoading = false
    this.isResetLoading = false
    this.isEditing = false
  }
}
</script>

<style lang="less">
@import '../../../assets/styles/variables.less';

.editable-block {
  .block-header {
    border-top: 1px solid @ke-border-secondary;
    border-right: 1px solid @ke-border-secondary;
    border-left: 1px solid @ke-border-secondary;
    // background: @regular-background-color;
    color: @text-title-color;
    font-size: 16px;
    line-height: 22px;
    font-weight: @font-medium;
    padding: 16px 24px 8px;
    border-top-left-radius: 8px;
    border-top-right-radius: 8px;
    > * {
      vertical-align: middle;
    }
  }
  .block-header .icon {
    color: @text-normal-color;
    border-radius: 50%;
    overflow: hidden;
    font-size: 14px;
    padding: 2px;
    cursor: pointer;
    &:hover {
      background: @text-placeholder-color;
      color: @base-color;
    }
  }
  .block-body {
    border-left: 1px solid @ke-border-secondary;
    border-right: 1px solid @ke-border-secondary;
    background: @fff;
    &.no-footer{
      border-bottom-left-radius: 8px;
      border-bottom-right-radius: 8px;
      border-bottom: 1px solid @ke-border-secondary;
    }
  }
  .block-foot {
    border-bottom: 1px solid @ke-border-secondary;
    border-right: 1px solid @ke-border-secondary;
    border-left: 1px solid @ke-border-secondary;
    padding: 0px 24px 16px;
    background: @fff;
    text-align: left;
    border-bottom-left-radius: 8px;
    border-bottom-right-radius: 8px;
    .el-button+.el-button {
      margin-left: 8px;
    }
  }
}
</style>

