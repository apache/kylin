<template>
<div class="ksd-tab">
  <el-tabs v-model="activeName" :type="type" :editable="editable" :addable="false" @tab-click="handleClick" @edit="handleTabsEdit">
    <slot name="defaultPane"></slot>
    <el-tab-pane
      v-for="item in tabs" :key="item.name + item.icon + $store.state.system.lang"
      :label="item.i18n? item.title.replace(item.i18n,$t('kylinLang.common.'+item.i18n)): item.title"
      :name="item.name"
      :closable="item.closable">
      <span slot="label" v-show="!item.disabled">
        <i :class="item.icon" :spin="item.spin" ></i> {{ item.i18n? item.title.replace(item.i18n,$t('kylinLang.common.'+item.i18n)): item.title }}</span>
      <slot :item = "item" v-show="!item.disabled"></slot>
    </el-tab-pane>
  </el-tabs>
</div>
</template>
<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
@Component({
  props: ['tabslist', 'isedit', 'active', 'type'],
  computed: {
    tabs () {
      return this.tabslist
    }
  },
  watch: {
    active () {
      this.activeName = this.active
    }
  },
  methods: {
    handleClick (tab, event) {
      this.$emit('clicktab', tab.name)
    },
    handleTabsEdit (targetName, action) {
      if (action === 'remove') {
        this.$emit('removetab', targetName, '', '_close')
      }
    }
  }
})
export default class Tab extends Vue {
  data () {
    return {
      currentView: '',
      editable: this.isedit,
      activeName: this.active
    }
  }
  created () {
  }
}
</script>
<style lang="less">
.ksd-tab {
  .el-tabs__new-tab{
    display: none;
  }
}
</style>
