<template>
   <el-select
    size="small"
    class="project_select"
    filterable
    :title="selected_project"
    :placeholder="$t('pleaseSearchOrSelectProject')"
    v-model="selected_project"
    popper-class="project-select_dropdown"
    @change="changeProject">
    <el-option v-if="systemActions.includes('viewAllProjectJobs') && needAllProjectView " value="**" :label="$t('selectAll')"></el-option>
    <span slot="prefix" v-if="projectList.length" class="el-input__icon" :class="'el-ksd-icon-project_16'"></span>
    <el-option
      v-for="item in projectList" :key="item.name"
      class="project_option"
      :label="item.name"
      :value="item.name">
      <i class="el-ksd-icon-project_16" v-if="item.maintain_model_type === 'MANUAL_MAINTAIN'"></i>
      <span>{{item.name}}</span>
    </el-option>
    </el-select>
</template>
<script>
import { mapGetters } from 'vuex'
import { hasRole } from 'util/business'
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
@Component({
  methods: {
    changeProject (val) {
      if (val === '**') {
        this.$store.state.project.isAllProject = true
      } else {
        this.$store.state.project.isAllProject = false
        this.$emit('changePro', val)
      }
    },
    checkAllProjectView () {
      if (this.$route.name === 'Job' || this.$route.name === 'StreamingJob') { // 支持在monitor页面让用户选择全局视角
        this.needAllProjectView = true
      } else {
        this.$store.state.project.isAllProject = false
        this.selected_project = this.$store.state.project.selected_project
        this.needAllProjectView = false
      }
    }
  },
  watch: {
    '$store.state.project.selected_project' () {
      this.selected_project = this.$store.state.project.selected_project
    },
    '$route' (route) {
      this.checkAllProjectView()
    }
  },
  computed: {
    ...mapGetters([
      'systemActions'
    ]),
    projectList () {
      return this.$store.state.project.allProject.filter(it => it.maintain_model_type === 'MANUAL_MAINTAIN')
    },
    isAdmin () {
      return hasRole(this, 'ROLE_ADMIN')
    }
  },
  locales: {
    'en': {selectAll: 'All Projects', pleaseSearchOrSelectProject: 'Please search or select project'}
  }
})
export default class ProjectSelect extends Vue {
  data () {
    return {
      selected_project: '',
      needAllProjectView: false
    }
  }
  created () {
    this.checkAllProjectView()
    this.selected_project = this.$store.state.project.selected_project
  }
}
</script>
<style lang="less">
@import "../../assets/styles/variables.less";

.project_select{
  margin: 10px 4px 0 4px;
  float: left;
  width: 192px;
  .el-input__inner {
    background-color: @ke-background-color-hover;
  }
  .el-input__icon {
    font-size: 18px;
    color: @text-disabled-color;
    transform: translate(-1px, 0px);
  }
}
.project_option {
  .el-icon-ksd-smart_mode_small,
  .el-icon-ksd-expert_mode_small {
    color: @text-disabled-color;
  }
}
.el-select-dropdown.project-select_dropdown {
  max-width: 440px;
}
</style>
