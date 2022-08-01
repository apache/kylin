<template>
  <div class="paddingbox" id="project-list">
 <div class="ksd-title-label ksd-mt-20">{{$t('projectsList')}}</div>
  <div>
    <el-button type="primary" size="medium" class="ksd-mb-10 ksd-mt-10" icon="el-ksd-icon-add_22" v-if="projectActions.includes('addProject')" @click="newProject">{{$t('kylinLang.common.project')}}</el-button>
    <div style="width:240px;" class="ksd-fright ksd-mtb-10">
      <el-input class="show-search-btn"
        size="medium"
        prefix-icon="el-ksd-icon-search_22"
        :placeholder="$t('projectFilter')"
        v-global-key-event.enter.debounce="inputFilter"
        @clear="inputFilter('')"
      >
      </el-input>
    </div>
    <el-table
      :data="projectList"
      tooltip-effect="dark"
      v-scroll-shadow
      :empty-text="emptyText"
      class="project-table"
      style="width: 100%">
      <el-table-column
        :label="$t('name')"
        show-overflow-tooltip
        :width="320"
        prop="name">
      </el-table-column>
      <el-table-column
        :label="$t('type')"
        show-overflow-tooltip
        :width="120"
        prop="maintain_model_type">
        <template slot-scope="scope">
          {{scope.row.maintain_model_type === projectType.auto ? $t('autoType') : $t('manualType')}}
        </template>
      </el-table-column>
      <el-table-column
        :label="$t('owner')"
        :width="220"
        show-overflow-tooltip
        prop="owner">
      </el-table-column>
      <el-table-column
        :label="$t('description')"
        show-overflow-tooltip
        prop="description">
      </el-table-column>
      <el-table-column
        show-overflow-tooltip
        :width="218"
        :label="$t('createTime')"
        prop="gmtTime">
        <template slot-scope="scope">
          {{transToGmtTime(scope.row.create_time_utc)}}
        </template>
      </el-table-column>
      <el-table-column
        :width="83"
        :label="$t('actions')">
        <template slot-scope="scope">
          <el-tooltip :content="$t('author')" effect="dark" placement="top">
            <router-link :to="{path: '/admin/project/' + scope.row.name, query: {projectId: scope.row.uuid}}">
              <i class="el-icon-ksd-security ksd-mr-10 ksd-fs-14" v-if="projectActions.includes('accessActions')"></i>
            </router-link>
          </el-tooltip><!--
          --><common-tip :content="$t('kylinLang.common.moreActions')">
            <el-dropdown trigger="click">
              <i class="el-icon-ksd-table_others"></i>
              <el-dropdown-menu slot="dropdown" class="project-dropdown">
                <el-dropdown-item v-if="canExecuteModelMetadata(scope.row)" @click.native="handleExportModels(scope.row)">{{$t('exportModelsMetadata')}}</el-dropdown-item>
                <el-dropdown-item v-if="canExecuteModelMetadata(scope.row)" @click.native="handleImportModels(scope.row)">{{$t('importModelsMetadata')}}</el-dropdown-item>
                <el-dropdown-item v-if="projectActions.includes('changeProjectOwner')" @click.native="openChangeProjectOwner(scope.row.name)">{{$t('changeProjectOwner')}}</el-dropdown-item>
                <el-dropdown-item v-if="projectActions.includes('deleteProject')" @click.native="removeProject(scope.row)">{{$t('delete')}}</el-dropdown-item>
              </el-dropdown-menu>
            </el-dropdown>
          </common-tip>
        </template>
      </el-table-column>
      </el-table>
      <kylin-pager
        class="ksd-center ksd-mtb-10" ref="pager"
        :refTag="pageRefTags.projectPager"
        :totalSize="projectsTotal"
        :perPageSize="filterData.page_size"
        :curPage="filterData.page_offset+1"
        @handleCurrentChange="handleCurrentChange">
      </kylin-pager>
    </div>
    <el-dialog width="480px" :title="$t('changeProjectOwner')" class="change_owner_dialog" :visible.sync="changeOwnerVisible" @close="resetProjectOwner" :close-on-click-modal="false">
      <el-alert
        :title="$t('changeDesc')"
        type="info"
        :show-background="false"
        :closable="false"
        class="ksd-pt-0"
        show-icon>
      </el-alert>
      <el-form :model="projectOwner" @submit.native.prevent ref="projectOwnerForm" label-width="130px" label-position="top">
        <el-form-item :label="$t('project')" prop="project">
          <el-input disabled name="project" v-model="projectOwner.project" size="medium"></el-input>
        </el-form-item>
        <el-form-item :label="$t('changeTo')" prop="owner">
         <el-select
          :placeholder="$t('pleaseChangeOwner')"
          filterable
          remote
          :remote-method="loadAvailableProjectOwners"
          @blur="(e) => loadAvailableProjectOwners(e.target.value)"
          v-model="projectOwner.owner"
          size="medium"
          class="owner-select"
          style="width:100%">
          <el-option :label="user" :value="user" v-for="user in userOptions" :key="user"></el-option>
        </el-select>
        </el-form-item>
      </el-form>
      <div slot="footer" class="dialog-footer ky-no-br-space">
        <el-button size="medium" @click="changeOwnerVisible = false">{{$t('kylinLang.common.cancel')}}</el-button>
        <el-button type="primary" size="medium" :disabled="!(projectOwner.project&&projectOwner.owner)" @click="changeProjectOwner" :loading="changeLoading">{{$t('change')}}</el-button>
      </div>
    </el-dialog>
 </div>
</template>
<script>
import { mapActions, mapGetters, mapMutations } from 'vuex'
import { permissions, projectCfgs, pageRefTags, bigPageCount } from '../../config/index'
import { handleSuccessAsync } from '../../util'
import { handleSuccess, handleError, transToGmtTime, hasPermission, hasRole, kylinConfirm } from '../../util/business'
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
@Component({
  methods: {
    transToGmtTime: transToGmtTime,
    ...mapActions({
      loadProjects: 'LOAD_PROJECT_LIST',
      loadAllProjects: 'LOAD_ALL_PROJECT',
      deleteProject: 'DELETE_PROJECT',
      updateProject: 'UPDATE_PROJECT',
      saveProject: 'SAVE_PROJECT',
      backupProject: 'BACKUP_PROJECT',
      getAvailableProjectOwners: 'GET_AVAILABLE_PROJECT_OWNERS',
      updateProjectOwner: 'UPDATE_PROJECT_OWNER'
    }),
    ...mapActions('ProjectEditModal', {
      callProjectEditModal: 'CALL_MODAL'
    }),
    ...mapActions('ModelsExportModal', {
      callModelsExportModal: 'CALL_MODAL'
    }),
    ...mapActions('ModelsImportModal', {
      callModelsImportModal: 'CALL_MODAL'
    }),
    ...mapMutations({
      resetQueryTabs: 'RESET_QUERY_TABS'
    }),
  },
  computed: {
    ...mapGetters([
      'projectActions'
    ]),
    projectList () {
      return this.$store.state.project.projectList
    },
    projectsTotal () {
      return this.$store.state.project.projectTotalSize
    },
    isAdmin () {
      return hasRole(this, 'ROLE_ADMIN')
    },
    emptyText () {
      return this.filterData.project ? this.$t('kylinLang.common.noResults') : this.$t('kylinLang.common.noData')
    }
  },
  locales: {
    'en': {
      manualType: 'AI Augmented Mode',
      project: 'Project',
      name: 'Project Name',
      type: 'Type',
      owner: 'Owner',
      description: 'Description',
      createTime: 'Create Time',
      actions: 'Actions',
      setting: 'Setting',
      access: 'Access',
      externalFilters: 'External Filters',
      edit: 'Configure',
      backup: 'Backup',
      delete: 'Delete',
      delProjectTitle: 'Delete Project',
      cancel: 'Cancel',
      yes: 'Ok',
      saveSuccessful: 'Added the project successfully.',
      saveFailed: 'Can\'t add the project at the moment.',
      deleteProjectTip: 'The project "{projectName}" cannot be restored after deletion. Are you sure you want to delete?',
      projectConfig: 'Configuration',
      backupProject: 'Are you sure you want to backup this project ?',
      noProject: 'No project was found. Please click the button to add a project.',
      projectsList: 'Project List',
      projectFilter: 'Search by project name',
      backupPro: 'Backup Project',
      author: 'Authorization',
      exportModelsMetadata: 'Export Model',
      importModelsMetadata: 'Import Model',
      changeProjectOwner: 'Change Owner',
      change: 'Change',
      changeDesc: 'System admin or the admin of this project could be set as the owner.',
      changeTo: 'Change Owner To',
      pleaseChangeOwner: 'Please choose project owner',
      changeProSuccess: 'The owner of the project "{project}" has been successfully changed to "{owner}".'
    }
  }
})
export default class ProjectList extends Vue {  
  canExecuteModelMetadata (row) {
    return this.projectActions.includes('executeModelsMetadata') &&
      row.maintain_model_type !== projectCfgs.projectType.auto
  }
  inputFilter (value) {
    this.filterData.project = value
    this.filterData.page_offset = 0
    this.loadProjects(this.filterData)
  }
  checkProjectForm () {
    this.$refs.projectForm.$emit('projectFormValid')
  }
  handleCurrentChange (currentPage, pageSize) {
    this.filterData.page_offset = currentPage
    this.filterData.page_size = pageSize
    this.loadProjects(this.filterData)
  }
  async newProject () {
    const isSubmit = await this.callProjectEditModal({ editType: 'new' })
    isSubmit && this.loadProjects(this.filterData)
    this.loadAllProjects()
  }
  async changeProject (project) {
    const isSubmit = await this.callProjectEditModal({ editType: 'edit', project })
    if (isSubmit) {
      this.loadProjects(this.filterData)
      this.loadAllProjects()
    }
  }
  removeProject (project) {
    kylinConfirm(this.$t('deleteProjectTip', {projectName: project.name}), null, this.$t('delProjectTitle')).then(() => {
      this.deleteProject(project.name).then((result) => {
        this.$message({
          type: 'success',
          message: this.$t('kylinLang.common.delSuccess')
        })
        this.loadProjects(this.filterData)
        this.loadAllProjects()
        this.resetQueryTabs({projectName: project.name})
      }, (res) => {
        handleError(res)
      })
    })
  }
  async handleExportModels (row) {
    const { name: project } = row
    await this.callModelsExportModal({ project, type: 'all' })
  }
  async handleImportModels (row) {
    const { name: project } = row
    await this.callModelsImportModal({ project })
  }
  backup (project) {
    kylinConfirm(this.$t('backupProject'), {type: 'info'}, this.$t('backupPro')).then(() => {
      this.backupProject(project).then((result) => {
        handleSuccess(result, (data, code, status, msg) => {
          this.$message({
            type: 'success',
            message: this.$t('kylinLang.common.backupSuccessTip') + data
          })
        })
      }, (res) => {
        handleError(res)
      })
    })
  }
  initAccessMeta () {
    return {
      permission: '',
      principal: true,
      sid: ''
    }
  }
  hasAdminProjectPermission () {
    return hasPermission(this, permissions.ADMINISTRATION.mask)
  }
  async loadAvailableProjectOwners (filterName) {
    this.ownerFilter.name = filterName || ''
    try {
      const res = await this.getAvailableProjectOwners(this.ownerFilter)
      const data = await handleSuccessAsync(res)
      this.userOptions = data.value
    } catch (e) {
      this.$message({ closeOtherMessages: true, message: e.body.msg, type: 'error' })
    }
  }
  async openChangeProjectOwner (projectName) {
    this.projectOwner.project = projectName
    this.ownerFilter.project = projectName
    await this.loadAvailableProjectOwners()
    this.changeOwnerVisible = true
  }
  async changeProjectOwner () {
    if (!(this.projectOwner.project && this.projectOwner.owner)) {
      return
    }
    this.changeLoading = true
    try {
      await this.updateProjectOwner(this.projectOwner)
      this.changeLoading = false
      this.changeOwnerVisible = false
      this.$message({
        type: 'success',
        message: this.$t('changeProSuccess', this.projectOwner)
      })
      this.loadProjects(this.filterData)
    } catch (e) {
      this.$message({ message: e.body.msg ?? e.body.message, type: 'error' })
      this.changeLoading = false
      this.changeOwnerVisible = false
    }
  }
  resetProjectOwner () {
    this.projectOwner = {
      project: '',
      owner: ''
    }
  }
  data () {
    return {
      pageRefTags: pageRefTags,
      projectType: projectCfgs.projectType,
      project: {},
      isEdit: false,
      FormVisible: false,
      deleteTip: false,
      editAccessVisible: false,
      editFilterVisible: false,
      accessMetaList: {},
      accessList: [{
        name: 'admin',
        type: 'user',
        access: 'Cube Admin'
      }],
      accessMeta: {
        permission: 'READ',
        principal: true,
        sid: '',
        editAccessVisible: false
      },
      selected_project: localStorage.getItem('selected_project'),
      projectWidth: '440px',
      filterData: {
        page_offset: 0,
        page_size: +localStorage.getItem(pageRefTags.projectPager) || bigPageCount,
        exact: false,
        project: '',
        permission: 'ADMINISTRATION'
      },
      changeOwnerVisible: false,
      changeLoading: false,
      projectOwner: {
        project: '',
        owner: ''
      },
      userOptions: [],
      ownerFilter: {
        page_size: 100,
        page_offset: 0,
        project: '',
        name: ''
      }
    }
  }
  created () {
    this.loadProjects(this.filterData)
  }
}
</script>
<style lang="less">
  @import "../../assets/styles/variables.less";
  #project-list{
    margin-left: 20px;
    margin-right: 20px;
    .nodata {
      text-align: center;
      margin-top: 220px;
      img {
        width: 80px;
        height: 80px;
      }
    }
    .project-table {
      .el-icon-ksd-security {
        color: @text-title-color;
        &:hover {
          color: @base-color;
        }
      }
      .el-icon-ksd-backup:hover,
      .el-icon-ksd-table_delete:hover {
        color: @base-color;
      }
    }
    .el-table__expanded-cell[class*=cell] {
      .el-pagination.is-background {
        button,
        .el-pager li {
          margin: 0;
        }
      }
    }
    .el-tabs__nav {
      margin-left: 0;
    }
    .el-tabs__item{
      transition: none;
    }
    .add-project {
      .el-dialog {
        .el-dialog__body {
          min-height: 90px;
        }
      }
    }
  }
</style>
