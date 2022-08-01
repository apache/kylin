<template>
  <div class="modals">
    <UserEditModal />
    <ProjectEditModal />
    <DataSourceModal />
    <SourceTableModal :project-name="currentSelectedProject" />
    <ModelsExportModal />
    <ModelsImportModal />
    <ModelERDiagramModal />
  </div>
</template>

<script>
import Vue from 'vue'
import { mapGetters } from 'vuex'
import { Component } from 'vue-property-decorator'
import UserEditModal from '../UserEditModal/index.vue'
import DataSourceModal from '../DataSourceModal/index.vue'
import SourceTableModal from '../SourceTableModal/SourceTableModal.vue'
import ProjectEditModal from '../ProjectEditModal/ProjectEditModal.vue'
import ModelsExportModal from '../ModelsExportModal/ModelsExportModal.vue'
import ModelsImportModal from '../ModelsImportModal/ModelsImportModal.vue'
import ModelERDiagramModal from '../ModelERDiagramModal/ModelERDiagramModal'

@Component({
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  components: {
    UserEditModal,
    DataSourceModal,
    SourceTableModal,
    ModelsExportModal,
    ModelsImportModal,
    ProjectEditModal,
    ModelERDiagramModal
  }
})
export default class Modal extends Vue {
}
</script>
