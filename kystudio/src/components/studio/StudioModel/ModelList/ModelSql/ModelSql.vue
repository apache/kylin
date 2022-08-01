<template>
  <div class="model-sql ksd-mb-15" v-loading="isLoading">
    <kylin-editor ref="modelSql" :value="convertHiveSql" lang="sql" theme="chrome" :readOnly="true" :dragable="false" :isAbridge="true">
    </kylin-editor>
  </div>
</template>
<script>
import Vue from 'vue'
import { Component } from 'vue-property-decorator'
import { handleSuccess, handleError } from 'util/index'
import { mapActions, mapGetters } from 'vuex'
@Component({
  name: 'ModelSql',
  props: ['model'],
  computed: {
    ...mapGetters([
      'currentSelectedProject'
    ])
  },
  methods: {
    ...mapActions({
      getModelSql: 'GET_MODEL_SQL'
    })
  }
})
export default class modelSql extends Vue {
  convertHiveSql = ''
  isLoading = false
  mounted () {
    this.isLoading = true
    this.getModelSql({
      model: this.model,
      project: this.currentSelectedProject
    }).then((res) => {
      handleSuccess(res, (data) => {
        this.convertHiveSql = data.replace(/`/g, '')
        this.isLoading = false
      })
    }, (res) => {
      handleError(res)
      this.isLoading = false
    })
  }
}
</script>
<style lang="less">
  @import '../../../../../assets/styles/variables.less';
  .model-sql {
    height: 100%;
    .smyles_editor_wrap {
      height: calc(~'100% - 50px');
      overflow: hidden;
      .smyles_editor {
        height: 100%;
      }
    }
    .edit-copy-btn {
      right: 20px;
    }
  }
</style>
